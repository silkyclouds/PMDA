import { useState, useEffect, useRef } from 'react';
import { Sparkles, CheckCircle2, XCircle, Loader2, ExternalLink } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { PasswordInput } from '@/components/ui/password-input';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';

interface OpenAISettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function OpenAISettings({ config, updateConfig, errors }: OpenAISettingsProps) {
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const [loadingModels, setLoadingModels] = useState(false);
  const [models, setModels] = useState<string[]>([
    'gpt-4o-mini',
    'gpt-4o',
    'gpt-4-turbo',
    'gpt-3.5-turbo',
  ]);
  const autoFetchedKeyRef = useRef<string | null>(null);

  // Auto-fetch models when API key is present
  useEffect(() => {
    const key = config.OPENAI_API_KEY?.trim();
    if (!key || autoFetchedKeyRef.current === key) return;
    autoFetchedKeyRef.current = key;
    setLoadingModels(true);
    (async () => {
      try {
        // Fetch models directly from OpenAI API
        const fetchedModels = await api.getOpenAIModels(key);
        if (fetchedModels.length > 0) {
          setModels(fetchedModels);
        } else {
          // If no models returned, show error message
          setTestResult({ success: false, message: 'No models available. Please check your API key.' });
        }
      } catch (error) {
        console.error('Failed to fetch models from OpenAI:', error);
        // Show error to user
        const errorMessage = error instanceof Error ? error.message : 'Failed to fetch models from OpenAI API';
        setTestResult({ success: false, message: errorMessage });
        // Keep default models as fallback for display, but user will see the error
      } finally {
        setLoadingModels(false);
      }
    })();
  }, [config.OPENAI_API_KEY]);

  const testOpenAI = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      // Pass the API key from the form to the check function
      const result = await api.checkOpenAI(config.OPENAI_API_KEY);
      setTestResult(result);
    } catch (error) {
      setTestResult({ success: false, message: 'OpenAI test failed' });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <Sparkles className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">AI</h3>
          <p className="text-sm text-muted-foreground">
            Configure OpenAI settings for duplicate analysis
          </p>
        </div>
      </div>

      <div className="space-y-4">
        {/* OpenAI Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <h4 className="font-medium text-sm">OpenAI</h4>

          <div className="space-y-2">
            <div className="flex items-center gap-1.5 flex-wrap">
              <Label htmlFor="openai-key">API Key</Label>
              <FieldTooltip content="Your OpenAI API key for LLM-based best-edition selection." />
              <a
                href="https://platform.openai.com/api-keys"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-primary hover:underline inline-flex items-center gap-1"
              >
                Get API key <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            <PasswordInput
              id="openai-key"
              placeholder="sk-..."
              value={config.OPENAI_API_KEY || ''}
              onChange={(e) => updateConfig({ OPENAI_API_KEY: e.target.value })}
            />
          </div>

          {config.OPENAI_API_KEY?.trim() && (
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label>Model</Label>
                <FieldTooltip content="The OpenAI model to use for duplicate analysis. gpt-4o-mini is recommended for cost-effectiveness." />
              </div>
              {loadingModels ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Loading available models...
                </div>
              ) : models.length > 0 ? (
                <>
                  <RadioGroup
                    value={config.OPENAI_MODEL || 'gpt-4o-mini'}
                    onValueChange={(value) => updateConfig({ OPENAI_MODEL: value })}
                    className="space-y-2"
                  >
                    {Array.isArray(models) && models.map((model) => (
                      <div key={model} className="flex items-center space-x-2">
                        <RadioGroupItem value={model} id={`model-${model}`} />
                        <Label
                          htmlFor={`model-${model}`}
                          className="font-normal cursor-pointer flex-1"
                        >
                          {model}
                        </Label>
                      </div>
                    ))}
                  </RadioGroup>
                  <p className="text-xs text-muted-foreground mt-2">
                    💡 Tip: Use a mini or nano model (e.g., gpt-4o-mini) if you have a large number of tracks or albums to reduce costs.
                  </p>
                </>
              ) : (
                <p className="text-sm text-muted-foreground">No models available. Check your API key.</p>
              )}
            </div>
          )}

          <Button
            type="button"
            variant="secondary"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              testOpenAI();
            }}
            disabled={testing || !config.OPENAI_API_KEY}
            className="gap-1.5"
          >
            {testing ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Sparkles className="w-4 h-4" />
            )}
            Test OpenAI
          </Button>

          {testResult && (
            <div
              className={`p-3 rounded-lg flex items-start gap-2 ${
                testResult.success ? 'bg-success/10' : 'bg-destructive/10'
              }`}
            >
              {testResult.success ? (
                <CheckCircle2 className="w-5 h-5 text-success flex-shrink-0" />
              ) : (
                <XCircle className="w-5 h-5 text-destructive flex-shrink-0" />
              )}
              <p
                className={`text-sm font-medium ${
                  testResult.success ? 'text-success' : 'text-destructive'
                }`}
              >
                {testResult.message}
              </p>
            </div>
          )}
        </div>

      </div>
    </div>
  );
}
