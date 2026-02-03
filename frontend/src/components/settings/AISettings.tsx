import { useState, useEffect, useRef } from 'react';
import { Sparkles, Loader2, XCircle, ChevronDown, ChevronUp, ExternalLink } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { PasswordInput } from '@/components/ui/password-input';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';

interface AISettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function AISettings({ config, updateConfig, errors }: AISettingsProps) {
  const [loadingModels, setLoadingModels] = useState(false);
  const [models, setModels] = useState<string[]>([]);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const credentialsRef = useRef<string>('');

  const provider = (config.AI_PROVIDER || 'openai') as 'openai' | 'anthropic' | 'google' | 'ollama';

  // Auto-fetch models when credentials are present
  useEffect(() => {
    let credentialsKey = '';
    let credentials: { apiKey?: string; url?: string } = {};

    if (provider === 'ollama') {
      const url = config.OLLAMA_URL?.trim() || '';
      if (!url) return;
      credentialsKey = `ollama:${url}`;
      credentials = { url };
    } else {
      let apiKey = '';
      if (provider === 'openai') {
        apiKey = config.OPENAI_API_KEY?.trim() || '';
      } else if (provider === 'anthropic') {
        apiKey = config.ANTHROPIC_API_KEY?.trim() || '';
      } else if (provider === 'google') {
        apiKey = config.GOOGLE_API_KEY?.trim() || '';
      }
      if (!apiKey) return;
      credentialsKey = `${provider}:${apiKey}`;
      credentials = { apiKey };
    }

    // Skip if already fetched with these credentials
    if (credentialsRef.current === credentialsKey) return;

    credentialsRef.current = credentialsKey;
    setLoadingModels(true);
    setErrorMessage(null);
    setModels([]);

    (async () => {
      try {
        const fetchedModels = await api.getAIModels(provider, credentials);
        if (fetchedModels.length > 0) {
          setModels(fetchedModels);
          setErrorMessage(null);
        } else {
          setErrorMessage('No models available. Please check your credentials.');
        }
      } catch (error) {
        console.error(`Failed to fetch models from ${provider}:`, error);
        const errorMsg = error instanceof Error ? error.message : `Failed to fetch models from ${provider}`;
        setErrorMessage(errorMsg);
        setModels([]);
      } finally {
        setLoadingModels(false);
      }
    })();
  }, [provider, config.OPENAI_API_KEY, config.ANTHROPIC_API_KEY, config.GOOGLE_API_KEY, config.OLLAMA_URL]);

  // Reset credentials ref when provider changes
  useEffect(() => {
    credentialsRef.current = '';
    setModels([]);
    setErrorMessage(null);
  }, [provider]);

  const getProviderLabel = (p: string) => {
    switch (p) {
      case 'openai': return 'OpenAI';
      case 'anthropic': return 'Anthropic (Claude)';
      case 'google': return 'Google (Gemini)';
      case 'ollama': return 'Ollama (Local)';
      default: return p;
    }
  };

  const getProviderHelp = (p: string) => {
    switch (p) {
      case 'openai':
        return 'Get your API key at platform.openai.com';
      case 'anthropic':
        return 'Get your API key at console.anthropic.com';
      case 'google':
        return 'Get your API key at makersuite.google.com/app/apikey';
      case 'ollama':
        return 'Enter the URL of your local Ollama instance (default: http://localhost:11434)';
      default:
        return '';
    }
  };

  const getModelFieldName = () => {
    // All providers use OPENAI_MODEL for now (backend handles the mapping)
    return 'OPENAI_MODEL';
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
            Configure AI provider for duplicate analysis
          </p>
        </div>
      </div>

      <div className="space-y-4">
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="space-y-2">
            <div className="flex items-center gap-1.5">
              <Label htmlFor="ai-provider">AI Provider</Label>
              <FieldTooltip content="Choose which AI provider to use for best-edition selection. OpenAI, Anthropic, and Google require API keys. Ollama runs locally on your machine." />
            </div>
            <Select
              value={provider}
              onValueChange={(value) => {
                updateConfig({ AI_PROVIDER: value as any });
                // Reset credentials ref when provider changes
                credentialsRef.current = '';
              }}
            >
              <SelectTrigger id="ai-provider">
                <SelectValue placeholder="Select provider" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="openai">OpenAI</SelectItem>
                <SelectItem value="anthropic">Anthropic (Claude)</SelectItem>
                <SelectItem value="google">Google (Gemini)</SelectItem>
                <SelectItem value="ollama">Ollama (Local)</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {provider === 'ollama' ? (
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="ollama-url">Ollama URL</Label>
                <FieldTooltip content={getProviderHelp(provider)} />
              </div>
              <Input
                id="ollama-url"
                placeholder="http://localhost:11434"
                value={config.OLLAMA_URL || ''}
                onChange={(e) => {
                  updateConfig({ OLLAMA_URL: e.target.value });
                  credentialsRef.current = '';
                }}
              />
            </div>
          ) : (
            <div className="space-y-2">
              <div className="flex items-center gap-1.5 flex-wrap">
                <Label htmlFor="ai-api-key">API Key</Label>
                <FieldTooltip content={getProviderHelp(provider)} />
                <a
                  href={
                    provider === 'openai'
                      ? 'https://platform.openai.com/api-keys'
                      : provider === 'anthropic'
                        ? 'https://console.anthropic.com/settings/keys'
                        : 'https://aistudio.google.com/apikey'
                  }
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-primary hover:underline inline-flex items-center gap-1"
                >
                  Get API key <ExternalLink className="w-3 h-3" />
                </a>
              </div>
              <PasswordInput
                id="ai-api-key"
                placeholder={provider === 'openai' ? 'sk-...' : 'Enter your API key'}
                value={
                  provider === 'openai' ? (config.OPENAI_API_KEY || '') :
                  provider === 'anthropic' ? (config.ANTHROPIC_API_KEY || '') :
                  (config.GOOGLE_API_KEY || '')
                }
                onChange={(e) => {
                  const updates: Partial<PMDAConfig> = {};
                  if (provider === 'openai') {
                    updates.OPENAI_API_KEY = e.target.value;
                  } else if (provider === 'anthropic') {
                    updates.ANTHROPIC_API_KEY = e.target.value;
                  } else if (provider === 'google') {
                    updates.GOOGLE_API_KEY = e.target.value;
                  }
                  updateConfig(updates);
                  credentialsRef.current = '';
                }}
              />
            </div>
          )}

          {(loadingModels || models.length > 0 || errorMessage) && (
            <div className="space-y-2 pt-2 border-t border-border">
              <div className="flex items-center gap-1.5">
                <Label>Model</Label>
                <FieldTooltip content={`Compatible ${getProviderLabel(provider)} models only (parseable output). The model to use for duplicate analysis.`} />
              </div>
              {loadingModels ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Loading available models...
                </div>
              ) : errorMessage ? (
                <div className="p-3 rounded-lg bg-destructive/10 flex items-start gap-2">
                  <XCircle className="w-5 h-5 text-destructive flex-shrink-0 mt-0.5" />
                  <p className="text-sm text-destructive">{errorMessage}</p>
                </div>
              ) : models.length > 0 ? (
                <>
                  <Select
                    value={config.OPENAI_MODEL || models[0]}
                    onValueChange={(value) => updateConfig({ OPENAI_MODEL: value })}
                  >
                    <SelectTrigger id="ai-model">
                      <SelectValue placeholder="Select a model" />
                    </SelectTrigger>
                    <SelectContent>
                      {Array.isArray(models) && models.map((model) => (
                        <SelectItem key={model} value={model}>
                          {model}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground mt-2">
                    Only models compatible with PMDA (parseable output format) are shown.{' '}
                    {provider === 'ollama'
                      ? 'Make sure the selected model is pulled in Ollama (ollama pull &lt;model-name&gt;).'
                      : provider === 'openai'
                        ? 'Use a mini or nano model (e.g. gpt-4o-mini) for large libraries to reduce costs.'
                        : 'Choose a model that balances performance and cost for your library size.'}
                  </p>
                </>
              ) : null}
            </div>
          )}
        </div>

        {/* Advanced: custom AI prompt */}
        <Collapsible open={showAdvanced} onOpenChange={setShowAdvanced}>
          <CollapsibleTrigger asChild>
            <Button
              variant="ghost"
              size="sm"
              className="w-full justify-between mt-2"
            >
              Advanced
              {showAdvanced ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="mt-3 space-y-2">
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="ai-prompt">Custom AI prompt</Label>
                <FieldTooltip content="Prompt sent to the AI when choosing the best edition among duplicates. Leave empty to use the default prompt. Changes are stored in the database." />
              </div>
              <Textarea
                id="ai-prompt"
                placeholder="Leave empty to use the default prompt..."
                value={config.AI_PROMPT ?? ''}
                onChange={(e) => updateConfig({ AI_PROMPT: e.target.value })}
                className="min-h-[200px] font-mono text-sm resize-y"
                rows={12}
              />
              <p className="text-xs text-muted-foreground">
                Stored in SQLite. Empty = use built-in default (classical vs non-classical, format priority, output format).
              </p>
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="openai-model-fallbacks">Model fallbacks</Label>
                <FieldTooltip content="Comma-separated list of fallback models if the primary model fails (e.g. gpt-4o-mini,gpt-4o-nano). Leave empty for no fallbacks." />
              </div>
              <Input
                id="openai-model-fallbacks"
                placeholder="gpt-4o-mini,gpt-4o-nano"
                value={config.OPENAI_MODEL_FALLBACKS ?? ''}
                onChange={(e) => updateConfig({ OPENAI_MODEL_FALLBACKS: e.target.value })}
              />
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </div>
  );
}
