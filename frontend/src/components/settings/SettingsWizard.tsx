import { useState, useEffect, useCallback } from 'react';
import { Check, ChevronLeft, ChevronRight, Loader2, Save, X, CheckCircle2, ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { cn } from '@/lib/utils';
import { toast } from 'sonner';
import { PlexSettings } from './PlexSettings';
import { LibrariesSettings } from './LibrariesSettings';
import { PathSettings } from './PathSettings';
import { ScanSettings } from './ScanSettings';
import { AISettings } from './AISettings';
import { MetadataSettings } from './MetadataSettings';
import { NotificationSettings } from './NotificationSettings';
import { IntegrationsSettings } from './IntegrationsSettings';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';

export { normalizeConfigForUI };

const steps = [
  { id: 'plex', label: 'Plex', component: PlexSettings },
  { id: 'libraries', label: 'Libraries', component: LibrariesSettings },
  { id: 'paths', label: 'Paths', component: PathSettings },
  { id: 'scan', label: 'Scan', component: ScanSettings },
  { id: 'openai', label: 'AI', component: AISettings },
  { id: 'metadata', label: 'Metadata', component: MetadataSettings },
  { id: 'integrations', label: 'Integrations', component: IntegrationsSettings },
  { id: 'notifications', label: 'Notifications', component: NotificationSettings },
  { id: 'review', label: 'Review', component: null },
];

interface SettingsWizardProps {
  onClose: () => void;
  onRebootStart?: () => void;
}

export function SettingsWizard({ onClose, onRebootStart }: SettingsWizardProps) {
  const [currentStep, setCurrentStep] = useState(0);
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});

  // Debug: log when currentStep changes
  useEffect(() => {
    console.log('[Wizard] currentStep changed to:', currentStep);
  }, [currentStep]);

  useEffect(() => {
    loadConfig();
  }, []);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Don't interfere with input fields
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
        return;
      }

      switch (e.key) {
        case 'Escape':
          onClose();
          break;
        case 'ArrowLeft':
          if (currentStep > 0) {
            setCurrentStep(prev => prev - 1);
          }
          break;
        case 'ArrowRight':
          if (currentStep < steps.length - 1 && validateStep(currentStep)) {
            setCurrentStep(prev => prev + 1);
          }
          break;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [currentStep, onClose]);

  const loadConfig = async () => {
    setIsLoading(true);
    try {
      const data = await api.getConfig();
      setConfig(normalizeConfigForUI(data));
    } catch (error) {
      console.error('Failed to load config:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const updateConfig = useCallback((updates: Partial<PMDAConfig>) => {
    setConfig(prev => ({ ...prev, ...updates }));
    // Clear errors for updated fields
    setErrors(prev => {
      const newErrors = { ...prev };
      Object.keys(updates).forEach(key => delete newErrors[key]);
      return newErrors;
    });
  }, []);

  const validateStep = (step: number): boolean => {
    const newErrors: Record<string, string> = {};

    if (step === 0) {
      // Plex validation
      if (!config.PLEX_HOST) newErrors.PLEX_HOST = 'Plex host is required';
      if (!config.PLEX_TOKEN) newErrors.PLEX_TOKEN = 'Plex token is required';
    }

    if (step === 1) {
      // Libraries validation
      if (!config.SECTION_IDS) newErrors.SECTION_IDS = 'At least one library must be selected';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const nextStep = (e?: React.MouseEvent) => {
    e?.preventDefault();
    e?.stopPropagation();
    console.log('[Wizard] nextStep called, currentStep:', currentStep, 'steps.length:', steps.length);
    const isValid = validateStep(currentStep);
    console.log('[Wizard] validateStep result:', isValid);
    if (isValid) {
      const nextStepIndex = Math.min(currentStep + 1, steps.length - 1);
      console.log('[Wizard] Moving to step:', nextStepIndex);
      setCurrentStep(nextStepIndex);
    } else {
      console.log('[Wizard] Validation failed, not moving to next step');
    }
  };

  const prevStep = () => {
    setCurrentStep(prev => Math.max(prev - 1, 0));
  };

  const saveConfig = async () => {
    setIsSaving(true);
    try {
      await api.saveConfig(config);
      toast.success(
        'Configuration saved successfully! The container will restart automatically.',
        { duration: 3000 }
      );
      // Close wizard and trigger rebooting overlay in Header
      onClose();
      if (onRebootStart) {
        onRebootStart();
      }
    } catch (error) {
      console.error('Failed to save config:', error);
      setErrors({ save: 'Failed to save configuration. Please try again.' });
      toast.error('Failed to save configuration');
      setIsSaving(false);
    }
  };

  const StepComponent = steps[currentStep].component;

  return (
    <>
      {/* Overlay with glass effect - very high z-index */}
      <div 
        className="fixed inset-0 z-[9998] bg-black/60 backdrop-blur-md" 
        onClick={(e) => {
          // Only close if clicking directly on overlay, not on modal content
          if (e.target === e.currentTarget) {
            onClose();
          }
        }}
        aria-hidden="true"
      />

      {/* Modal content - highest z-index */}
      <div 
        className="fixed left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 z-[9999] w-full max-w-4xl max-h-[90vh] overflow-hidden bg-card border border-border rounded-2xl shadow-2xl animate-in fade-in-0 zoom-in-95 duration-200"
        role="dialog"
        aria-modal="true"
        aria-labelledby="settings-title"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header with steps */}
        <div className="p-4 border-b border-border bg-card/95 backdrop-blur-sm">
          <div className="flex items-center justify-between mb-4">
            <h2 id="settings-title" className="text-lg font-semibold">PMDA Configuration</h2>
            <button
              onClick={onClose}
              className="p-1.5 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
              aria-label="Close settings"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Step indicators */}
          <div className="flex items-center gap-1 overflow-x-auto pb-1">
            {steps.map((step, index) => (
              <button
                key={step.id}
                onClick={() => {
                  if (index < currentStep || validateStep(currentStep)) {
                    setCurrentStep(index);
                  }
                }}
                className={cn(
                  "wizard-step flex-1 min-w-0 cursor-pointer",
                  index < currentStep && "wizard-step-complete",
                  index === currentStep && "wizard-step-active"
                )}
              >
                <div className="wizard-step-number">
                  {index < currentStep ? (
                    <Check className="w-4 h-4" />
                  ) : (
                    index + 1
                  )}
                </div>
                <span className={cn(
                  "text-xs hidden sm:block truncate",
                  index === currentStep ? "text-foreground font-medium" : "text-muted-foreground"
                )}>
                  {step.label}
                </span>
              </button>
            ))}
          </div>
        </div>

        {/* Body */}
        <div className="p-6 min-h-[300px] max-h-[60vh] overflow-y-auto">
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-primary" />
            </div>
          ) : currentStep === steps.length - 1 ? (
            // Review step
            <div className="space-y-4">
              <div className="flex items-center gap-3 mb-4">
                <div className="p-2 rounded-lg bg-success/10">
                  <CheckCircle2 className="w-5 h-5 text-success" />
                </div>
                <div>
                  <h3 className="font-medium">Review & Save</h3>
                  <p className="text-sm text-muted-foreground">
                    Review your settings before saving to the server
                  </p>
                </div>
              </div>

              <div className="space-y-3 text-sm">
                <h4 className="font-medium text-foreground">Plex</h4>
                <div className="grid gap-2 pl-2">
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Plex Host:</span> {config.PLEX_HOST || 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Plex Token:</span> {config.PLEX_TOKEN ? '••••••••' : 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Plex DB path:</span> {config.PLEX_BASE_PATH || config.PLEX_DB_PATH || 'Not set'}</div>
                </div>
                <h4 className="font-medium text-foreground">Libraries & Paths</h4>
                <div className="grid gap-2 pl-2">
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Section IDs:</span> {config.SECTION_IDS || 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Duplicates Folder:</span> {config.DUPE_ROOT || 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Music parent path:</span> {config.MUSIC_PARENT_PATH || 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Path mapping:</span> {config.PATH_MAP && Object.keys(config.PATH_MAP).length > 0 ? `${Object.keys(config.PATH_MAP).length} entries` : 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Disable path crosscheck:</span> {config.DISABLE_PATH_CROSSCHECK ? 'Yes' : 'No'}</div>
                </div>
                <h4 className="font-medium text-foreground">Scan</h4>
                <div className="grid gap-2 pl-2">
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Cross-Library Dedupe:</span> {config.CROSS_LIBRARY_DEDUPE ? 'Enabled' : 'Disabled'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Format Preference:</span> {Array.isArray(config.FORMAT_PREFERENCE) ? config.FORMAT_PREFERENCE.join(', ') : typeof config.FORMAT_PREFERENCE === 'string' ? config.FORMAT_PREFERENCE : 'Default'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Auto-move dupes:</span> {config.AUTO_MOVE_DUPES ? 'Enabled' : 'Disabled'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Scan threads:</span> {config.SCAN_THREADS ?? 'auto'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Skip folders:</span> {config.SKIP_FOLDERS || 'None'}</div>
                </div>
	                <h4 className="font-medium text-foreground">AI</h4>
	                <div className="grid gap-2 pl-2">
	                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">AI Provider:</span> {config.AI_PROVIDER || 'openai'}</div>
	                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Model:</span> {config.OPENAI_MODEL || 'Not set'}</div>
	                </div>
                <h4 className="font-medium text-foreground">Metadata</h4>
                <div className="grid gap-2 pl-2">
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Use MusicBrainz:</span> {config.USE_MUSICBRAINZ ? 'Enabled' : 'Disabled'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">MB Re-check not found:</span> {config.MB_RETRY_NOT_FOUND ? 'On' : 'Off'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Required tags:</span> {Array.isArray(config.REQUIRED_TAGS) ? config.REQUIRED_TAGS.join(', ') : 'Default'}</div>
                </div>
                <h4 className="font-medium text-foreground">Integrations</h4>
                <div className="grid gap-2 pl-2">
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Player target:</span> {config.PIPELINE_PLAYER_TARGET || 'none'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Pipeline match/fix:</span> {config.PIPELINE_ENABLE_MATCH_FIX !== false ? 'Enabled' : 'Disabled'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Pipeline dedupe:</span> {config.PIPELINE_ENABLE_DEDUPE !== false ? 'Enabled' : 'Disabled'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Pipeline incomplete move:</span> {config.PIPELINE_ENABLE_INCOMPLETE_MOVE !== false ? 'Enabled' : 'Disabled'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Autobrr:</span> {config.AUTOBRR_URL ? 'Configured' : 'Not set'}</div>
                </div>
                <h4 className="font-medium text-foreground">Notifications & Logging</h4>
                <div className="grid gap-2 pl-2">
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Discord webhook:</span> {config.DISCORD_WEBHOOK ? 'Set' : 'Not set'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Log level:</span> {config.LOG_LEVEL || 'INFO'}</div>
                  <div className="p-2 rounded bg-muted/50"><span className="font-medium">Log file:</span> {config.LOG_FILE || 'Console only'}</div>
                </div>
              </div>

              {/* Logging Advanced Options */}
              <Collapsible>
                <CollapsibleTrigger asChild>
                  <Button variant="ghost" size="sm" className="gap-1.5 text-muted-foreground w-full justify-start">
                    <ChevronDown className="w-4 h-4" />
                    Logging
                  </Button>
                </CollapsibleTrigger>
                <CollapsibleContent className="mt-2 space-y-4">
                  <div className="grid grid-cols-2 gap-4 p-4 rounded-lg border border-border bg-muted/30">
                    <div className="space-y-2">
                      <div className="flex items-center gap-1.5">
                        <Label htmlFor="log-level">Log Level</Label>
                        <FieldTooltip content="Logging verbosity. DEBUG shows everything, ERROR shows only errors. INFO is recommended for normal use." />
                      </div>
                      <Select
                        value={config.LOG_LEVEL || 'INFO'}
                        onValueChange={(value) =>
                          setConfig(prev => ({ ...prev, LOG_LEVEL: value as PMDAConfig['LOG_LEVEL'] }))
                        }
                      >
                        <SelectTrigger id="log-level">
                          <SelectValue placeholder="Select level" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="DEBUG">DEBUG</SelectItem>
                          <SelectItem value="INFO">INFO</SelectItem>
                          <SelectItem value="WARNING">WARNING</SelectItem>
                          <SelectItem value="ERROR">ERROR</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2">
                      <div className="flex items-center gap-1.5">
                        <Label htmlFor="log-file">Log File</Label>
                        <FieldTooltip content="Optional path to a log file. Leave empty to log to console only." />
                      </div>
                      <Input
                        id="log-file"
                        placeholder="/config/pmda.log"
                        value={config.LOG_FILE || ''}
                        onChange={(e) => setConfig(prev => ({ ...prev, LOG_FILE: e.target.value }))}
                      />
                    </div>
                  </div>
                </CollapsibleContent>
              </Collapsible>

              <div className="p-3 rounded-lg bg-primary/5 border border-primary/20 text-sm">
                <p className="text-muted-foreground">
                  <strong className="text-foreground">Note:</strong> Configuration will be saved and the container will restart automatically.
                </p>
              </div>

              {errors.save && (
                <div className="p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
                  {errors.save}
                </div>
              )}
            </div>
          ) : StepComponent ? (
            <StepComponent
              config={config}
              updateConfig={updateConfig}
              errors={errors}
            />
          ) : null}
        </div>

        {/* Footer */}
        <div 
          className="flex items-center justify-between p-4 border-t border-border bg-card/95 backdrop-blur-sm"
          onClick={(e) => e.stopPropagation()}
        >
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="ghost"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                prevStep();
              }}
              disabled={currentStep === 0}
              className="gap-1.5"
            >
              <ChevronLeft className="w-4 h-4" />
              Back
            </Button>
          </div>

          {currentStep === steps.length - 1 ? (
            <Button
              type="button"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                saveConfig();
              }}
              disabled={isSaving}
              className="gap-1.5"
            >
              {isSaving ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              Save Configuration
            </Button>
          ) : (
            <Button
              type="button"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                console.log('[Wizard] Next button clicked, preventing default and stopping propagation');
                nextStep(e);
              }}
              className="gap-1.5"
            >
              Next
              <ChevronRight className="w-4 h-4" />
            </Button>
          )}
        </div>
      </div>
    </>
  );
}
