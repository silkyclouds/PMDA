import { useState } from 'react';
import { Bell, ChevronDown } from 'lucide-react';

import type { PMDAConfig } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

interface NotificationSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
}

export function NotificationSettings({ config, updateConfig }: NotificationSettingsProps) {
  const [advancedInAppOpen, setAdvancedInAppOpen] = useState(false);

  const inAppEnabled = Boolean(config.TASK_NOTIFICATIONS_ENABLED ?? true);
  const inAppSuccess = Boolean(config.TASK_NOTIFICATIONS_SUCCESS ?? true);
  const inAppFailure = Boolean(config.TASK_NOTIFICATIONS_FAILURE ?? true);
  const silentInteractive = Boolean(config.TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN ?? false);
  const cooldownSec = Math.max(0, Number(config.TASK_NOTIFICATIONS_COOLDOWN_SEC ?? 20) || 20);

  const taskToggles: Array<{ key: keyof PMDAConfig; label: string; description: string }> = [
    { key: 'TASK_NOTIFY_SCAN_CHANGED', label: 'Changed scan', description: 'Notification when changed-only scans end/fail.' },
    { key: 'TASK_NOTIFY_SCAN_FULL', label: 'Full scan', description: 'Notification when full scans end/fail.' },
    { key: 'TASK_NOTIFY_ENRICH_BATCH', label: 'Enrichment', description: 'Notification for enrich batch completion/failure.' },
    { key: 'TASK_NOTIFY_DEDUPE', label: 'Dedupe', description: 'Notification for dedupe completion/failure.' },
    { key: 'TASK_NOTIFY_INCOMPLETE_MOVE', label: 'Incomplete move', description: 'Notification when incomplete albums move ends/fails.' },
    { key: 'TASK_NOTIFY_EXPORT', label: 'Export', description: 'Notification when export jobs end/fail.' },
    { key: 'TASK_NOTIFY_PLAYER_SYNC', label: 'Player sync', description: 'Notification when player sync jobs end/fail.' },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <div className="p-2 rounded-lg bg-primary/10">
          <Bell className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">In-app notifications</h3>
          <p className="text-sm text-muted-foreground">
            Keep PMDA notifications inside the UI. External webhook notifications are disabled.
          </p>
        </div>
      </div>

      <div className="space-y-4 p-4 rounded-lg border border-border">
        <div className="flex items-start justify-between rounded-md bg-muted/40 p-3">
          <div className="space-y-1">
            <Label>Enable in-app task notifications</Label>
            <p className="text-xs text-muted-foreground">Show notifications when scans and pipeline tasks complete or fail.</p>
          </div>
          <Switch
            checked={inAppEnabled}
            onCheckedChange={(checked) => updateConfig({ TASK_NOTIFICATIONS_ENABLED: Boolean(checked) })}
          />
        </div>

        <Collapsible open={advancedInAppOpen} onOpenChange={setAdvancedInAppOpen}>
          <div className="rounded-md border border-border/70 bg-muted/20">
            <CollapsibleTrigger asChild>
              <Button type="button" variant="ghost" className="w-full justify-between rounded-none px-3 py-2 text-left">
                <span className="text-sm font-medium">Advanced</span>
                <ChevronDown className={`w-4 h-4 transition-transform ${advancedInAppOpen ? 'rotate-180' : ''}`} />
              </Button>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <div className="space-y-3 border-t border-border/60 p-3">
                <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                  <div className="flex items-center justify-between rounded-md border border-border bg-background px-3 py-2">
                    <span className="text-sm">Success notifications</span>
                    <Switch
                      checked={inAppSuccess}
                      onCheckedChange={(checked) => updateConfig({ TASK_NOTIFICATIONS_SUCCESS: Boolean(checked) })}
                    />
                  </div>
                  <div className="flex items-center justify-between rounded-md border border-border bg-background px-3 py-2">
                    <span className="text-sm">Failure notifications</span>
                    <Switch
                      checked={inAppFailure}
                      onCheckedChange={(checked) => updateConfig({ TASK_NOTIFICATIONS_FAILURE: Boolean(checked) })}
                    />
                  </div>
                </div>

                <div className="flex items-center justify-between rounded-md border border-border bg-background px-3 py-2">
                  <div>
                    <p className="text-sm">Silent during interactive scan</p>
                    <p className="text-xs text-muted-foreground">Skip notifications triggered by manual interactive scans.</p>
                  </div>
                  <Switch
                    checked={silentInteractive}
                    onCheckedChange={(checked) => updateConfig({ TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN: Boolean(checked) })}
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="task-toast-cooldown">Notification cooldown (seconds)</Label>
                  <Input
                    id="task-toast-cooldown"
                    type="number"
                    min={0}
                    max={3600}
                    value={cooldownSec}
                    onChange={(e) => {
                      const value = Math.max(0, Math.min(3600, Number(e.target.value) || 0));
                      updateConfig({ TASK_NOTIFICATIONS_COOLDOWN_SEC: value });
                    }}
                  />
                </div>

                <div className="space-y-2">
                  <Label>Task filters</Label>
                  <div className="space-y-2">
                    {taskToggles.map((task) => {
                      const checked = Boolean(config[task.key] ?? true);
                      return (
                        <div key={String(task.key)} className="flex items-center justify-between rounded-md border border-border bg-background px-3 py-2">
                          <div className="space-y-0.5">
                            <p className="text-sm">{task.label}</p>
                            <p className="text-xs text-muted-foreground">{task.description}</p>
                          </div>
                          <Switch
                            checked={checked}
                            onCheckedChange={(next) => updateConfig({ [task.key]: Boolean(next) } as Partial<PMDAConfig>)}
                          />
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            </CollapsibleContent>
          </div>
        </Collapsible>
      </div>
    </div>
  );
}
