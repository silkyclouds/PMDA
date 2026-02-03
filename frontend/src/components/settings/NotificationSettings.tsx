import { useState } from 'react';
import { Bell, Loader2, CheckCircle2, XCircle } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { PasswordInput } from '@/components/ui/password-input';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';

interface NotificationSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function NotificationSettings({ config, updateConfig, errors }: NotificationSettingsProps) {
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const testDiscord = async () => {
    const webhook = config.DISCORD_WEBHOOK?.trim();
    if (!webhook) {
      setTestResult({ success: false, message: 'Discord webhook URL is required' });
      return;
    }

    setTesting(true);
    setTestResult(null);
    try {
      // Send a test notification to Discord
      const response = await fetch(webhook, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          content: '🎉 Test notification from PMDA! If you see this message, your Discord webhook is configured correctly.',
        }),
      });

      if (response.ok) {
        setTestResult({ success: true, message: 'Test notification sent successfully! Check your Discord channel.' });
      } else {
        const errorText = await response.text().catch(() => 'Unknown error');
        setTestResult({ success: false, message: `Failed to send test notification: ${errorText}` });
      }
    } catch (error) {
      setTestResult({ 
        success: false, 
        message: error instanceof Error ? error.message : 'Failed to send test notification' 
      });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <Bell className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Notifications</h3>
          <p className="text-sm text-muted-foreground">
            Configure notification settings for scan completion
          </p>
        </div>
      </div>

      <div className="space-y-4">
        {/* Discord Webhook */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <h4 className="font-medium text-sm">Discord</h4>

          <div className="space-y-2">
            <div className="flex items-center gap-1.5">
              <Label htmlFor="discord-webhook">Discord Webhook URL</Label>
              <FieldTooltip content="Discord webhook URL for completion notifications. Create one in Discord server settings → Integrations → Webhooks." />
            </div>
            <PasswordInput
              id="discord-webhook"
              placeholder="https://discord.com/api/webhooks/..."
              value={config.DISCORD_WEBHOOK || ''}
              onChange={(e) => updateConfig({ DISCORD_WEBHOOK: e.target.value })}
            />
            <p className="text-xs text-muted-foreground">
              Optional. PMDA will send a notification to this webhook when a scan completes.
            </p>
          </div>

          {config.DISCORD_WEBHOOK?.trim() && (
            <Button
              variant="secondary"
              onClick={testDiscord}
              disabled={testing}
              className="gap-1.5"
            >
              {testing ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Bell className="w-4 h-4" />
              )}
              Send Test Notification
            </Button>
          )}

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
