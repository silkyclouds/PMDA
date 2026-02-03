import { useState } from 'react';
import { Link2, Loader2, CheckCircle2, XCircle, ExternalLink } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Button } from '@/components/ui/button';
import { PasswordInput } from '@/components/ui/password-input';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { toast } from 'sonner';

interface IntegrationsSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function IntegrationsSettings({ config, updateConfig, errors }: IntegrationsSettingsProps) {
  const [testingLidarr, setTestingLidarr] = useState(false);
  const [lidarrTestResult, setLidarrTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const [testingAutobrr, setTestingAutobrr] = useState(false);
  const [autobrrTestResult, setAutobrrTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const testLidarr = async () => {
    const url = config.LIDARR_URL?.trim();
    const apiKey = config.LIDARR_API_KEY?.trim();
    
    if (!url || !apiKey) {
      setLidarrTestResult({ success: false, message: 'Lidarr URL and API Key are required' });
      return;
    }

    setTestingLidarr(true);
    setLidarrTestResult(null);
    try {
      const result = await api.testLidarr(url, apiKey);
      setLidarrTestResult(result);
      if (result.success) {
        toast.success('Lidarr connection successful');
      } else {
        toast.error(result.message || 'Lidarr connection failed');
      }
    } catch (error: any) {
      const message = error?.message || 'Failed to test Lidarr connection';
      setLidarrTestResult({ success: false, message });
      toast.error(message);
    } finally {
      setTestingLidarr(false);
    }
  };

  const testAutobrr = async () => {
    const url = config.AUTOBRR_URL?.trim();
    const apiKey = config.AUTOBRR_API_KEY?.trim();
    
    if (!url || !apiKey) {
      setAutobrrTestResult({ success: false, message: 'Autobrr URL and API Key are required' });
      return;
    }

    setTestingAutobrr(true);
    setAutobrrTestResult(null);
    try {
      const result = await api.testAutobrr(url, apiKey);
      setAutobrrTestResult(result);
      if (result.success) {
        toast.success('Autobrr connection successful');
      } else {
        toast.error(result.message || 'Autobrr connection failed');
      }
    } catch (error: any) {
      const message = error?.message || 'Failed to test Autobrr connection';
      setAutobrrTestResult({ success: false, message });
      toast.error(message);
    } finally {
      setTestingAutobrr(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <Link2 className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Integrations</h3>
          <p className="text-sm text-muted-foreground">
            Configure integrations with Lidarr and Autobrr for automated album management
          </p>
        </div>
      </div>

      <div className="space-y-6">
        {/* Lidarr Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="flex items-center justify-between">
            <div>
              <h4 className="font-medium text-sm">Lidarr</h4>
              <p className="text-xs text-muted-foreground mt-1">
                Automatically send broken albums to Lidarr for re-download
              </p>
            </div>
            <Button
              variant="outline"
              size="sm"
              asChild
              className="gap-1.5 shrink-0"
            >
              <a
                href="https://wiki.servarr.com/lidarr"
                target="_blank"
                rel="noopener noreferrer"
              >
                <ExternalLink className="w-3 h-3" />
                Docs
              </a>
            </Button>
          </div>

          <div className="space-y-3 pt-2 border-t border-border">
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="lidarr-url">Lidarr URL</Label>
                <FieldTooltip content="Base URL of your Lidarr instance (e.g. http://192.168.1.100:8686). No trailing slash." />
              </div>
              <Input
                id="lidarr-url"
                placeholder="http://192.168.1.100:8686"
                value={config.LIDARR_URL || ''}
                onChange={(e) => updateConfig({ LIDARR_URL: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="lidarr-api-key">Lidarr API Key</Label>
                <FieldTooltip content="API key from Lidarr Settings → General → Security → API Key" />
              </div>
              <PasswordInput
                id="lidarr-api-key"
                placeholder="Enter your Lidarr API Key"
                value={config.LIDARR_API_KEY || ''}
                onChange={(e) => updateConfig({ LIDARR_API_KEY: e.target.value })}
              />
            </div>

            <div className="flex items-start justify-between p-3 rounded-lg bg-muted/50">
              <div className="space-y-0.5 flex-1">
                <div className="flex items-center gap-1.5">
                  <Label>Automatically fix broken albums</Label>
                  <FieldTooltip content="When enabled, PMDA will automatically send broken albums (missing tracks) to Lidarr for re-download. Requires MusicBrainz ID to be available." />
                </div>
                <p className="text-xs text-muted-foreground">
                  Automatically send broken albums to Lidarr when detected during scan
                </p>
              </div>
              <Switch
                checked={config.AUTO_FIX_BROKEN_ALBUMS ?? false}
                onCheckedChange={(checked) => updateConfig({ AUTO_FIX_BROKEN_ALBUMS: checked })}
                className="mt-1"
                disabled={!config.LIDARR_URL || !config.LIDARR_API_KEY}
              />
            </div>

            {(config.LIDARR_URL?.trim() || config.LIDARR_API_KEY?.trim()) && (
              <Button
                variant="secondary"
                onClick={testLidarr}
                disabled={testingLidarr || !config.LIDARR_URL?.trim() || !config.LIDARR_API_KEY?.trim()}
                className="gap-1.5 w-full"
              >
                {testingLidarr ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Testing...
                  </>
                ) : (
                  <>
                    Test Lidarr Connection
                  </>
                )}
              </Button>
            )}

            {lidarrTestResult && (
              <div className={`p-3 rounded-lg flex items-start gap-2 ${
                lidarrTestResult.success 
                  ? 'bg-green-500/10 border border-green-500/20' 
                  : 'bg-red-500/10 border border-red-500/20'
              }`}>
                {lidarrTestResult.success ? (
                  <CheckCircle2 className="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
                )}
                <p className={`text-xs ${lidarrTestResult.success ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                  {lidarrTestResult.message}
                </p>
              </div>
            )}

            <p className="text-xs text-muted-foreground">
              Lidarr integration allows PMDA to automatically send broken albums (missing tracks) to Lidarr for re-download. 
              The album must have a MusicBrainz Release Group ID for this to work.
            </p>
          </div>
        </div>

        {/* Autobrr Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="flex items-center justify-between">
            <div>
              <h4 className="font-medium text-sm">Autobrr</h4>
              <p className="text-xs text-muted-foreground mt-1">
                Create filters in Autobrr to monitor artists for new releases
              </p>
            </div>
            <Button
              variant="outline"
              size="sm"
              asChild
              className="gap-1.5 shrink-0"
            >
              <a
                href="https://autobrr.com"
                target="_blank"
                rel="noopener noreferrer"
              >
                <ExternalLink className="w-3 h-3" />
                Docs
              </a>
            </Button>
          </div>

          <div className="space-y-3 pt-2 border-t border-border">
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="autobrr-url">Autobrr URL</Label>
                <FieldTooltip content="Base URL of your Autobrr instance (e.g. http://192.168.1.100:7474). No trailing slash." />
              </div>
              <Input
                id="autobrr-url"
                placeholder="http://192.168.1.100:7474"
                value={config.AUTOBRR_URL || ''}
                onChange={(e) => updateConfig({ AUTOBRR_URL: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="autobrr-api-key">Autobrr API Key</Label>
                <FieldTooltip content="API key from Autobrr Settings → API Keys. Generate a new key if needed." />
              </div>
              <PasswordInput
                id="autobrr-api-key"
                placeholder="Enter your Autobrr API Key"
                value={config.AUTOBRR_API_KEY || ''}
                onChange={(e) => updateConfig({ AUTOBRR_API_KEY: e.target.value })}
              />
            </div>

            {(config.AUTOBRR_URL?.trim() || config.AUTOBRR_API_KEY?.trim()) && (
              <Button
                variant="secondary"
                onClick={testAutobrr}
                disabled={testingAutobrr || !config.AUTOBRR_URL?.trim() || !config.AUTOBRR_API_KEY?.trim()}
                className="gap-1.5 w-full"
              >
                {testingAutobrr ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Testing...
                  </>
                ) : (
                  <>
                    Test Autobrr Connection
                  </>
                )}
              </Button>
            )}

            {autobrrTestResult && (
              <div className={`p-3 rounded-lg flex items-start gap-2 ${
                autobrrTestResult.success 
                  ? 'bg-green-500/10 border border-green-500/20' 
                  : 'bg-red-500/10 border border-red-500/20'
              }`}>
                {autobrrTestResult.success ? (
                  <CheckCircle2 className="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
                )}
                <p className={`text-xs ${autobrrTestResult.success ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                  {autobrrTestResult.message}
                </p>
              </div>
            )}

            <p className="text-xs text-muted-foreground">
              Autobrr integration allows PMDA to create filters for monitoring artists. 
              You can add artists to Autobrr from the Library Browser when viewing similar artists.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
