import { useMemo, useState } from 'react';
import { Link2, Loader2, CheckCircle2, XCircle, ExternalLink, SlidersHorizontal } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Button } from '@/components/ui/button';
import { PasswordInput } from '@/components/ui/password-input';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import * as api from '@/lib/api';
import type { PMDAConfig, PlayerTarget } from '@/lib/api';
import { toast } from 'sonner';

interface IntegrationsSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

const PLAYER_TARGETS: Array<{ value: PlayerTarget; label: string }> = [
  { value: 'none', label: 'None' },
  { value: 'plex', label: 'Plex' },
  { value: 'jellyfin', label: 'Jellyfin' },
  { value: 'navidrome', label: 'Navidrome' },
];

export function IntegrationsSettings({ config, updateConfig }: IntegrationsSettingsProps) {
  const [testingPlayer, setTestingPlayer] = useState(false);
  const [refreshingPlayer, setRefreshingPlayer] = useState(false);
  const [playerResult, setPlayerResult] = useState<{ success: boolean; message: string } | null>(null);
  const [testingAutobrr, setTestingAutobrr] = useState(false);
  const [autobrrTestResult, setAutobrrTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const playerTarget: PlayerTarget = useMemo(() => {
    const value = String(config.PIPELINE_PLAYER_TARGET || 'none').trim().toLowerCase();
    return (['none', 'plex', 'jellyfin', 'navidrome'].includes(value) ? value : 'none') as PlayerTarget;
  }, [config.PIPELINE_PLAYER_TARGET]);

  const testPlayer = async () => {
    setTestingPlayer(true);
    setPlayerResult(null);
    try {
      const result = await api.playerCheck({
        target: playerTarget,
        PLEX_HOST: config.PLEX_HOST,
        PLEX_TOKEN: config.PLEX_TOKEN,
        JELLYFIN_URL: config.JELLYFIN_URL,
        JELLYFIN_API_KEY: config.JELLYFIN_API_KEY,
        NAVIDROME_URL: config.NAVIDROME_URL,
        NAVIDROME_USERNAME: config.NAVIDROME_USERNAME,
        NAVIDROME_PASSWORD: config.NAVIDROME_PASSWORD,
        NAVIDROME_API_KEY: config.NAVIDROME_API_KEY,
      });
      setPlayerResult({ success: result.success, message: result.message });
      if (result.success) {
        toast.success(result.message || 'Player connection successful');
      } else {
        toast.error(result.message || 'Player connection failed');
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Failed to test player connection';
      setPlayerResult({ success: false, message });
      toast.error(message);
    } finally {
      setTestingPlayer(false);
    }
  };

  const triggerPlayerRefresh = async () => {
    setRefreshingPlayer(true);
    try {
      const result = await api.playerRefresh(playerTarget);
      if (result.success) {
        toast.success(result.message || 'Player refresh triggered');
      } else {
        toast.error(result.message || 'Failed to trigger player refresh');
      }
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : 'Failed to trigger player refresh');
    } finally {
      setRefreshingPlayer(false);
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
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Failed to test Autobrr connection';
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
          <h3 className="font-medium">Pipeline & Integrations</h3>
          <p className="text-sm text-muted-foreground">
            Configure the automated pipeline and optional player sync.
          </p>
        </div>
      </div>

      <div className="space-y-4 p-4 rounded-lg border border-border">
        <div className="flex items-center gap-2">
          <SlidersHorizontal className="w-4 h-4 text-primary" />
          <h4 className="font-medium text-sm">Pipeline steps</h4>
        </div>

        <div className="space-y-3">
          <div className="flex items-start justify-between p-3 rounded-lg bg-muted/50">
            <div className="space-y-0.5 flex-1">
              <Label>Match + Fix metadata</Label>
              <p className="text-xs text-muted-foreground">Tags, album covers and artist images.</p>
            </div>
            <Switch
              checked={config.PIPELINE_ENABLE_MATCH_FIX ?? true}
              onCheckedChange={(checked) => updateConfig({ PIPELINE_ENABLE_MATCH_FIX: checked })}
              className="mt-1"
            />
          </div>

          <div className="flex items-start justify-between p-3 rounded-lg bg-muted/50">
            <div className="space-y-0.5 flex-1">
              <Label>Deduplicate</Label>
              <p className="text-xs text-muted-foreground">Move duplicate loser albums to the dupe folder.</p>
            </div>
            <Switch
              checked={config.PIPELINE_ENABLE_DEDUPE ?? true}
              onCheckedChange={(checked) => updateConfig({ PIPELINE_ENABLE_DEDUPE: checked })}
              className="mt-1"
            />
          </div>

          <div className="flex items-start justify-between p-3 rounded-lg bg-muted/50">
            <div className="space-y-0.5 flex-1">
              <Label>Move incomplete albums</Label>
              <p className="text-xs text-muted-foreground">Move incomplete albums to the configured incomplete folder.</p>
            </div>
            <Switch
              checked={config.PIPELINE_ENABLE_INCOMPLETE_MOVE ?? true}
              onCheckedChange={(checked) => updateConfig({ PIPELINE_ENABLE_INCOMPLETE_MOVE: checked })}
              className="mt-1"
            />
          </div>

          <div className="flex items-start justify-between p-3 rounded-lg bg-muted/50">
            <div className="space-y-0.5 flex-1">
              <Label>Export files library</Label>
              <p className="text-xs text-muted-foreground">Build/update the export tree using hardlink/symlink/copy/move strategy.</p>
            </div>
            <Switch
              checked={config.PIPELINE_ENABLE_EXPORT ?? false}
              onCheckedChange={(checked) => updateConfig({ PIPELINE_ENABLE_EXPORT: checked })}
              className="mt-1"
            />
          </div>

          <div className="flex items-start justify-between p-3 rounded-lg bg-muted/50">
            <div className="space-y-0.5 flex-1">
              <Label>Sync external player</Label>
              <p className="text-xs text-muted-foreground">Trigger Plex/Jellyfin/Navidrome library refresh after scan.</p>
            </div>
            <Switch
              checked={config.PIPELINE_ENABLE_PLAYER_SYNC ?? false}
              onCheckedChange={(checked) => updateConfig({ PIPELINE_ENABLE_PLAYER_SYNC: checked })}
              className="mt-1"
            />
          </div>
        </div>
      </div>

      <div className="space-y-4 p-4 rounded-lg border border-border">
        <div className="flex items-center justify-between">
          <div>
            <h4 className="font-medium text-sm">Player sync target</h4>
            <p className="text-xs text-muted-foreground mt-1">
              Choose where PMDA sends refresh requests after the pipeline.
            </p>
          </div>
        </div>

        <div className="space-y-2">
          <div className="flex items-center gap-1.5">
            <Label htmlFor="player-target">Target</Label>
            <FieldTooltip content="Choose where PMDA sends refresh requests after the pipeline." />
          </div>
          <Select
            value={playerTarget}
            onValueChange={(value) => updateConfig({ PIPELINE_PLAYER_TARGET: value as PMDAConfig['PIPELINE_PLAYER_TARGET'] })}
          >
            <SelectTrigger id="player-target">
              <SelectValue placeholder="Select target" />
            </SelectTrigger>
            <SelectContent>
              {PLAYER_TARGETS.map((item) => (
                <SelectItem key={item.value} value={item.value}>
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {playerTarget === 'plex' && (
          <div className="space-y-3 pt-2 border-t border-border">
            <div className="space-y-2">
              <Label htmlFor="plex-url">Plex URL</Label>
              <Input
                id="plex-url"
                placeholder="http://192.168.1.100:32400"
                value={config.PLEX_HOST || ''}
                onChange={(e) => updateConfig({ PLEX_HOST: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="plex-token">Plex token</Label>
              <PasswordInput
                id="plex-token"
                placeholder="x-plex-token"
                value={config.PLEX_TOKEN || ''}
                onChange={(e) => updateConfig({ PLEX_TOKEN: e.target.value })}
              />
            </div>
          </div>
        )}

        {playerTarget === 'jellyfin' && (
          <div className="space-y-3 pt-2 border-t border-border">
            <div className="space-y-2">
              <Label htmlFor="jellyfin-url">Jellyfin URL</Label>
              <Input
                id="jellyfin-url"
                placeholder="http://192.168.1.100:8096"
                value={config.JELLYFIN_URL || ''}
                onChange={(e) => updateConfig({ JELLYFIN_URL: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="jellyfin-api-key">Jellyfin API Key</Label>
              <PasswordInput
                id="jellyfin-api-key"
                placeholder="Enter Jellyfin API key"
                value={config.JELLYFIN_API_KEY || ''}
                onChange={(e) => updateConfig({ JELLYFIN_API_KEY: e.target.value })}
              />
            </div>
          </div>
        )}

        {playerTarget === 'navidrome' && (
          <div className="space-y-3 pt-2 border-t border-border">
            <div className="space-y-2">
              <Label htmlFor="navidrome-url">Navidrome URL</Label>
              <Input
                id="navidrome-url"
                placeholder="http://192.168.1.100:4533"
                value={config.NAVIDROME_URL || ''}
                onChange={(e) => updateConfig({ NAVIDROME_URL: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="navidrome-user">Username</Label>
              <Input
                id="navidrome-user"
                placeholder="admin"
                value={config.NAVIDROME_USERNAME || ''}
                onChange={(e) => updateConfig({ NAVIDROME_USERNAME: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="navidrome-pass">Password</Label>
              <PasswordInput
                id="navidrome-pass"
                placeholder="Enter Navidrome password"
                value={config.NAVIDROME_PASSWORD || ''}
                onChange={(e) => updateConfig({ NAVIDROME_PASSWORD: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="navidrome-api-key">API key (optional)</Label>
              <PasswordInput
                id="navidrome-api-key"
                placeholder="Optional if username/password is set"
                value={config.NAVIDROME_API_KEY || ''}
                onChange={(e) => updateConfig({ NAVIDROME_API_KEY: e.target.value })}
              />
            </div>
          </div>
        )}

        {playerTarget !== 'none' && (
          <div className="flex flex-wrap items-center gap-2 pt-2">
            <Button
              variant="secondary"
              onClick={testPlayer}
              disabled={testingPlayer}
              className="gap-1.5"
            >
              {testingPlayer ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              Test connection
            </Button>
            <Button
              variant="outline"
              onClick={triggerPlayerRefresh}
              disabled={refreshingPlayer}
              className="gap-1.5"
            >
              {refreshingPlayer ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              Trigger refresh now
            </Button>
          </div>
        )}

        {playerResult && (
          <div className={`p-3 rounded-lg flex items-start gap-2 ${
            playerResult.success
              ? 'bg-green-500/10 border border-green-500/20'
              : 'bg-red-500/10 border border-red-500/20'
          }`}>
            {playerResult.success ? (
              <CheckCircle2 className="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
            ) : (
              <XCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
            )}
            <p className={`text-xs ${playerResult.success ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
              {playerResult.message}
            </p>
          </div>
        )}
      </div>

      <div className="space-y-4 p-4 rounded-lg border border-border">
        <div className="flex items-center justify-between">
          <div>
            <h4 className="font-medium text-sm">Autobrr</h4>
            <p className="text-xs text-muted-foreground mt-1">
              Optional monitoring integration for similar artists.
            </p>
          </div>
          <Button variant="outline" size="sm" asChild className="gap-1.5 shrink-0">
            <a href="https://autobrr.com" target="_blank" rel="noopener noreferrer">
              <ExternalLink className="w-3 h-3" />
              Docs
            </a>
          </Button>
        </div>

        <div className="space-y-3 pt-2 border-t border-border">
          <div className="space-y-2">
            <Label htmlFor="autobrr-url">Autobrr URL</Label>
            <Input
              id="autobrr-url"
              placeholder="http://192.168.1.100:7474"
              value={config.AUTOBRR_URL || ''}
              onChange={(e) => updateConfig({ AUTOBRR_URL: e.target.value })}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="autobrr-api-key">Autobrr API Key</Label>
            <PasswordInput
              id="autobrr-api-key"
              placeholder="Enter Autobrr API key"
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
              {testingAutobrr ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
              Test Autobrr connection
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
        </div>
      </div>
    </div>
  );
}
