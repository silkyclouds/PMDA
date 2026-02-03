import { useState, useEffect, useRef } from 'react';
import { CheckCircle2, XCircle, Loader2, Server, ChevronDown, List, LogIn, Copy, Check } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { PasswordInput } from '@/components/ui/password-input';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import { Checkbox } from '@/components/ui/checkbox';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import * as api from '@/lib/api';
import type { PMDAConfig, PlexServerEntry, PlexDatabasePathHint } from '@/lib/api';

interface PlexSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function PlexSettings({ config, updateConfig, errors }: PlexSettingsProps) {
  const [testing, setTesting] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [fetchingServers, setFetchingServers] = useState(false);
  const [servers, setServers] = useState<PlexServerEntry[]>([]);
  const [connectedViaPlexTv, setConnectedViaPlexTv] = useState(false);
  const [codeCopied, setCodeCopied] = useState(false);
  const [serversError, setServersError] = useState<string | null>(null);
  const [dbPathHints, setDbPathHints] = useState<PlexDatabasePathHint[]>([]);
  const [dbPathHintsLoaded, setDbPathHintsLoaded] = useState(false);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
    libraries?: Array<{ id: string; name: string }>;
  } | null>(null);
  const [pinDialogOpen, setPinDialogOpen] = useState(false);
  const [pinId, setPinId] = useState<number | null>(null);
  const [pinCode, setPinCode] = useState('');
  const [pinLinkUrl, setPinLinkUrl] = useState('https://www.plex.tv/link/');
  const [pinError, setPinError] = useState<string | null>(null);
  const [pinLoading, setPinLoading] = useState(false);
  const [plexUseHttp, setPlexUseHttp] = useState(true);
  const [verifyDbLoading, setVerifyDbLoading] = useState(false);
  const [verifyDbResult, setVerifyDbResult] = useState<{ success: boolean; message?: string } | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const plexPopupRef = useRef<Window | null>(null);
  const autoFetchedTokenRef = useRef<string | null>(null);

  /** Normalize URL for comparison (ignore http vs https). */
  const normalizeUrl = (url: string) => (url || '').replace(/^https?:\/\//i, '').toLowerCase();
  /** URL actually used for connection (http if "Use HTTP on LAN" is checked). */
  const getEffectivePlexHost = (): string => {
    const url = (config.PLEX_HOST ?? '').trim();
    if (!url) return '';
    if (!plexUseHttp) return url;
    return url.replace(/^https:\/\//i, 'http://');
  };

  const closePlexPopup = () => {
    try {
      if (plexPopupRef.current && !plexPopupRef.current.closed) {
        plexPopupRef.current.close();
      }
    } finally {
      plexPopupRef.current = null;
    }
  };

  const startPlexSignIn = async () => {
    setPinError(null);
    setPinId(null);
    setPinCode('');
    setCodeCopied(false);
    setConnectedViaPlexTv(false);
    closePlexPopup();
    setPinLoading(true);
    try {
      const result = await api.createPlexPin();
      if (!result.success || result.id == null) {
        setPinError(result.message || 'Failed to create PIN');
        setPinDialogOpen(true);
        return;
      }
      setPinId(result.id);
      setPinCode(result.code ?? '');
      const linkUrl = result.link_url || 'https://www.plex.tv/link/';
      setPinLinkUrl(linkUrl);
      setPinDialogOpen(true);
      setConnectedViaPlexTv(false);
      // Open Plex sign-in in a popup (plex.tv blocks embedding in iframe)
      plexPopupRef.current = window.open(
        linkUrl,
        'plex-signin',
        'width=520,height=640,scrollbars=yes,resizable=yes,menubar=no,toolbar=no'
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Could not reach PMDA or plex.tv.';
      setPinError(msg);
      setPinDialogOpen(true);
    } finally {
      setPinLoading(false);
    }
  };

  useEffect(() => {
    const url = (config.PLEX_HOST ?? '').trim();
    if (url && url.toLowerCase().startsWith('http://')) setPlexUseHttp(true);
  }, []);

  // Auto-fetch servers once when token is present (e.g. returning user or after Sign in with Plex.tv)
  useEffect(() => {
    const token = config.PLEX_TOKEN?.trim();
    if (!token || autoFetchedTokenRef.current === token) return;
    autoFetchedTokenRef.current = token;
    let cancelled = false;
    (async () => {
      try {
        const result = await api.getPlexServers(token);
        if (cancelled) return;
        if (result.success && Array.isArray(result.servers) && result.servers.length > 0) {
          setServers(result.servers);
          setServersError(null);
          const list = result.servers as PlexServerEntry[];
          const norm = (u: string) => (u || '').replace(/^https?:\/\//i, '').toLowerCase();
          const currentNorm = norm(config.PLEX_HOST || '');
          const hasMatch = list.some((s) => norm(s.uri) === currentNorm);
          if (!hasMatch && list[0]?.uri) {
            updateConfig({ PLEX_HOST: plexUseHttp ? list[0].uri.replace(/^https:\/\//i, 'http://') : list[0].uri });
          }
        }
      } catch {
        if (!cancelled) setServersError('Failed to fetch Plex servers.');
      }
    })();
    return () => { cancelled = true; };
  }, [config.PLEX_TOKEN]);

  useEffect(() => {
    if (!pinDialogOpen || pinId == null) return;
    pollRef.current = setInterval(async () => {
      const r = await api.pollPlexPin(pinId);
      if (r.status === 'linked' && r.token) {
        if (pollRef.current) clearInterval(pollRef.current);
        pollRef.current = null;
        closePlexPopup();
        updateConfig({ PLEX_TOKEN: r.token });
        const serversResult = await api.getPlexServers(r.token);
        if (serversResult.success && Array.isArray(serversResult.servers) && serversResult.servers.length > 0) {
          setServers(serversResult.servers);
          setServersError(null);
          const first = serversResult.servers[0];
          if (first?.uri)
            updateConfig({
              PLEX_HOST: plexUseHttp ? first.uri.replace(/^https:\/\//i, 'http://') : first.uri,
            });
        }
        setPinDialogOpen(false);
        setPinId(null);
        setConnectedViaPlexTv(true);
      } else if (r.status === 'expired' || (r.success === false && r.message)) {
        if (pollRef.current) clearInterval(pollRef.current);
        pollRef.current = null;
        closePlexPopup();
        setPinError(r.message || 'PIN expired');
      }
    }, 2000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = null;
      closePlexPopup();
    };
  }, [pinDialogOpen, pinId, updateConfig]);

  const fetchPlexServers = async () => {
    const token = config.PLEX_TOKEN?.trim();
    if (!token) return;
    setFetchingServers(true);
    setServersError(null);
    setServers([]);
    try {
      const result = await api.getPlexServers(token);
      if (result.success && Array.isArray(result.servers) && result.servers.length > 0) {
        setServers(result.servers);
        const list = result.servers as PlexServerEntry[];
        const norm = (u: string) => (u || '').replace(/^https?:\/\//i, '').toLowerCase();
        const currentNorm = norm(config.PLEX_HOST || '');
        const hasMatch = list.some((s) => norm(s.uri) === currentNorm);
        if (!hasMatch && list[0]?.uri) {
          updateConfig({ PLEX_HOST: list[0].uri });
        }
      } else {
        setServersError(result.message || 'No Plex servers found or invalid token.');
      }
    } catch {
      setServersError('Failed to fetch Plex servers.');
    } finally {
      setFetchingServers(false);
    }
  };

  const copyCodeToClipboard = async () => {
    if (!pinCode) return;
    // Clean the code (remove spaces that might be in the display)
    const cleanCode = pinCode.replace(/\s+/g, '');
    
    try {
      // Try modern Clipboard API first (requires HTTPS or localhost)
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(cleanCode);
        setCodeCopied(true);
        setTimeout(() => setCodeCopied(false), 2000);
        return;
      }
    } catch (err) {
      console.warn('Clipboard API failed, trying fallback:', err);
    }
    
    // Fallback: use execCommand for older browsers or when clipboard API fails
    try {
      const textArea = document.createElement('textarea');
      textArea.value = cleanCode;
      textArea.style.position = 'fixed';
      textArea.style.left = '-999999px';
      textArea.style.top = '-999999px';
      document.body.appendChild(textArea);
      textArea.focus();
      textArea.select();
      const successful = document.execCommand('copy');
      document.body.removeChild(textArea);
      
      if (successful) {
        setCodeCopied(true);
        setTimeout(() => setCodeCopied(false), 2000);
      } else {
        console.error('execCommand copy failed');
        // Show error to user
        setPinError('Failed to copy code. Please copy it manually.');
      }
    } catch (err) {
      console.error('Fallback copy failed:', err);
      setPinError('Failed to copy code. Please copy it manually.');
    }
  };

  const testConnection = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const url = getEffectivePlexHost();
      const result = await api.checkPlexConnection(url, config.PLEX_TOKEN ?? '');
      setTestResult(result);
    } catch (error) {
      setTestResult({ success: false, message: 'Connection test failed' });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <Server className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Plex Configuration</h3>
          <p className="text-sm text-muted-foreground">
            Connect to your Plex Media Server
          </p>
        </div>
      </div>

      {/* Sign in with Plex.tv first (Tautulli-style: no manual token) */}
      <div className="rounded-lg border bg-muted/30 p-4 space-y-3">
        <p className="text-sm font-medium">Connect with your Plex account</p>
        <p className="text-xs text-muted-foreground">
          Sign in via plex.tv — we get a token and list your servers automatically. No need to copy a token.
        </p>
        <Button
          type="button"
          onClick={startPlexSignIn}
          disabled={pinLoading}
          className="gap-2"
        >
          {pinLoading ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <LogIn className="w-4 h-4" />
          )}
          {pinLoading ? 'Connecting…' : 'Sign in with Plex.tv'}
        </Button>

        {/* Code to enter on plex.tv/link — popup opens automatically; user can use link if blocked */}
        {pinDialogOpen && pinCode && !pinError && (
          <div className="mt-4 pt-4 border-t border-border space-y-3">
            <p className="text-sm font-medium">Code to enter on plex.tv/link</p>
            <div className="flex flex-wrap items-center gap-3">
              <span className="text-3xl font-mono font-bold tracking-[0.3em] tabular-nums bg-primary text-primary-foreground px-5 py-3 rounded-lg">
                {pinCode}
              </span>
              <Button
                type="button"
                variant="outline"
                size="icon"
                onClick={copyCodeToClipboard}
                title="Copy code"
                className="h-12 w-12 shrink-0"
              >
                {codeCopied ? <Check className="w-5 h-5 text-green-600" /> : <Copy className="w-5 h-5" />}
              </Button>
              <Button asChild variant="outline" size="lg">
                <a href={pinLinkUrl} target="_blank" rel="noopener noreferrer">
                  Open plex.tv/link (if popup was blocked)
                </a>
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              A window has opened for Plex sign-in. Sign in if needed, enter the code above, then return here.
            </p>
          </div>
        )}
        {connectedViaPlexTv && config.PLEX_TOKEN && (
          <div className="mt-4 pt-4 border-t border-border flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
            <CheckCircle2 className="w-5 h-5 shrink-0" />
            <span>Connected via plex.tv</span>
          </div>
        )}
        {pinDialogOpen && pinError && (
          <div className="mt-4 pt-4 border-t border-border">
            <p className="text-sm text-destructive">{pinError}</p>
          </div>
        )}
      </div>

      <div className="space-y-4">
        {/* Server list: shown when we have servers (auto-fetched or after Sign in with Plex.tv) */}
        {Array.isArray(servers) && servers.length > 0 && (
          <div className="space-y-2">
            <p className="text-xs text-muted-foreground">
              {servers.length} Plex server(s) detected: {(servers as PlexServerEntry[]).map((s) => s.name).join(', ')}.
            </p>
            <Label>Choose a Plex server</Label>
            <div className="space-y-2 rounded-md border border-input p-3">
              {(servers as PlexServerEntry[]).map((s) => {
                const isSelected =
                  normalizeUrl(config.PLEX_HOST || '') === normalizeUrl(s.uri);
                return (
                  <label
                    key={`${s.name}-${s.uri}`}
                    className={`flex cursor-pointer items-start gap-3 rounded-md border px-3 py-2 transition-colors hover:bg-muted/50 ${
                      isSelected ? 'border-primary bg-primary/5 ring-1 ring-primary' : 'border-transparent'
                    }`}
                  >
                    <input
                      type="radio"
                      name="plex-server"
                      value={s.uri}
                      checked={isSelected}
                      onChange={() =>
                        updateConfig({
                          PLEX_HOST: plexUseHttp ? (s.uri || '').replace(/^https:\/\//i, 'http://') : s.uri,
                        })
                      }
                      className="mt-1 h-4 w-4 shrink-0"
                    />
                    <div className="min-w-0 flex-1">
                      <p className="font-medium text-foreground">{s.name}</p>
                      <p className="text-xs text-muted-foreground break-all">{s.uri}</p>
                    </div>
                  </label>
                );
              })}
            </div>
          </div>
        )}

        {/* Test Connection */}
        <div className="pt-2">
          <Button
            variant="secondary"
            onClick={testConnection}
            disabled={testing || !config.PLEX_HOST || !config.PLEX_TOKEN}
            className="gap-1.5"
          >
            {testing ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Server className="w-4 h-4" />
            )}
            Test Connection
          </Button>

          {testResult && (
            <div className={`mt-3 p-3 rounded-lg flex items-start gap-2 ${
              testResult.success ? 'bg-success/10' : 'bg-destructive/10'
            }`}>
              {testResult.success ? (
                <CheckCircle2 className="w-5 h-5 text-success flex-shrink-0" />
              ) : (
                <XCircle className="w-5 h-5 text-destructive flex-shrink-0" />
              )}
              <div>
                <p className={`text-sm font-medium ${
                  testResult.success ? 'text-success' : 'text-destructive'
                }`}>
                  {testResult.message}
                </p>
              </div>
            </div>
          )}
        </div>

        {/* Advanced Options: token, URL, DB base path, verify, DB filename, hints */}
        <Collapsible open={showAdvanced} onOpenChange={setShowAdvanced}>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" size="sm" className="gap-1.5 text-muted-foreground">
              <ChevronDown className={`w-4 h-4 transition-transform ${showAdvanced ? 'rotate-180' : ''}`} />
              Advanced Options
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="mt-4 space-y-4">
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="plex-token">Plex Token</Label>
                <FieldTooltip content="Filled automatically after Sign in with Plex.tv, or enter manually if you prefer." />
              </div>
              <PasswordInput
                id="plex-token"
                placeholder="Enter your Plex token"
                value={config.PLEX_TOKEN || ''}
                onChange={(e) => updateConfig({ PLEX_TOKEN: e.target.value })}
                className={errors.PLEX_TOKEN ? 'border-destructive' : ''}
              />
              {errors.PLEX_TOKEN && (
                <p className="text-sm text-destructive">{errors.PLEX_TOKEN}</p>
              )}
            </div>
            <div className="space-y-2">
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  variant="outline"
                  onClick={fetchPlexServers}
                  disabled={fetchingServers || !config.PLEX_TOKEN?.trim()}
                  className="gap-1.5"
                >
                  {fetchingServers ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <List className="w-4 h-4" />
                  )}
                  Fetch my servers
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Uses your token and plex.tv to list your servers (with their LAN URLs). If Plex uses a non-standard port, enter its URL below first.
              </p>
              {serversError && (
                <p className="text-sm text-destructive">{serversError}</p>
              )}
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="plex-host">Plex Server URL</Label>
                <FieldTooltip content="Filled by the list above or type manually (e.g. http://192.168.1.1:32400)." />
              </div>
              <Input
                id="plex-host"
                placeholder="http://192.168.1.100:32400"
                value={config.PLEX_HOST || ''}
                onChange={(e) => updateConfig({ PLEX_HOST: e.target.value })}
                className={errors.PLEX_HOST ? 'border-destructive' : ''}
              />
              <div className="flex items-center gap-2">
                <Checkbox
                  id="plex-use-http"
                  checked={plexUseHttp}
                  onCheckedChange={(checked) => {
                    const useHttp = checked === true;
                    setPlexUseHttp(useHttp);
                    const url = (config.PLEX_HOST ?? '').trim();
                    if (url) {
                      updateConfig({
                        PLEX_HOST: useHttp ? url.replace(/^https:\/\//i, 'http://') : url.replace(/^http:\/\//i, 'https://'),
                      });
                    }
                  }}
                />
                <Label htmlFor="plex-use-http" className="text-sm font-normal cursor-pointer">
                  Use HTTP on LAN (recommended if Plex has no SSL locally)
                </Label>
              </div>
              {errors.PLEX_HOST && (
                <p className="text-sm text-destructive">{errors.PLEX_HOST}</p>
              )}
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="plex-base-path">Plex installation base path (container)</Label>
                <FieldTooltip content="Base path of your Plex installation inside the container (e.g. /database if you mount Plex config root at /database). PMDA will search for the database on first startup and save the resolved path." />
              </div>
              <Input
                id="plex-base-path"
                placeholder="/database"
                value={config.PLEX_BASE_PATH ?? ''}
                onChange={(e) => updateConfig({ PLEX_BASE_PATH: e.target.value })}
                className={errors.PLEX_BASE_PATH ? 'border-destructive' : ''}
              />
              {errors.PLEX_BASE_PATH && (
                <p className="text-sm text-destructive">{errors.PLEX_BASE_PATH}</p>
              )}
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="plex-db-path-resolved">Database path (resolved)</Label>
                <FieldTooltip content="Path where PMDA found the Plex database. Filled automatically on startup when using a base path. Save the base path above and restart to re-detect." />
              </div>
              <Input
                id="plex-db-path-resolved"
                placeholder="(detected on startup)"
                value={config.PLEX_DB_PATH || ''}
                readOnly
                className="bg-muted font-mono text-sm"
              />
              {errors.PLEX_DB_PATH && (
                <p className="text-sm text-destructive">{errors.PLEX_DB_PATH}</p>
              )}
            </div>
            <div className="space-y-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={verifyDbLoading}
                onClick={async () => {
                  setVerifyDbResult(null);
                  setVerifyDbLoading(true);
                  try {
                    const r = await api.verifyPlexDb();
                    setVerifyDbResult(r);
                  } finally {
                    setVerifyDbLoading(false);
                  }
                }}
              >
                {verifyDbLoading ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span className="ml-2">Verifying...</span>
                  </>
                ) : (
                  'Verify Plex DB access'
                )}
              </Button>
              {verifyDbResult && (
                <p className={`text-sm ${verifyDbResult.success ? 'text-green-600 dark:text-green-400' : 'text-destructive'}`}>
                  {verifyDbResult.success ? 'Plex database is readable.' : (verifyDbResult.message ?? 'Verification failed.')}
                </p>
              )}
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="plex-db-file">Database Filename</Label>
                <FieldTooltip content="The Plex database filename. Usually 'com.plexapp.plugins.library.db' - only change if you have a custom setup." />
              </div>
              <Input
                id="plex-db-file"
                placeholder="com.plexapp.plugins.library.db"
                value={config.PLEX_DB_FILE || 'com.plexapp.plugins.library.db'}
                onChange={(e) => updateConfig({ PLEX_DB_FILE: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label className="text-muted-foreground">Common database locations (by platform)</Label>
              {!dbPathHintsLoaded && (
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={async () => {
                    const r = await api.getPlexDatabasePaths();
                    if (r.success && Array.isArray(r.paths) && r.paths.length) setDbPathHints(r.paths);
                    setDbPathHintsLoaded(true);
                  }}
                >
                  Show common paths
                </Button>
              )}
              {Array.isArray(dbPathHints) && dbPathHints.length > 0 && (
                <div className="rounded-md border bg-muted/30 p-2 text-xs max-h-48 overflow-auto space-y-1.5">
                  {(dbPathHints as PlexDatabasePathHint[]).map((h, i) => (
                    <div key={i}>
                      <span className="font-medium text-foreground">{h.platform}</span>
                      <pre className="mt-0.5 whitespace-pre-wrap break-all text-muted-foreground">{h.path}</pre>
                      {h.note && <p className="text-muted-foreground italic">{h.note}</p>}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>

      {/* Plex code displayed inline in the same step */}
    </div>
  );
}
