import { useCallback, useEffect, useState } from 'react';
import { Bot, Copy, KeyRound, Loader2, RefreshCw, ShieldCheck, ShieldOff } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type { McpStatusSummary, PMDAConfig } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

type Props = {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
};

function formatTimestamp(epoch?: number | null): string {
  if (!epoch || !Number.isFinite(Number(epoch))) return 'never';
  return new Date(Number(epoch) * 1000).toLocaleString();
}

function formatAgo(epoch?: number | null): string {
  if (!epoch || !Number.isFinite(Number(epoch))) return 'never';
  const diff = Math.max(0, Date.now() / 1000 - Number(epoch));
  if (diff < 60) return `${Math.max(1, Math.round(diff))}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  return `${Math.round(diff / 86400)}d ago`;
}

export function McpAccessSettings({ config, updateConfig }: Props) {
  const enabled = Boolean(config.MCP_ENABLED);
  const [status, setStatus] = useState<McpStatusSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState('');
  const [newToken, setNewToken] = useState('');

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setStatus(await api.getMcpStatus());
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to load MCP status');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh, enabled]);

  const rotateToken = useCallback(async () => {
    setBusy('rotate');
    try {
      const response = await api.rotateMcpToken(['read', 'scan_control', 'runtime_repair', 'review_propose']);
      setNewToken(response.token);
      setStatus(response.mcp);
      toast.success('MCP token rotated');
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to rotate MCP token');
    } finally {
      setBusy('');
    }
  }, []);

  const revokeToken = useCallback(async () => {
    setBusy('revoke');
    try {
      const response = await api.revokeMcpToken();
      setNewToken('');
      setStatus(response.mcp);
      toast.success('MCP token revoked');
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to revoke MCP token');
    } finally {
      setBusy('');
    }
  }, []);

  const copyToken = useCallback(async () => {
    if (!newToken) return;
    try {
      await navigator.clipboard.writeText(newToken);
      toast.success('MCP token copied');
    } catch {
      toast.error('Clipboard copy failed');
    }
  }, [newToken]);

  const activeToken = status?.active_token || null;
  const scopes = activeToken?.scopes?.length ? activeToken.scopes : status?.scopes || [];
  const tokenActive = Boolean(enabled && status?.enabled && activeToken?.active);
  const audit = status?.audit || [];

  return (
    <Card className="border-cyan-500/20 bg-cyan-500/[0.04]">
      <CardHeader>
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div className="space-y-2">
            <CardTitle className="flex items-center gap-2 text-base">
              <Bot className="h-4 w-4 text-cyan-300" />
              MCP agent access
            </CardTitle>
            <CardDescription>
              Local stdio access for Codex, Cursor, Claude Code and similar agents. Disabled tokens cannot call PMDA while the switch is off.
            </CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant={enabled ? 'default' : 'secondary'} className={enabled ? 'bg-cyan-500/20 text-cyan-100' : ''}>
              {enabled ? 'On' : 'Off'}
            </Badge>
            <Button type="button" size="sm" variant="outline" onClick={() => void refresh()} disabled={loading}>
              {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-start justify-between gap-4 rounded-lg border border-border/60 bg-background/40 p-3">
          <div className="space-y-1">
            <Label className="flex items-center gap-2">
              {enabled ? <ShieldCheck className="h-4 w-4 text-cyan-300" /> : <ShieldOff className="h-4 w-4 text-muted-foreground" />}
              Enable MCP access to PMDA
            </Label>
            <p className="text-xs leading-5 text-muted-foreground">
              Off means every MCP tool call returns <span className="font-mono">mcp_disabled</span>, even with a previously issued token.
            </p>
          </div>
          <Switch checked={enabled} onCheckedChange={(checked) => updateConfig({ MCP_ENABLED: checked })} />
        </div>

        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-lg border border-border/60 bg-background/40 p-3">
            <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Status</div>
            <div className="mt-1 text-sm font-semibold">{tokenActive ? 'Enabled with active token' : enabled ? 'Enabled, no active token' : 'Disabled'}</div>
          </div>
          <div className="rounded-lg border border-border/60 bg-background/40 p-3">
            <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Last access</div>
            <div className="mt-1 text-sm font-semibold">{formatAgo(status?.last_access_at || activeToken?.last_used_at)}</div>
          </div>
          <div className="rounded-lg border border-border/60 bg-background/40 p-3">
            <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Scopes</div>
            <div className="mt-2 flex flex-wrap gap-1">
              {scopes.length ? scopes.map((scope) => (
                <Badge key={scope} variant="outline" className="text-[10px]">{scope}</Badge>
              )) : <span className="text-xs text-muted-foreground">none</span>}
            </div>
          </div>
        </div>

        <div className="rounded-lg border border-border/60 bg-background/40 p-3">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div className="space-y-1">
              <div className="flex items-center gap-2 text-sm font-medium">
                <KeyRound className="h-4 w-4 text-cyan-300" />
                Token management
              </div>
              <p className="text-xs text-muted-foreground">
                Active token: <span className="font-mono">{activeToken?.token_id || 'none'}</span>
                {activeToken?.created_at ? <span> · created {formatTimestamp(activeToken.created_at)}</span> : null}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button type="button" size="sm" variant="outline" onClick={() => void rotateToken()} disabled={busy === 'rotate'}>
                {busy === 'rotate' ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
                Rotate token
              </Button>
              <Button type="button" size="sm" variant="outline" onClick={() => void revokeToken()} disabled={!activeToken || busy === 'revoke'}>
                {busy === 'revoke' ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
                Revoke
              </Button>
            </div>
          </div>

          {newToken ? (
            <div className="mt-3 rounded-md border border-cyan-500/30 bg-cyan-950/40 p-3">
              <div className="mb-2 text-xs font-semibold text-cyan-100">New token, shown once</div>
              <div className="flex gap-2">
                <code className="min-w-0 flex-1 overflow-x-auto rounded bg-black/30 px-2 py-1 text-xs text-cyan-50">{newToken}</code>
                <Button type="button" size="sm" variant="outline" onClick={() => void copyToken()}>
                  <Copy className="h-3.5 w-3.5" />
                </Button>
              </div>
              <p className="mt-2 text-[11px] text-cyan-100/75">
                Stdio bridge: <span className="font-mono">PMDA_MCP_TOKEN=... python -m pmda_mcp.server</span>
              </p>
            </div>
          ) : null}
        </div>

        <div className="rounded-lg border border-border/60 bg-background/40 p-3">
          <div className="mb-2 flex items-center justify-between">
            <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Audit log</div>
            <span className="text-[11px] text-muted-foreground">{audit.length} recent</span>
          </div>
          {audit.length ? (
            <div className="space-y-1">
              {audit.slice(0, 5).map((entry) => (
                <div key={entry.audit_id} className="flex flex-col gap-1 rounded border border-border/40 bg-black/10 px-2 py-1 text-xs md:flex-row md:items-center md:justify-between">
                  <span className="font-mono text-foreground">{entry.tool || 'unknown'}</span>
                  <span className="text-muted-foreground">{entry.status} · {entry.duration_ms}ms · {formatAgo(entry.created_at)}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-xs text-muted-foreground">No MCP tool calls audited yet.</div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
