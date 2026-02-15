import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { matchPath, useLocation, useNavigate } from 'react-router-dom';
import { Bot, Loader2, Send, Sparkles } from 'lucide-react';

import { cn } from '@/lib/utils';
import * as api from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet';
import { useToast } from '@/hooks/use-toast';

type AssistantContext = { artist_id?: number; context_inferred?: boolean; [k: string]: unknown };

type AssistantLink = {
  kind: 'internal' | 'external';
  label: string;
  href: string;
  entity_type?: string;
  entity_id?: number;
  thumb?: string | null;
};

function epochToTime(ts: number): string {
  if (!ts) return '';
  try {
    const d = new Date(ts * 1000);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

function getCitations(meta: unknown): api.AssistantCitation[] {
  if (!meta || typeof meta !== 'object') return [];
  const obj = meta as Record<string, unknown>;
  const c = obj.citations;
  if (!Array.isArray(c)) return [];
  return c
    .map((x) => (x && typeof x === 'object' ? (x as api.AssistantCitation) : null))
    .filter((x): x is api.AssistantCitation => Boolean(x && typeof x.entity_type === 'string'));
}

function getLinks(meta: unknown): AssistantLink[] {
  if (!meta || typeof meta !== 'object') return [];
  const obj = meta as Record<string, unknown>;
  const raw = obj.links;
  if (!Array.isArray(raw)) return [];
  return raw
    .map((x) => (x && typeof x === 'object' ? (x as AssistantLink) : null))
    .filter((x): x is AssistantLink => Boolean(x && typeof x.label === 'string' && typeof x.href === 'string'));
}

export function AssistantDock({ bottomOffsetPx = 16 }: { bottomOffsetPx?: number }) {
  const { toast } = useToast();
  const location = useLocation();
  const navigate = useNavigate();

  const match = useMemo(() => matchPath({ path: '/library/artist/:artistId' }, location.pathname), [location.pathname]);
  const contextArtistId = useMemo(() => {
    const raw = match?.params?.artistId;
    const id = raw ? Number(raw) : 0;
    return Number.isFinite(id) && id > 0 ? id : 0;
  }, [match?.params?.artistId]);

  const [open, setOpen] = useState(false);
  const [status, setStatus] = useState<api.AssistantStatus | null>(null);
  const [sessionId, setSessionId] = useState<string>(() => {
    try {
      return localStorage.getItem('pmda_assistant_session_id') || '';
    } catch {
      return '';
    }
  });
  const [messages, setMessages] = useState<api.AssistantMessage[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [sending, setSending] = useState(false);
  const [draft, setDraft] = useState('');
  const [contextLabel, setContextLabel] = useState<string>('');

  const inputRef = useRef<HTMLInputElement | null>(null);
  const panelInputRef = useRef<HTMLInputElement | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  const assistantOnline = Boolean(status?.ai_ready && status?.postgres_ready);
  const offlineReason = useMemo(() => {
    if (!status) return 'Assistant unavailable (status endpoint failed)';
    if (!status.postgres_ready) return 'PostgreSQL is not ready (check PMDA_PG_* settings)';
    if (!status.ai_ready) return (status.ai_error || 'AI is not ready (check API key + model)') as string;
    return '';
  }, [status]);

  const context: AssistantContext = useMemo(() => {
    return contextArtistId > 0 ? { artist_id: contextArtistId } : {};
  }, [contextArtistId]);

  const loadStatus = useCallback(async () => {
    try {
      const s = await api.getAssistantStatus();
      setStatus(s);
    } catch {
      setStatus(null);
    }
  }, []);

  const loadHistory = useCallback(async () => {
    if (!sessionId) return;
    setLoadingHistory(true);
    try {
      const res = await api.getAssistantSession(sessionId, 200);
      setMessages(Array.isArray(res.messages) ? res.messages : []);
    } catch {
      // If the session is missing/old, keep local empty and let POST create a new one.
      setMessages([]);
    } finally {
      setLoadingHistory(false);
    }
  }, [sessionId]);

  const resolveContextLabel = useCallback(async () => {
    if (contextArtistId <= 0) {
      setContextLabel('');
      return;
    }
    try {
      const res = await fetch(`/api/library/artist/${encodeURIComponent(String(contextArtistId))}`);
      const data = (await res.json().catch(() => ({}))) as { artist_name?: string };
      if (res.ok && data?.artist_name) setContextLabel(String(data.artist_name));
      else setContextLabel(`Artist #${contextArtistId}`);
    } catch {
      setContextLabel(`Artist #${contextArtistId}`);
    }
  }, [contextArtistId]);

  useEffect(() => {
    void loadStatus();
    const t = setInterval(() => {
      void loadStatus();
    }, 60_000);
    return () => clearInterval(t);
  }, [loadStatus]);

  useEffect(() => {
    if (!open) return;
    void loadStatus();
    void loadHistory();
  }, [open, loadStatus, loadHistory]);

  useEffect(() => {
    // Update context label opportunistically (fast path, backend is cached).
    if (!open) return;
    void resolveContextLabel();
  }, [open, resolveContextLabel]);

  useEffect(() => {
    if (!open) return;
    const t = setTimeout(() => {
      panelInputRef.current?.focus();
    }, 80);
    return () => clearTimeout(t);
  }, [open]);

  useEffect(() => {
    // Autoscroll to bottom on new messages.
    if (!open) return;
    const el = scrollRef.current;
    if (!el) return;
    // Allow layout to paint first.
    const t = setTimeout(() => {
      el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
    }, 50);
    return () => clearTimeout(t);
  }, [open, messages.length]);

  const persistSessionId = (sid: string) => {
    setSessionId(sid);
    try {
      localStorage.setItem('pmda_assistant_session_id', sid);
    } catch {
      // ignore
    }
  };

  const resetSession = () => {
    setMessages([]);
    setSessionId('');
    try {
      localStorage.removeItem('pmda_assistant_session_id');
    } catch {
      // ignore
    }
  };

  const submit = useCallback(async () => {
    const text = draft.trim();
    if (!text || sending) return;
    setSending(true);

    const optimisticId = Date.now();
    const optimisticUser: api.AssistantMessage = {
      id: optimisticId,
      role: 'user',
      content: text,
      created_at: Math.floor(Date.now() / 1000),
      context,
      metadata: {},
    };
    setMessages((prev) => [...prev, optimisticUser]);
    setDraft('');

    try {
      const res = await api.postAssistantChat({
        message: text,
        session_id: sessionId || undefined,
        context,
      });
      if (res?.session_id) persistSessionId(res.session_id);

      // Replace the optimistic user message with the server version when possible.
      setMessages((prev) => {
        const next = [...prev];
        const idx = next.findIndex((m) => m.id === optimisticId);
        if (idx >= 0) next[idx] = res.user_message;
        else next.push(res.user_message);
        next.push(res.assistant_message);
        return next;
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Assistant request failed';
      toast({ title: 'Assistant error', description: msg, variant: 'destructive' });
      setMessages((prev) => prev.filter((m) => m.id !== optimisticId));
      setDraft(text);
    } finally {
      setSending(false);
    }
  }, [context, draft, sending, sessionId, toast]);

  const pill = (
    <div
      className={cn(
        'fixed right-4 z-[70] w-[min(420px,calc(100vw-2rem))]',
        'pointer-events-auto',
      )}
      style={{ bottom: bottomOffsetPx }}
    >
      <div
        className={cn(
          'group relative flex items-center gap-2 rounded-full border bg-background/75 backdrop-blur-md shadow-[0_10px_40px_rgba(0,0,0,0.18)]',
          'dark:bg-zinc-950/60',
          'border-border/60',
          'px-3 py-2',
        )}
      >
        <div className="relative">
          <Sparkles className={cn('h-4 w-4', assistantOnline ? 'text-amber-500' : 'text-muted-foreground')} />
          <span
            className={cn(
              'absolute -right-1 -bottom-1 h-2 w-2 rounded-full border border-background',
              assistantOnline ? 'bg-emerald-500' : 'bg-zinc-500',
            )}
            title={assistantOnline ? 'Assistant online' : 'Assistant offline'}
          />
        </div>
        <Input
          ref={inputRef}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onFocus={() => setOpen(true)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault();
              setOpen(true);
              void submit();
            }
          }}
          placeholder={assistantOnline ? 'Ask PMDA…' : offlineReason || 'Assistant unavailable'}
          disabled={!assistantOnline || sending}
          className={cn(
            'h-9 border-0 bg-transparent px-0 shadow-none focus-visible:ring-0',
            'placeholder:text-muted-foreground/80',
          )}
        />
        <Button
          type="button"
          size="icon"
          variant="ghost"
          className="h-9 w-9 rounded-full"
          onClick={() => {
            setOpen(true);
            void submit();
          }}
          disabled={!assistantOnline || sending || !draft.trim()}
          title="Send"
        >
          {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
        </Button>
      </div>
    </div>
  );

  return (
    <>
      {!open ? pill : null}
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="right" className="w-full sm:max-w-xl p-0">
          <div className="flex h-full flex-col">
            <SheetHeader className="p-6 pb-3 border-b border-border/60">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <SheetTitle className="flex items-center gap-2">
                    <Bot className="h-5 w-5 text-primary" />
                    <span className="truncate">PMDA Intelligence</span>
                  </SheetTitle>
                  <div className="mt-2 flex flex-wrap items-center gap-2">
                    <Badge variant={assistantOnline ? 'secondary' : 'outline'}>
                      {assistantOnline ? 'Online' : 'Offline'}
                    </Badge>
                    {status?.ai_provider ? (
                      <Badge variant="outline" className="max-w-full truncate">
                        {status.ai_provider} {status.ai_model ? `· ${status.ai_model}` : ''}
                      </Badge>
                    ) : null}
                    {contextArtistId > 0 ? (
                      <Badge variant="outline" className="max-w-full truncate">
                        Context: {contextLabel || `Artist #${contextArtistId}`}
                      </Badge>
                    ) : null}
                    {contextArtistId > 0 ? (
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        className="h-7 px-2"
                        onClick={() => navigate(`/library/artist/${contextArtistId}`)}
                      >
                        Open
                      </Button>
                    ) : null}
                    <Button type="button" size="sm" variant="ghost" className="h-7 px-2" onClick={resetSession} title="Start a new chat">
                      New chat
                    </Button>
                  </div>
                  {!assistantOnline && offlineReason ? (
                    <div className="mt-2 text-xs text-muted-foreground">
                      {offlineReason}
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="mt-3 flex flex-wrap gap-2">
                {contextArtistId > 0 ? (
                  <>
                    <Button type="button" size="sm" variant="secondary" className="h-8" onClick={() => setDraft('Quels sont les alias (AKA) de cet artiste ?')}>
                      Aliases / AKA
                    </Button>
                    <Button type="button" size="sm" variant="secondary" className="h-8" onClick={() => setDraft('Quels albums de cet artiste sont dans ma collection locale ?')}>
                      Albums (local)
                    </Button>
                    <Button type="button" size="sm" variant="secondary" className="h-8" onClick={() => setDraft('Quels artistes similaires dois-je ecouter ensuite ?')}>
                      Similar
                    </Button>
                  </>
                ) : (
                  <>
                    <Button type="button" size="sm" variant="secondary" className="h-8" onClick={() => setDraft('Recommande-moi 10 morceaux a partir de ce que j ecoute souvent.')}>
                      Recommendations
                    </Button>
                    <Button type="button" size="sm" variant="secondary" className="h-8" onClick={() => setDraft('Quels sont mes artistes les plus presents dans ma bibliotheque ?')}>
                      My library
                    </Button>
                  </>
                )}
              </div>
            </SheetHeader>

            <div className="flex-1 min-h-0">
              <ScrollArea className="h-full" viewportRef={scrollRef}>
                <div className="p-6 space-y-4">
                  {loadingHistory ? (
                    <div className="flex items-center gap-2 text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Loading conversation…
                    </div>
                  ) : null}

                  {messages.length === 0 && !loadingHistory ? (
                    <div className="rounded-xl border border-border/60 bg-muted/30 p-4 text-sm text-muted-foreground">
                      Ask questions about your local library. On artist pages, the assistant uses the current artist context automatically.
                    </div>
                  ) : null}

                  {messages.map((m) => {
                    const isUser = m.role === 'user';
                    const citations = getCitations(m.metadata);
                    const links = getLinks(m.metadata);
                    return (
                      <div key={m.id} className={cn('flex', isUser ? 'justify-end' : 'justify-start')}>
                        <div className={cn('max-w-[85%] space-y-2', isUser ? 'items-end' : 'items-start')}>
                          <div
                            className={cn(
                              'rounded-2xl px-4 py-3 text-sm leading-relaxed shadow-sm',
                              isUser
                                ? 'bg-primary text-primary-foreground rounded-br-md'
                                : 'bg-muted text-foreground rounded-bl-md',
                            )}
                          >
                            <div className="whitespace-pre-wrap">{m.content}</div>
                          </div>
                          <div className={cn('flex items-center gap-2 text-[11px] text-muted-foreground', isUser ? 'justify-end' : 'justify-start')}>
                            {epochToTime(m.created_at)}
                          </div>

                          {!isUser && links.length > 0 ? (
                            <div className="flex flex-wrap gap-2">
                              {links.slice(0, 12).map((l) => {
                                const thumb = (l.thumb || '').trim();
                                const pillContent = (
                                  <>
                                    <span className="flex h-7 w-7 items-center justify-center overflow-hidden rounded-full border border-border/60 bg-muted shrink-0">
                                      {thumb ? (
                                        <img
                                          src={thumb}
                                          alt={l.label}
                                          className="h-full w-full object-cover animate-in fade-in-0 duration-300"
                                          loading="lazy"
                                        />
                                      ) : (
                                        <Sparkles className="h-3.5 w-3.5 text-muted-foreground" />
                                      )}
                                    </span>
                                    <span className="truncate">{l.label}</span>
                                  </>
                                );

                                if (l.kind === 'internal') {
                                  return (
                                    <button
                                      key={`${l.kind}:${l.href}:${l.label}`}
                                      type="button"
                                      className={cn(
                                        'inline-flex max-w-full items-center gap-2 rounded-full border border-border/60 bg-background/70 px-3 py-1 text-xs',
                                        'hover:bg-accent transition-colors',
                                      )}
                                      onClick={() => navigate(l.href)}
                                      title={l.href}
                                    >
                                      {pillContent}
                                    </button>
                                  );
                                }

                                return (
                                  <a
                                    key={`${l.kind}:${l.href}:${l.label}`}
                                    className={cn(
                                      'inline-flex max-w-full items-center gap-2 rounded-full border border-border/60 bg-background/70 px-3 py-1 text-xs',
                                      'hover:bg-accent transition-colors',
                                    )}
                                    href={l.href}
                                    target="_blank"
                                    rel="noreferrer"
                                    title={l.href}
                                  >
                                    {pillContent}
                                  </a>
                                );
                              })}
                            </div>
                          ) : null}

                          {!isUser && citations.length > 0 ? (
                            <div className="rounded-xl border border-border/60 bg-background/60 p-3 space-y-2">
                              <div className="text-[11px] font-medium text-muted-foreground">Sources used</div>
                              <div className="space-y-2">
                                {citations.slice(0, 4).map((c) => (
                                  <button
                                    key={`${c.chunk_id}`}
                                    type="button"
                                    className="w-full text-left rounded-lg px-2 py-1.5 hover:bg-accent transition-colors"
                                    onClick={() => {
                                      if (c.entity_type === 'artist' && c.entity_id) navigate(`/library/artist/${c.entity_id}`);
                                    }}
                                  >
                                    <div className="flex items-center justify-between gap-2">
                                      <div className="text-xs font-medium truncate">
                                        {c.title || `${c.entity_type} #${c.entity_id}`}
                                      </div>
                                      <Badge variant="outline" className="shrink-0 text-[10px]">
                                        {c.source || 'unknown'}
                                      </Badge>
                                    </div>
                                    <div
                                      className="mt-1 text-[11px] text-muted-foreground"
                                      style={{
                                        display: '-webkit-box',
                                        WebkitLineClamp: 2,
                                        WebkitBoxOrient: 'vertical',
                                        overflow: 'hidden',
                                      }}
                                    >
                                      {c.snippet}
                                    </div>
                                  </button>
                                ))}
                              </div>
                            </div>
                          ) : null}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </ScrollArea>
            </div>

            <div className="border-t border-border/60 p-4">
              <div className="flex items-end gap-2">
                <Input
                  ref={panelInputRef}
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  placeholder={assistantOnline ? 'Ask PMDA…' : 'Assistant unavailable'}
                  disabled={!assistantOnline || sending}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault();
                      void submit();
                    }
                  }}
                  className="h-11"
                />
                <Button type="button" className="h-11 px-4" disabled={!assistantOnline || sending || !draft.trim()} onClick={() => void submit()}>
                  {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
                </Button>
              </div>
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </>
  );
}
