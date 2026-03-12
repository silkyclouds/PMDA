import { useState, useEffect, useRef, useCallback, type ComponentType } from 'react';
import { Save, Loader2, Check, FolderOutput, RefreshCw, X, Database, Sparkles, ExternalLink, Copy, MapPin, ChevronDown, AlertTriangle, Trash2, SlidersHorizontal, Workflow } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import { IntegrationsSettings } from '@/components/settings/IntegrationsSettings';
import { SchedulerSettings } from '@/components/settings/SchedulerSettings';
import { SourcesAutonomySettings } from '@/components/settings/SourcesAutonomySettings';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ProviderIcon } from '@/components/providers/ProviderIcon';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { normalizeConfigForUI } from '@/lib/configUtils';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { PasswordInput } from '@/components/ui/password-input';
import { Slider } from '@/components/ui/slider';
import { Progress } from '@/components/ui/progress';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';
import { Switch } from '@/components/ui/switch';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';

type SettingsSection = {
  id: string;
  label: string;
  icon: ComponentType<{ className?: string }>;
  navClass: string;
  navActiveClass: string;
  cardClass: string;
  iconClass: string;
};

const SETTINGS_SECTIONS: SettingsSection[] = [
  {
    id: 'settings-files-export',
    label: 'Folders',
    icon: FolderOutput,
    navClass: 'border-cyan-300/60 bg-cyan-500/10 text-cyan-800 hover:bg-cyan-500/20 dark:border-cyan-500/30 dark:text-cyan-100',
    navActiveClass: 'ring-1 ring-cyan-400/40 bg-cyan-500/20 dark:bg-cyan-500/25',
    cardClass: 'border-cyan-500/20 bg-cyan-500/[0.04]',
    iconClass: 'bg-cyan-500/20 text-cyan-700 dark:text-cyan-200',
  },
  {
    id: 'settings-scan-behavior',
    label: 'Scan behavior',
    icon: SlidersHorizontal,
    navClass: 'border-indigo-300/60 bg-indigo-500/10 text-indigo-800 hover:bg-indigo-500/20 dark:border-indigo-500/30 dark:text-indigo-100',
    navActiveClass: 'ring-1 ring-indigo-400/40 bg-indigo-500/20 dark:bg-indigo-500/25',
    cardClass: 'border-indigo-500/20 bg-indigo-500/[0.04]',
    iconClass: 'bg-indigo-500/20 text-indigo-700 dark:text-indigo-200',
  },
  {
    id: 'settings-pipeline',
    label: 'Pipeline',
    icon: Sparkles,
    navClass: 'border-violet-300/60 bg-violet-500/10 text-violet-800 hover:bg-violet-500/20 dark:border-violet-500/30 dark:text-violet-100',
    navActiveClass: 'ring-1 ring-violet-400/40 bg-violet-500/20 dark:bg-violet-500/25',
    cardClass: 'border-violet-500/20 bg-violet-500/[0.04]',
    iconClass: 'bg-violet-500/20 text-violet-700 dark:text-violet-200',
  },
  {
    id: 'settings-ai',
    label: 'AI',
    icon: Sparkles,
    navClass: 'border-fuchsia-300/60 bg-fuchsia-500/10 text-fuchsia-800 hover:bg-fuchsia-500/20 dark:border-fuchsia-500/30 dark:text-fuchsia-100',
    navActiveClass: 'ring-1 ring-fuchsia-400/40 bg-fuchsia-500/20 dark:bg-fuchsia-500/25',
    cardClass: 'border-fuchsia-500/20 bg-fuchsia-500/[0.04]',
    iconClass: 'bg-fuchsia-500/20 text-fuchsia-700 dark:text-fuchsia-200',
  },
  {
    id: 'settings-providers',
    label: 'Metadata providers',
    icon: Database,
    navClass: 'border-sky-300/60 bg-sky-500/10 text-sky-800 hover:bg-sky-500/20 dark:border-sky-500/30 dark:text-sky-100',
    navActiveClass: 'ring-1 ring-sky-400/40 bg-sky-500/20 dark:bg-sky-500/25',
    cardClass: 'border-sky-500/20 bg-sky-500/[0.04]',
    iconClass: 'bg-sky-500/20 text-sky-700 dark:text-sky-200',
  },
  {
    id: 'settings-concerts',
    label: 'Concerts',
    icon: MapPin,
    navClass: 'border-amber-300/60 bg-amber-500/10 text-amber-800 hover:bg-amber-500/20 dark:border-amber-500/30 dark:text-amber-100',
    navActiveClass: 'ring-1 ring-amber-400/40 bg-amber-500/20 dark:bg-amber-500/25',
    cardClass: 'border-amber-500/20 bg-amber-500/[0.04]',
    iconClass: 'bg-amber-500/20 text-amber-700 dark:text-amber-200',
  },
  {
    id: 'settings-danger-zone',
    label: 'Danger zone',
    icon: AlertTriangle,
    navClass: 'border-rose-300/60 bg-rose-500/10 text-rose-800 hover:bg-rose-500/20 dark:border-rose-500/30 dark:text-rose-100',
    navActiveClass: 'ring-1 ring-rose-400/40 bg-rose-500/20 dark:bg-rose-500/25',
    cardClass: 'border-destructive/30 bg-destructive/[0.04]',
    iconClass: 'bg-destructive/20 text-destructive',
  },
];

const SETTINGS_SECTION_MAP: Record<string, SettingsSection> = SETTINGS_SECTIONS.reduce<Record<string, SettingsSection>>(
  (acc, section) => {
    acc[section.id] = section;
    return acc;
  },
  {},
);

function getSettingsSection(id: string): SettingsSection {
  return SETTINGS_SECTION_MAP[id] || SETTINGS_SECTIONS[0]!;
}

type MaintenanceResetAction = api.MaintenanceResetAction;
type DangerPreset = 'reset_all_keep_settings';

type DangerPresetMeta = {
  id: DangerPreset;
  title: string;
  description: string;
  buttonLabel: string;
  resetActions: MaintenanceResetAction[];
};

const AI_USAGE_LEVELS: Array<{
  value: NonNullable<PMDAConfig['AI_USAGE_LEVEL']>;
  label: string;
  description: string;
}> = [
  {
    value: 'limited',
    label: 'Limited',
    description: 'AI only for truly ambiguous cases; no vision, no web MBID search, no AI duplicate arbitration.',
  },
  {
    value: 'medium',
    label: 'Medium',
    description: 'Balanced cost/accuracy: AI match verification + provider identity assistance; vision and web MBID stay disabled.',
  },
  {
    value: 'aggressive',
    label: 'Aggressive',
    description: 'Maximum AI usage: verification + matching + vision + web MBID + duplicate arbitration.',
  },
];

type AIProviderId = 'openai-api' | 'openai-codex' | 'anthropic' | 'google' | 'ollama';

const AI_PROVIDER_OPTIONS: Array<{ value: AIProviderId; label: string; provider: string; codexOnly?: boolean }> = [
  { value: 'openai-api', label: 'OpenAI API (key)', provider: 'openai-api' },
  { value: 'openai-codex', label: 'OpenAI Codex (ChatGPT OAuth)', provider: 'openai-codex', codexOnly: true },
  { value: 'anthropic', label: 'Anthropic', provider: 'anthropic' },
  { value: 'google', label: 'Google', provider: 'google' },
  { value: 'ollama', label: 'Ollama (local)', provider: 'ollama' },
];

const DANGER_PRESETS: DangerPresetMeta[] = [
  {
    id: 'reset_all_keep_settings',
    title: 'Reset PMDA (keep settings)',
    description:
      'Clears NVMe media cache, resets cache.db, resets state.db, clears PostgreSQL files index, and empties the visible library until the next full scan. Settings, users, and provider keys are preserved.',
    buttonLabel: 'Reset PMDA now',
    resetActions: ['media_cache', 'cache_db', 'state_db', 'files_index'],
  },
];

function parsePathListValue(value: unknown): string[] {
  const normalizeFolderPath = (input: string): string => {
    const raw = String(input || '').trim();
    if (!raw) return '';
    if (raw === '/') return '/';
    return raw.replace(/\/+$/, '') || raw;
  };

  const out: string[] = [];
  const seen = new Set<string>();
  const queue: unknown[] = [value];

  while (queue.length > 0) {
    const item = queue.shift();
    if (item == null) continue;
    if (Array.isArray(item)) {
      queue.push(...item);
      continue;
    }
    if (typeof item === 'string') {
      const s = item.trim();
      if (!s) continue;
      if (s.startsWith('[') || s.startsWith('"')) {
        try {
          const parsed = JSON.parse(s) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // Fall back to CSV split.
        }
      }
      if (s.includes(',')) {
        const parts = s.split(',').map((p) => p.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      const normalized = normalizeFolderPath(s);
      if (normalized && !seen.has(normalized) && !normalized.startsWith('[')) {
        seen.add(normalized);
        out.push(normalized);
      }
      continue;
    }
    const s = normalizeFolderPath(String(item));
    if (s && !seen.has(s) && !s.startsWith('[')) {
      seen.add(s);
      out.push(s);
    }
  }

  return out;
}

function SettingsPage() {
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [activeSettingsSection, setActiveSettingsSection] = useState<string>('settings-files-export');
  const [openaiOAuth, setOpenaiOAuth] = useState<{
    sessionId: string;
    verificationUrl: string;
    userCode: string;
    intervalSec: number;
    status: 'pending' | 'completed' | 'error';
    message?: string;
    warning?: string;
    apiKeySaved?: boolean;
  } | null>(null);
  const [openaiOAuthBusy, setOpenaiOAuthBusy] = useState(false);
  const [openaiModelsLoading, setOpenaiModelsLoading] = useState(false);
  const [openaiModels, setOpenaiModels] = useState<string[]>([]);
  const [openaiModelsError, setOpenaiModelsError] = useState<string | null>(null);
  const [openaiCodexStatus, setOpenaiCodexStatus] = useState<api.OpenAICodexOAuthStatusResponse | null>(null);
  const [providerPreferences, setProviderPreferences] = useState<api.AIProviderPreferencesResponse | null>(null);
  const [providerPreferencesBusy, setProviderPreferencesBusy] = useState(false);
  const [rebuildIndexLoading, setRebuildIndexLoading] = useState(false);
  const [advancedFoldersOpen, setAdvancedFoldersOpen] = useState(false);
  const [schedulerAdvancedOpen, setSchedulerAdvancedOpen] = useState(false);
  const [providersChecking, setProvidersChecking] = useState(false);
  const [providersPreflight, setProvidersPreflight] = useState<api.ScanPreflightResult | null>(null);
  const [providersPreflightAt, setProvidersPreflightAt] = useState<number | null>(null);
  const [dangerDialogOpen, setDangerDialogOpen] = useState(false);
  const [pendingDangerPreset, setPendingDangerPreset] = useState<DangerPreset | null>(null);
  const [dangerBusyPreset, setDangerBusyPreset] = useState<DangerPreset | null>(null);
  const [isRestarting, setIsRestarting] = useState(false);
  const [rebootCountdown, setRebootCountdown] = useState(35);
  const [rebootProgress, setRebootProgress] = useState(0);
  const openaiModelsKeyRef = useRef<string>('');
  const openaiApiModeEnabled = config.OPENAI_ENABLE_API_KEY_MODE !== false;
  const openaiCodexModeEnabled = config.OPENAI_ENABLE_CODEX_OAUTH_MODE !== false;
  const codexProfileConnected = Boolean(openaiCodexStatus?.profile_connected || openaiCodexStatus?.connected);
  const codexReady = Boolean(openaiCodexStatus?.connected);

  const getApiErrorMessage = (e: unknown): string | null => {
    const bodyMsg = (e as { body?: { message?: unknown } } | null)?.body?.message;
    if (typeof bodyMsg === 'string' && bodyMsg.trim()) return bodyMsg.trim();
    return null;
  };

  const copyTextToClipboard = async (text: string): Promise<boolean> => {
    const clean = String(text ?? '');
    if (!clean) return false;
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(clean);
        return true;
      }
    } catch {
      // Fall back below (HTTP/non-secure contexts).
    }
    try {
      const el = document.createElement('textarea');
      el.value = clean;
      el.setAttribute('readonly', 'true');
      el.style.position = 'fixed';
      el.style.left = '-9999px';
      el.style.top = '0';
      document.body.appendChild(el);
      el.focus();
      el.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(el);
      return Boolean(ok);
    } catch {
      return false;
    }
  };

  const refreshProviderStatus = useCallback(async () => {
    setProvidersChecking(true);
    try {
      const status = await api.getProvidersPreflight();
      setProvidersPreflight(status);
      setProvidersPreflightAt(Date.now());
      toast.success('Provider checks completed');
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Provider checks failed'));
    } finally {
      setProvidersChecking(false);
    }
  }, [getApiErrorMessage]);

  useEffect(() => {
    loadConfig();
  }, []);

  useEffect(() => {
    const pickActiveFromHash = () => {
      const raw = window.location.hash?.replace('#', '').trim();
      if (!raw) return;
      const exists = SETTINGS_SECTIONS.some((section) => section.id === raw);
      if (exists) setActiveSettingsSection(raw);
    };
    pickActiveFromHash();
    window.addEventListener('hashchange', pickActiveFromHash);
    return () => window.removeEventListener('hashchange', pickActiveFromHash);
  }, []);

  // Fetch the curated list of compatible OpenAI models for the dropdown (avoids manual typing).
  useEffect(() => {
    if (!openaiApiModeEnabled) {
      openaiModelsKeyRef.current = '';
      setOpenaiModels([]);
      setOpenaiModelsError(null);
      setOpenaiModelsLoading(false);
      return;
    }
    const key = (config.OPENAI_API_KEY || '').trim();
    // Avoid calling the endpoint on every keystroke while user is typing.
    if (!key || !key.startsWith('sk-') || key.length < 20) {
      openaiModelsKeyRef.current = '';
      setOpenaiModels([]);
      setOpenaiModelsError(null);
      setOpenaiModelsLoading(false);
      return;
    }
    if (openaiModelsKeyRef.current === key) return;
    openaiModelsKeyRef.current = key;
    setOpenaiModelsLoading(true);
    setOpenaiModelsError(null);
    (async () => {
      try {
        const models = await api.getOpenAIModels(key);
        setOpenaiModels(Array.isArray(models) ? models : []);
      } catch (e: unknown) {
        const msg = getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to fetch OpenAI models');
        setOpenaiModels([]);
        setOpenaiModelsError(msg);
      } finally {
        setOpenaiModelsLoading(false);
      }
    })();
  }, [config.OPENAI_API_KEY, openaiApiModeEnabled]);

  const refreshOpenAICodexStatus = useCallback(async (checkRuntime = false) => {
    try {
      const status = await api.getOpenAICodexOAuthStatus({ checkRuntime });
      setOpenaiCodexStatus(status);
      return status;
    } catch {
      return null;
    }
  }, []);

  const loadOptionalProviderState = useCallback(async () => {
    try {
      const [codexStatus, prefs] = await Promise.all([
        refreshOpenAICodexStatus(false),
        api.getAIProviderPreferences().catch(() => null),
      ]);
      if (codexStatus) setOpenaiCodexStatus(codexStatus);
      if (prefs) setProviderPreferences(prefs);
    } catch {
      // Keep Settings responsive even if optional provider checks fail.
    }
  }, [refreshOpenAICodexStatus]);

  const loadConfig = async () => {
    setIsLoading(true);
    try {
      const data = await api.getConfig();
      setConfig(normalizeConfigForUI(data));
      setIsLoading(false);
      void loadOptionalProviderState();
      return;
    } catch (error) {
      console.error('Failed to load config:', error);
      toast.error('Failed to load configuration');
    } finally {
      setIsLoading(false);
    }
  };

  const startOpenAIOAuth = useCallback(async () => {
    setOpenaiOAuthBusy(true);
    try {
      const res = await api.startOpenAIDeviceOAuth();
      if (!res.ok || !res.session_id || !res.verification_url || !res.user_code) {
        toast.error(res.message || 'Failed to start OpenAI OAuth');
        return;
      }
      setOpenaiOAuth({
        sessionId: res.session_id,
        verificationUrl: res.verification_url,
        userCode: res.user_code,
        intervalSec: typeof res.interval === 'number' ? res.interval : 5,
        status: 'pending',
        message: res.message,
        warning: res.warning,
      });
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to start OpenAI OAuth'));
    } finally {
      setOpenaiOAuthBusy(false);
    }
  }, []);

  const disconnectOpenAIOAuth = useCallback(async () => {
    setOpenaiOAuthBusy(true);
    try {
      const result = await api.disconnectOpenAICodexOAuth();
      if (!result.ok) {
        throw new Error(result.message || 'Failed to disconnect OpenAI Codex OAuth');
      }
      setOpenaiOAuth(null);
      toast.success(result.message || 'OpenAI Codex OAuth disconnected');
      await loadConfig();
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to disconnect OpenAI OAuth'));
    } finally {
      setOpenaiOAuthBusy(false);
    }
  }, [loadConfig]);

  const pollOpenAIOAuth = useCallback(async () => {
    if (!openaiOAuth?.sessionId || openaiOAuthBusy) return;
    setOpenaiOAuthBusy(true);
    try {
      const res = await api.pollOpenAIDeviceOAuth(openaiOAuth.sessionId);
      if (res.status === 'completed') {
        const saved = typeof res.api_key_saved === 'boolean' ? res.api_key_saved : undefined;
        setOpenaiOAuth((prev) => prev ? { ...prev, status: 'completed', message: res.message, apiKeySaved: saved } : prev);
        if (saved === false) {
          toast.info(res.message || 'Connected, but API key not saved');
        } else {
          toast.success(res.message || 'OpenAI connected');
        }
        await loadConfig();
        return;
      }
      if (res.status === 'error') {
        setOpenaiOAuth((prev) => prev ? { ...prev, status: 'error', message: res.message } : prev);
        toast.error(res.message || 'OpenAI OAuth failed');
        return;
      }
      setOpenaiOAuth((prev) => prev ? { ...prev, status: 'pending', message: res.message } : prev);
    } catch (e) {
      const message = getApiErrorMessage(e) || (e instanceof Error ? e.message : 'OpenAI OAuth poll failed');
      setOpenaiOAuth((prev) => prev ? { ...prev, status: 'error', message } : prev);
      toast.error(message);
    } finally {
      setOpenaiOAuthBusy(false);
    }
  }, [loadConfig, openaiOAuth?.sessionId, openaiOAuthBusy]);

  const saveProviderPreferences = useCallback(
    async (patch: Partial<Pick<api.AIProviderPreferencesResponse, 'interactive_provider_id' | 'batch_provider_id' | 'web_search_provider_id'>>) => {
      const current = providerPreferences || {
        interactive_provider_id: 'openai-codex',
        batch_provider_id: 'openai-codex',
        web_search_provider_id: 'openai-codex',
      };
      const payload = {
        interactive_provider_id: String(patch.interactive_provider_id || current.interactive_provider_id || 'openai-codex'),
        batch_provider_id: String(patch.batch_provider_id || current.batch_provider_id || 'openai-codex'),
        web_search_provider_id: String(patch.web_search_provider_id || current.web_search_provider_id || 'openai-codex'),
      };
      setProviderPreferencesBusy(true);
      setProviderPreferences((prev) => ({ ...(prev || current), ...payload }));
      try {
        const saved = await api.saveAIProviderPreferences(payload);
        setProviderPreferences(saved);
        toast.success('AI provider preferences saved');
        await loadConfig();
      } catch (e) {
        toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to save provider preferences'));
        await loadConfig();
      } finally {
        setProviderPreferencesBusy(false);
      }
    },
    [getApiErrorMessage, loadConfig, providerPreferences],
  );

  useEffect(() => {
    if (!openaiOAuth || openaiOAuth.status !== 'pending') return;
    // Poll in the background while the user completes the flow in another tab.
    const delayMs = Math.max(1000, Math.min(5000, (openaiOAuth.intervalSec || 5) * 1000));
    const t = setTimeout(() => {
      pollOpenAIOAuth();
    }, delayMs);
    return () => clearTimeout(t);
  }, [openaiOAuth, pollOpenAIOAuth]);

  const pendingSaveRef = useRef<Partial<PMDAConfig>>({});
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [lastSaved, setLastSaved] = useState<boolean | null>(null);

  const flushSave = useCallback(async () => {
    const toSave = pendingSaveRef.current;
    pendingSaveRef.current = {};
    if (Object.keys(toSave).length === 0) return;
    try {
      await api.saveConfig(toSave);
      setLastSaved(true);
      setTimeout(() => setLastSaved(null), 4000);
      toast.success('Setting saved', { duration: 2500 });
    } catch (e) {
      console.error('Auto-save failed:', e);
      toast.error('Failed to save setting');
    }
  }, []);

  const updateConfig = useCallback((updates: Partial<PMDAConfig>) => {
    setConfig(prev => ({ ...prev, ...updates }));
    setErrors(prev => {
      const next = { ...prev };
      Object.keys(updates).forEach(key => delete next[key as keyof typeof next]);
      return next;
    });
    pendingSaveRef.current = { ...pendingSaveRef.current, ...updates };
    if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    debounceTimerRef.current = setTimeout(() => {
      debounceTimerRef.current = null;
      flushSave();
    }, 350);
  }, [flushSave]);
  const filesRoots = parsePathListValue(config.FILES_ROOTS);
  const selectedAiLevel = (() => {
    const raw = String(config.AI_USAGE_LEVEL || 'medium').trim().toLowerCase();
    return AI_USAGE_LEVELS.find((lvl) => lvl.value === raw)?.value || 'medium';
  })();
  const selectedAiLevelIndex = Math.max(0, AI_USAGE_LEVELS.findIndex((lvl) => lvl.value === selectedAiLevel));
  const selectedAiLevelMeta = AI_USAGE_LEVELS[selectedAiLevelIndex] || AI_USAGE_LEVELS[1];
  const schedulerPaused = config.SCHEDULER_PAUSED !== false;
  const allowNonScanJobs = Boolean(config.SCHEDULER_ALLOW_NON_SCAN_JOBS ?? false);
  const postScanAsync = Boolean(config.PIPELINE_POST_SCAN_ASYNC ?? false);
  const scanFirstModeEnabled = schedulerPaused && !allowNonScanJobs && !postScanAsync;

  type ProviderState = {
    variant: 'default' | 'secondary' | 'destructive' | 'outline';
    label: string;
    message: string;
  };

  const providerState = (
    key: 'discogs' | 'lastfm' | 'fanart' | 'serper' | 'acoustid',
    configured: boolean,
  ): ProviderState => {
    if (!configured) {
      return { variant: 'outline', label: 'Not configured', message: 'No credentials set.' };
    }
    if (!providersPreflight) {
      return { variant: 'secondary', label: 'Configured', message: 'Click “Check keys” to verify credentials.' };
    }
    const result = providersPreflight[key];
    if (!result) {
      return { variant: 'secondary', label: 'Configured', message: 'No check result available yet.' };
    }
    if (result.ok) {
      return { variant: 'default', label: 'Valid', message: result.message || 'Credentials look good.' };
    }
    return { variant: 'destructive', label: 'Issue', message: result.message || 'Credential check failed.' };
  };

  const toggleScanFirstMode = useCallback((enabled: boolean) => {
    if (enabled) {
      updateConfig({
        SCHEDULER_PAUSED: true,
        SCHEDULER_ALLOW_NON_SCAN_JOBS: false,
        PIPELINE_POST_SCAN_ASYNC: false,
      });
      toast.success('Scan-first mode enabled');
      return;
    }
    updateConfig({ SCHEDULER_PAUSED: false });
    toast.success('Scheduled scans re-enabled');
  }, [updateConfig]);

  const handleRebuildFilesIndex = useCallback(async () => {
    if (rebuildIndexLoading) return;
    if (filesRoots.length === 0) {
      toast.error('Configure at least one music folder before rebuilding the index.');
      return;
    }
    setRebuildIndexLoading(true);
    try {
      const res = await api.postLibraryFilesIndexRebuild();
      if (res.status === 'started') {
        toast.success('Library index rebuild started from configured music folders.');
      } else if (res.status === 'already_running') {
        toast.info('A library index rebuild is already running.');
      } else {
        toast.info(res.message || 'Library index rebuild request sent.');
      }
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to start library index rebuild'));
    } finally {
      setRebuildIndexLoading(false);
    }
  }, [filesRoots.length, getApiErrorMessage, rebuildIndexLoading]);

  const activeDangerMeta = DANGER_PRESETS.find((item) => item.id === pendingDangerPreset) || null;

  const openDangerConfirm = useCallback((action: DangerPreset) => {
    if (dangerBusyPreset) return;
    setPendingDangerPreset(action);
    setDangerDialogOpen(true);
  }, [dangerBusyPreset]);

  const runDangerAction = useCallback(async () => {
    if (!pendingDangerPreset || dangerBusyPreset) return;
    const preset = DANGER_PRESETS.find((item) => item.id === pendingDangerPreset) || null;
    if (!preset) return;
    setDangerBusyPreset(preset.id);
    try {
      const result = await api.runMaintenanceReset({
        actions: preset.resetActions,
        restart: true,
      });
      if (result.status === 'blocked') {
        toast.error(result.message || 'Stop the running scan before using Danger Zone actions.');
        return;
      }
      if (result.status === 'partial' || (result.errors && result.errors.length > 0)) {
        toast.error(result.message || 'Maintenance action completed with errors.');
      } else {
        toast.success(result.message || 'Maintenance action completed.');
      }
      if (result.restart_initiated) {
        setDangerDialogOpen(false);
        setPendingDangerPreset(null);
        setRebootCountdown(35);
        setRebootProgress(0);
        setIsRestarting(true);
        return;
      }
      setDangerDialogOpen(false);
      setPendingDangerPreset(null);
      await loadConfig();
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Maintenance action failed'));
    } finally {
      setDangerBusyPreset(null);
    }
  }, [dangerBusyPreset, getApiErrorMessage, pendingDangerPreset, loadConfig]);

  useEffect(() => {
    if (!isRestarting) return;
    const totalMs = 35_000;
    const startedAt = Date.now();
    const timer = setInterval(() => {
      const elapsed = Date.now() - startedAt;
      const remaining = Math.max(0, totalMs - elapsed);
      const seconds = Math.ceil(remaining / 1000);
      const progress = Math.min(100, (elapsed / totalMs) * 100);
      setRebootCountdown(seconds);
      setRebootProgress(progress);
      if (remaining <= 0) {
        clearInterval(timer);
        window.location.reload();
      }
    }, 100);
    return () => clearInterval(timer);
  }, [isRestarting]);

  useEffect(() => {
    return () => {
      if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    };
  }, []);

  const saveConfig = async () => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = null;
    }
    pendingSaveRef.current = {};
    setIsSaving(true);
    try {
      const configToSave = Object.fromEntries(
        Object.entries(config).filter(([_, v]) => v !== undefined)
      );
      const result = await api.saveConfig(configToSave);
      if (result.restart_initiated) {
        toast.success('Configuration saved successfully! The container will restart automatically.', { duration: 3000 });
        setTimeout(() => window.location.reload(), 3000);
      } else {
        toast.success(result.message || 'Settings saved successfully', { duration: 2000 });
        setIsSaving(false);
        loadConfig();
      }
    } catch (error) {
      console.error('Failed to save config:', error);
      setErrors({ save: 'Failed to save configuration. Please try again.' });
      toast.error('Failed to save configuration');
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="container mx-auto p-6 max-w-5xl">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold">Settings</h1>
            <p className="text-muted-foreground mt-1">
              Changes are saved automatically. No restart needed.
            </p>
          </div>
          <div className="flex items-center gap-2">
            {lastSaved === true && (
              <span className="text-sm font-medium text-green-600 dark:text-green-400 flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-green-500/10">
                <Check className="w-4 h-4" /> Saved
              </span>
            )}
            <Button onClick={saveConfig} disabled={isSaving} variant="outline" size="sm" className="gap-2">
              {isSaving ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Saving...
                </>
              ) : (
                <>
                  <Save className="w-4 h-4" />
                  Save all now
                </>
              )}
            </Button>
          </div>
        </div>

        {errors.save && (
          <div className="p-4 bg-destructive/10 border border-destructive/20 rounded-md text-destructive text-sm mb-6">
            {errors.save}
          </div>
        )}

        {/* Mobile: horizontal scroll nav */}
        <nav className="md:hidden mb-4 -mx-6 px-6 overflow-x-auto pb-3 border-b border-border" aria-label="Settings sections">
          <div className="flex gap-2 min-w-max">
            {SETTINGS_SECTIONS.map(({ id, label, icon: Icon, navClass, navActiveClass }) => (
              <a
                key={id}
                href={`#${id}`}
                onClick={() => setActiveSettingsSection(id)}
                className={`shrink-0 flex items-center gap-1.5 py-2 px-3 rounded-lg border text-sm transition-colors whitespace-nowrap ${navClass} ${activeSettingsSection === id ? navActiveClass : ''}`}
              >
                <Icon className="w-3.5 h-3.5" />
                {label}
              </a>
            ))}
          </div>
        </nav>

        <div className="flex gap-8">
          {/* Desktop: sticky left sidebar with anchors */}
          <nav className="hidden md:block shrink-0 w-56 top-24 self-start sticky rounded-xl border border-border bg-card/50 p-3 shadow-sm" aria-label="Settings sections">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">Settings map</p>
            <ul className="space-y-1.5">
              {SETTINGS_SECTIONS.map(({ id, label, icon: Icon, navClass, navActiveClass }) => (
                <li key={id}>
                  <a
                    href={`#${id}`}
                    onClick={() => setActiveSettingsSection(id)}
                    className={`flex items-center gap-2 py-2 px-2.5 rounded-lg border text-sm transition-colors ${navClass} ${activeSettingsSection === id ? navActiveClass : ''}`}
                  >
                    <Icon className="w-4 h-4" />
                    <span>{label}</span>
                  </a>
                </li>
              ))}
            </ul>
          </nav>

          <div className="min-w-0 flex-1 space-y-6">
            <Card id="settings-files-export" className={`scroll-mt-24 ${getSettingsSection('settings-files-export').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-files-export').iconClass}`}>
                    <FolderOutput className="w-4 h-4" />
                  </span>
                  Folders
                </CardTitle>
                <CardDescription>
                  Configure scan roots, incoming/library roles, cache folders, and export destinations in one place.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2 rounded-lg border border-border p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="space-y-1">
                      <Label>Library index</Label>
                      <p className="text-xs text-muted-foreground">
                        Rebuild indexed files from currently enabled source roots.
                      </p>
                    </div>
                    <AlertDialog>
                      <AlertDialogTrigger asChild>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="gap-2 shrink-0"
                          disabled={filesRoots.length === 0 || rebuildIndexLoading}
                        >
                          {rebuildIndexLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                          Rebuild index
                        </Button>
                      </AlertDialogTrigger>
                      <AlertDialogContent className="max-w-lg">
                        <AlertDialogHeader>
                          <AlertDialogTitle>Rebuild library index from scratch?</AlertDialogTitle>
                          <AlertDialogDescription>
                            This clears files-library rows and rebuilds from the enabled source roots below.
                          </AlertDialogDescription>
                        </AlertDialogHeader>
                        <div className="rounded-md border border-border bg-muted/30 p-3 text-xs text-muted-foreground space-y-1">
                          <p className="font-medium text-foreground">Current roots</p>
                          {filesRoots.map((rootPath) => (
                            <p key={`rebuild-${rootPath}`} className="font-mono truncate" title={rootPath}>{rootPath}</p>
                          ))}
                        </div>
                        <AlertDialogFooter>
                          <AlertDialogCancel disabled={rebuildIndexLoading}>Cancel</AlertDialogCancel>
                          <AlertDialogAction onClick={handleRebuildFilesIndex} disabled={rebuildIndexLoading}>
                            {rebuildIndexLoading ? (
                              <span className="inline-flex items-center gap-2">
                                <Loader2 className="w-4 h-4 animate-spin" />
                                Rebuilding...
                              </span>
                            ) : (
                              'Yes, rebuild from scratch'
                            )}
                          </AlertDialogAction>
                        </AlertDialogFooter>
                      </AlertDialogContent>
                    </AlertDialog>
                  </div>
                </div>
                <div className="space-y-2">
                  <Label>Media cache folder</Label>
                  <p className="text-xs text-muted-foreground">
                    NVMe cache for instant artist/album artwork rendering (thumbnails pre-generated by PMDA).
                  </p>
                  <FolderBrowserInput
                    value={config.MEDIA_CACHE_ROOT ?? '/config/media_cache'}
                    onChange={(path) => updateConfig({ MEDIA_CACHE_ROOT: path || '/config/media_cache' })}
                    placeholder="/config/media_cache"
                    selectLabel="Select media cache folder"
                  />
                  <Collapsible open={advancedFoldersOpen} onOpenChange={setAdvancedFoldersOpen}>
                    <div className="rounded-md border border-border/70 bg-muted/20">
                      <CollapsibleTrigger asChild>
                        <Button
                          type="button"
                          variant="ghost"
                          className="w-full justify-between rounded-none px-3 py-2 text-left"
                        >
                          <span className="text-sm font-medium">Advanced cache settings</span>
                          <ChevronDown className={`w-4 h-4 transition-transform ${advancedFoldersOpen ? 'rotate-180' : ''}`} />
                        </Button>
                      </CollapsibleTrigger>
                      <CollapsibleContent>
                        <div className="p-3 space-y-3 border-t border-border/60">
                          <div className="flex items-start justify-between gap-3">
                            <div className="space-y-1">
                              <Label htmlFor="artwork-ram-auto">Auto RAM tuning</Label>
                              <p className="text-[11px] text-muted-foreground">
                                PMDA recalculates artwork RAM cache from available memory every few minutes. You can cap it below.
                              </p>
                            </div>
                            <Switch
                              id="artwork-ram-auto"
                              checked={Boolean(config.ARTWORK_RAM_CACHE_AUTO ?? true)}
                              onCheckedChange={(checked) => updateConfig({ ARTWORK_RAM_CACHE_AUTO: checked })}
                            />
                          </div>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <div className="space-y-1">
                              <Label htmlFor="artwork-ram-auto-max-mb">Auto max RAM (MB)</Label>
                              <Input
                                id="artwork-ram-auto-max-mb"
                                type="number"
                                min={0}
                                max={65536}
                                step={256}
                                value={config.ARTWORK_RAM_CACHE_AUTO_MAX_MB ?? 0}
                                onChange={(e) => {
                                  const v = Number(e.target.value);
                                  updateConfig({ ARTWORK_RAM_CACHE_AUTO_MAX_MB: Number.isFinite(v) ? Math.max(0, Math.min(65536, Math.round(v))) : 0 });
                                }}
                              />
                              <p className="text-[11px] text-muted-foreground">Set `16384` to reserve max 16 GB for PMDA artwork cache. `0` = no cap.</p>
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor="artwork-ram-auto-interval">Auto tune interval (sec)</Label>
                              <Input
                                id="artwork-ram-auto-interval"
                                type="number"
                                min={30}
                                max={3600}
                                step={30}
                                value={config.ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC ?? 120}
                                onChange={(e) => {
                                  const v = Number(e.target.value);
                                  updateConfig({ ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC: Number.isFinite(v) ? Math.max(30, Math.min(3600, Math.round(v))) : 120 });
                                }}
                              />
                            </div>
                          </div>
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                            <div className="space-y-1">
                              <Label htmlFor="artwork-ram-cache-mb">Artwork RAM cache (MB)</Label>
                              <Input
                                id="artwork-ram-cache-mb"
                                type="number"
                                min={0}
                                max={65536}
                                step={128}
                                value={config.ARTWORK_RAM_CACHE_MB ?? 1024}
                                onChange={(e) => {
                                  const v = Number(e.target.value);
                                  updateConfig({ ARTWORK_RAM_CACHE_MB: Number.isFinite(v) ? Math.max(0, Math.min(65536, Math.round(v))) : 1024 });
                                }}
                              />
                              <p className="text-[11px] text-muted-foreground">Manual baseline/fallback. When auto tuning is ON, PMDA adjusts this value dynamically.</p>
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor="artwork-ram-cache-ttl">Artwork cache TTL (sec)</Label>
                              <Input
                                id="artwork-ram-cache-ttl"
                                type="number"
                                min={60}
                                max={2592000}
                                step={60}
                                value={config.ARTWORK_RAM_CACHE_TTL_SEC ?? 21600}
                                onChange={(e) => {
                                  const v = Number(e.target.value);
                                  updateConfig({ ARTWORK_RAM_CACHE_TTL_SEC: Number.isFinite(v) ? Math.max(60, Math.min(2592000, Math.round(v))) : 21600 });
                                }}
                              />
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor="artwork-ram-cache-item-mb">Max image in RAM (MB)</Label>
                              <Input
                                id="artwork-ram-cache-item-mb"
                                type="number"
                                min={1}
                                max={64}
                                step={1}
                                value={config.ARTWORK_RAM_CACHE_MAX_ITEM_MB ?? 8}
                                onChange={(e) => {
                                  const v = Number(e.target.value);
                                  updateConfig({ ARTWORK_RAM_CACHE_MAX_ITEM_MB: Number.isFinite(v) ? Math.max(1, Math.min(64, Math.round(v))) : 8 });
                                }}
                              />
                            </div>
                          </div>
                        </div>
                      </CollapsibleContent>
                    </div>
                  </Collapsible>
                </div>
                <div className="space-y-1">
                  <Label>Incomplete albums folder</Label>
                  <p className="text-xs text-muted-foreground">
                    Destination used by the incomplete step. Default is <span className="font-mono">/dupes/incomplete_albums</span>.
                  </p>
                  <FolderBrowserInput
                    value={config.INCOMPLETE_ALBUMS_TARGET_DIR ?? '/dupes/incomplete_albums'}
                    onChange={(path) => updateConfig({ INCOMPLETE_ALBUMS_TARGET_DIR: path || '/dupes/incomplete_albums' })}
                    placeholder="/dupes/incomplete_albums"
                    selectLabel="Select incomplete albums destination folder"
                  />
                </div>
                <div className="space-y-1">
                  <Label>Duplicates folder</Label>
                  <p className="text-xs text-muted-foreground">
                    Destination used by the dedupe step. Default is <span className="font-mono">/dupes</span>, but you can choose another writable folder.
                  </p>
                  <FolderBrowserInput
                    value={config.DUPE_ROOT ?? '/dupes'}
                    onChange={(path) => updateConfig({ DUPE_ROOT: path || '/dupes' })}
                    placeholder="/dupes"
                    selectLabel="Select duplicates destination folder"
                  />
                </div>
                <Separator />
                <div className="space-y-2">
                  <Label>Library export folder</Label>
                  <p className="text-xs text-muted-foreground">
                    Destination used by the export step when enabled.
                  </p>
                  <FolderBrowserInput
                    value={config.EXPORT_ROOT ?? '/music/library'}
                    onChange={(path) => updateConfig({ EXPORT_ROOT: path || '/music/library' })}
                    placeholder="/music/library"
                    selectLabel="Select library export destination folder"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Export strategy</Label>
                  <p className="text-xs text-muted-foreground">
                    Choose how PMDA writes files to the export library folder.
                  </p>
                  <Select
                    value={(config.EXPORT_LINK_STRATEGY as 'hardlink' | 'symlink' | 'copy' | 'move' | undefined) ?? 'hardlink'}
                    onValueChange={(value: 'hardlink' | 'symlink' | 'copy' | 'move') => updateConfig({ EXPORT_LINK_STRATEGY: value })}
                  >
                    <SelectTrigger className="w-full md:w-[320px]">
                      <SelectValue placeholder="Select strategy" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="hardlink">Hardlink (fast, no extra space)</SelectItem>
                      <SelectItem value="symlink">Symlink (keeps original files)</SelectItem>
                      <SelectItem value="copy">Copy (duplicates files)</SelectItem>
                      <SelectItem value="move">Move (relocate files)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Separator />
                <div className="space-y-3">
                  <div className="space-y-1">
                    <Label>Source folders (Library / Incoming)</Label>
                    <p className="text-xs text-muted-foreground">
                      Add all scan roots here, set each one as Library or Incoming, and pick the primary Library destination.
                    </p>
                  </div>
                  <SourcesAutonomySettings />
                </div>
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-scan-behavior" className={`scroll-mt-24 ${getSettingsSection('settings-scan-behavior').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-scan-behavior').iconClass}`}>
                    <SlidersHorizontal className="w-4 h-4" />
                  </span>
                  Scan behavior
                </CardTitle>
                <CardDescription>
                  Recommended mode is scan-first: PMDA runs pipeline inside the scan and avoids random background jobs.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="rounded-lg border border-border p-4 space-y-3">
                  <div className="flex items-start justify-between gap-3 rounded-md bg-muted/40 p-3">
                    <div className="space-y-1">
                      <Label>Scan-first mode (recommended)</Label>
                      <p className="text-xs text-muted-foreground">
                        Disable scheduler jobs and run pipeline during manual scans only.
                      </p>
                    </div>
                    <Switch
                      checked={scanFirstModeEnabled}
                      onCheckedChange={(checked) => toggleScanFirstMode(Boolean(checked))}
                    />
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div className="rounded-md border border-border/60 px-3 py-2">
                      <p className="text-xs text-muted-foreground">Scheduled scans</p>
                      <p className="text-sm font-medium">{schedulerPaused ? 'Paused' : 'Enabled'}</p>
                    </div>
                    <div className="rounded-md border border-border/60 px-3 py-2">
                      <p className="text-xs text-muted-foreground">Non-scan background jobs</p>
                      <p className="text-sm font-medium">{allowNonScanJobs ? 'Allowed' : 'Disabled'}</p>
                    </div>
                    <div className="rounded-md border border-border/60 px-3 py-2">
                      <p className="text-xs text-muted-foreground">Post-scan chain</p>
                      <p className="text-sm font-medium">{postScanAsync ? 'Async queue' : 'Inline in scan'}</p>
                    </div>
                  </div>
                </div>

                <div className="rounded-lg border border-border p-4 space-y-3">
                  <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
                    <div className="space-y-1">
                      <Label>Allow scheduled scans</Label>
                      <p className="text-xs text-muted-foreground">
                        If disabled, PMDA will only scan when you launch a scan manually.
                      </p>
                    </div>
                    <Switch
                      checked={!schedulerPaused}
                      onCheckedChange={(checked) => updateConfig({ SCHEDULER_PAUSED: !Boolean(checked) })}
                    />
                  </div>
                  <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
                    <div className="space-y-1">
                      <Label>Allow non-scan background jobs</Label>
                      <p className="text-xs text-muted-foreground">
                        Allows scheduler to run enrichment/dedupe/export outside scan runs.
                      </p>
                    </div>
                    <Switch
                      checked={allowNonScanJobs}
                      onCheckedChange={(checked) => updateConfig({ SCHEDULER_ALLOW_NON_SCAN_JOBS: Boolean(checked) })}
                    />
                  </div>
                  <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
                    <div className="space-y-1">
                      <Label>Post-scan chain in async queue</Label>
                      <p className="text-xs text-muted-foreground">
                        Disabled = run all pipeline steps in the same scan run for predictable behavior.
                      </p>
                    </div>
                    <Switch
                      checked={postScanAsync}
                      onCheckedChange={(checked) => updateConfig({ PIPELINE_POST_SCAN_ASYNC: Boolean(checked) })}
                    />
                  </div>
                </div>

                <Collapsible open={schedulerAdvancedOpen} onOpenChange={setSchedulerAdvancedOpen}>
                  <div className="rounded-lg border border-border bg-muted/20">
                    <CollapsibleTrigger asChild>
                      <Button
                        type="button"
                        variant="ghost"
                        className="w-full justify-between rounded-none px-3 py-2 text-left"
                      >
                        <span className="text-sm font-medium">Advanced scheduler rules</span>
                        <ChevronDown className={`w-4 h-4 transition-transform ${schedulerAdvancedOpen ? 'rotate-180' : ''}`} />
                      </Button>
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <div className="border-t border-border/60 p-3">
                        {schedulerAdvancedOpen ? (
                          <SchedulerSettings config={config} updateConfig={updateConfig} />
                        ) : null}
                      </div>
                    </CollapsibleContent>
                  </div>
                </Collapsible>
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-pipeline" className={`scroll-mt-24 ${getSettingsSection('settings-pipeline').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-pipeline').iconClass}`}>
                    <Workflow className="w-4 h-4" />
                  </span>
                  Pipeline automation
                </CardTitle>
                <CardDescription>Enable/disable each pipeline stage and configure external player sync.</CardDescription>
              </CardHeader>
              <CardContent>
                <IntegrationsSettings config={config} updateConfig={updateConfig} errors={errors} />
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-ai" className={`scroll-mt-24 ${getSettingsSection('settings-ai').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-ai').iconClass}`}>
                    <Sparkles className="w-4 h-4" />
                  </span>
                  AI
                </CardTitle>
                <CardDescription>
                  Configure OpenAI API key for batch workloads and OpenAI Codex OAuth for interactive workflows.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="rounded-lg border border-border p-4 space-y-4">
                  <div className="space-y-1">
                    <Label>OpenAI modes</Label>
                    <p className="text-xs text-muted-foreground">
                      Activate only the OpenAI auth modes you want PMDA to use at runtime.
                    </p>
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
                      <div className="space-y-0.5">
                        <p className="text-sm font-medium">API key mode</p>
                        <p className="text-xs text-muted-foreground">Used by `openai-api` provider.</p>
                      </div>
                      <Switch
                        checked={openaiApiModeEnabled}
                        onCheckedChange={(checked) => updateConfig({ OPENAI_ENABLE_API_KEY_MODE: checked })}
                      />
                    </div>
                    <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
                      <div className="space-y-0.5">
                        <p className="text-sm font-medium">ChatGPT OAuth mode</p>
                        <p className="text-xs text-muted-foreground">Used by `openai-codex` provider.</p>
                      </div>
                      <Switch
                        checked={openaiCodexModeEnabled}
                        onCheckedChange={(checked) => updateConfig({ OPENAI_ENABLE_CODEX_OAUTH_MODE: checked })}
                      />
                    </div>
                  </div>
                </div>

                <div className="rounded-lg border border-border p-4 space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="space-y-1">
                      <Label>OpenAI Codex (Sign in with ChatGPT)</Label>
                      <p className="text-xs text-muted-foreground">
                        Recommended for interactive flows. PMDA stores OAuth tokens securely in settings DB.
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant={codexReady ? 'default' : (codexProfileConnected ? 'secondary' : 'outline')}>
                        {codexReady ? 'Connected' : (codexProfileConnected ? 'Reconnect required' : 'Not connected')}
                      </Badge>
                      {openaiCodexStatus?.auth_mode ? (
                        <Badge variant="outline">{openaiCodexStatus.auth_mode}</Badge>
                      ) : null}
                    </div>
                  </div>
                  {openaiOAuth?.warning && (
                    <p className="text-xs text-muted-foreground">
                      <span className="font-medium">Note:</span> {openaiOAuth.warning}
                    </p>
                  )}
                  {codexProfileConnected ? (
                    <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                      {openaiCodexStatus.account_id ? <Badge variant="outline">Account: {openaiCodexStatus.account_id}</Badge> : null}
                      {typeof openaiCodexStatus.expires_in_sec === 'number' ? (
                        <Badge variant="outline">Expires in {Math.max(0, Math.floor(openaiCodexStatus.expires_in_sec / 60))} min</Badge>
                      ) : null}
                      {openaiCodexStatus.has_refresh_token ? <Badge variant="outline">Refresh token available</Badge> : null}
                    </div>
                  ) : null}
                  {openaiCodexStatus?.error ? (
                    <p className="text-xs text-destructive">
                      OAuth runtime issue: {openaiCodexStatus.error}
                    </p>
                  ) : null}
                  <div className="flex flex-wrap items-center gap-2">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={startOpenAIOAuth}
                      disabled={openaiOAuthBusy || !openaiCodexModeEnabled}
                    >
                      {openaiOAuthBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <ExternalLink className="w-4 h-4" />}
                      Connect
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={disconnectOpenAIOAuth}
                      disabled={openaiOAuthBusy || !openaiCodexModeEnabled || !codexProfileConnected}
                    >
                      {openaiOAuthBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <X className="w-4 h-4" />}
                      Disconnect
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={async () => {
                        setOpenaiOAuthBusy(true);
                        try {
                          await refreshOpenAICodexStatus(true);
                        } finally {
                          setOpenaiOAuthBusy(false);
                        }
                      }}
                      disabled={openaiOAuthBusy}
                    >
                      <RefreshCw className="w-4 h-4" />
                      Refresh status
                    </Button>
                  </div>
                  {!openaiCodexModeEnabled ? (
                    <p className="text-xs text-amber-600">ChatGPT OAuth mode is disabled. Interactive OAuth routing is inactive.</p>
                  ) : null}
                  {openaiOAuth && (
                    <div className="space-y-2 pt-2 border-t border-border/60">
                      <div className="flex flex-col sm:flex-row sm:items-center gap-2">
                        <Input readOnly value={openaiOAuth.userCode} className="font-mono" />
                        <div className="flex items-center gap-2">
                          <Button
                            type="button"
                            variant="secondary"
                            size="sm"
                            className="gap-1.5"
                            onClick={async () => {
                              const ok = await copyTextToClipboard(openaiOAuth.userCode);
                              if (ok) toast.success('Code copied');
                              else toast.error('Copy failed');
                            }}
                          >
                            <Copy className="w-4 h-4" />
                            Copy
                          </Button>
                          <Button type="button" variant="secondary" size="sm" className="gap-1.5" asChild>
                            <a href={openaiOAuth.verificationUrl} target="_blank" rel="noreferrer">
                              <ExternalLink className="w-4 h-4" />
                              Open OpenAI
                            </a>
                          </Button>
                        </div>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        1) Open the OpenAI page, 2) enter the code, 3) come back here. PMDA will connect automatically.
                      </p>
                      <div className="flex flex-wrap items-center gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="gap-2"
                          onClick={pollOpenAIOAuth}
                          disabled={openaiOAuthBusy || openaiOAuth.status !== 'pending'}
                        >
                          {openaiOAuthBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                          Check status
                        </Button>
                        <span className="text-xs text-muted-foreground">
                          {openaiOAuth.message
                            || (openaiOAuth.status === 'pending' ? 'Waiting for authorization…'
                              : openaiOAuth.status === 'completed' ? 'Connected' : 'Error')}
                        </span>
                      </div>
                    </div>
                  )}
                </div>

                <div className="rounded-lg border border-border p-4 space-y-3">
                  <div className="space-y-2">
                    <Label>OpenAI API key (batch/automation)</Label>
                    <p className="text-xs text-muted-foreground">
                      Used by `openai-api` provider for batch scan jobs, metadata enrichment and automations.
                    </p>
                    <PasswordInput
                      value={config.OPENAI_API_KEY || ''}
                      onChange={(e) => updateConfig({ OPENAI_API_KEY: e.target.value, AI_PROVIDER: 'openai-api' })}
                      placeholder="sk-..."
                      disabled={!openaiApiModeEnabled}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Model (optional)</Label>
                    {openaiModelsLoading ? (
                      <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Loading available models...
                      </div>
                    ) : openaiModels.length > 0 ? (
                      (() => {
                        const current = (config.OPENAI_MODEL || '').trim();
                        const items = current && !openaiModels.includes(current)
                          ? [current, ...openaiModels]
                          : openaiModels;
                        return (
                          <Select
                            value={current}
                            onValueChange={(value) => updateConfig({ OPENAI_MODEL: value, AI_PROVIDER: 'openai-api' })}
                            disabled={!openaiApiModeEnabled}
                          >
                            <SelectTrigger>
                              <SelectValue placeholder="Select a model" />
                            </SelectTrigger>
                            <SelectContent>
                              {items.map((m) => (
                                <SelectItem key={m} value={m}>
                                  {m}{m === current && !openaiModels.includes(current) ? ' (custom)' : ''}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        );
                      })()
                    ) : (
                      <Input
                        placeholder="gpt-5-nano"
                        value={config.OPENAI_MODEL || ''}
                        onChange={(e) => updateConfig({ OPENAI_MODEL: e.target.value, AI_PROVIDER: 'openai-api' })}
                        disabled={!openaiApiModeEnabled}
                      />
                    )}
                    {openaiModelsError && (
                      <p className="text-xs text-destructive">{openaiModelsError}</p>
                    )}
                  </div>
                  {!openaiApiModeEnabled ? (
                    <p className="text-xs text-amber-600">API key mode is disabled. Batch/web-search routing to `openai-api` is inactive.</p>
                  ) : null}
                </div>

                <div className="rounded-lg border border-border p-4 space-y-3">
                  <div className="space-y-1">
                    <Label>Provider routing by context</Label>
                    <p className="text-xs text-muted-foreground">
                      Select which provider PMDA uses by runtime context. Interactive will fallback automatically if Codex OAuth is unavailable.
                    </p>
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div className="space-y-2">
                      <Label className="text-xs">Interactive</Label>
                      <Select
                        value={providerPreferences?.interactive_provider_id || config.OPENAI_PROVIDER_PREF_INTERACTIVE || 'openai-codex'}
                        onValueChange={(value: AIProviderId) => {
                          void saveProviderPreferences({ interactive_provider_id: value });
                        }}
                        disabled={providerPreferencesBusy}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select provider" />
                        </SelectTrigger>
                        <SelectContent>
                          {AI_PROVIDER_OPTIONS.map((opt) => (
                            <SelectItem
                              key={`interactive-${opt.value}`}
                              value={opt.value}
                              disabled={Boolean(
                                (opt.value === 'openai-api' && !openaiApiModeEnabled)
                                || (opt.value === 'openai-codex' && (!openaiCodexModeEnabled || !openaiCodexStatus?.connected))
                                || (opt.codexOnly && !openaiCodexStatus?.connected)
                              )}
                            >
                              <span className="inline-flex items-center gap-2">
                                <ProviderIcon provider={opt.provider} />
                                {opt.label}
                              </span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <div className="text-[11px] text-muted-foreground flex items-center gap-2">
                        <span>Effective:</span>
                        <ProviderBadge
                          provider={config.OPENAI_PROVIDER_EFFECTIVE_INTERACTIVE || providerPreferences?.effective?.interactive_provider_id || 'openai-api'}
                          className="h-5 px-2 py-0 text-[10px]"
                        />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs">Batch</Label>
                      <Select
                        value={providerPreferences?.batch_provider_id || config.OPENAI_PROVIDER_PREF_BATCH || 'openai-codex'}
                        onValueChange={(value: AIProviderId) => {
                          void saveProviderPreferences({ batch_provider_id: value });
                        }}
                        disabled={providerPreferencesBusy}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select provider" />
                        </SelectTrigger>
                        <SelectContent>
                          {AI_PROVIDER_OPTIONS.map((opt) => (
                            <SelectItem
                              key={`batch-${opt.value}`}
                              value={opt.value}
                              disabled={Boolean(
                                (opt.value === 'openai-api' && !openaiApiModeEnabled)
                                || (opt.value === 'openai-codex' && (!openaiCodexModeEnabled || !openaiCodexStatus?.connected))
                                || (opt.codexOnly && !openaiCodexStatus?.connected)
                              )}
                            >
                              <span className="inline-flex items-center gap-2">
                                <ProviderIcon provider={opt.provider} />
                                {opt.label}
                              </span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <div className="text-[11px] text-muted-foreground flex items-center gap-2">
                        <span>Effective:</span>
                        <ProviderBadge
                          provider={config.OPENAI_PROVIDER_EFFECTIVE_BATCH || providerPreferences?.effective?.batch_provider_id || 'openai-codex'}
                          className="h-5 px-2 py-0 text-[10px]"
                        />
                      </div>
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs">Web search</Label>
                      <Select
                        value={providerPreferences?.web_search_provider_id || config.OPENAI_PROVIDER_PREF_WEB_SEARCH || 'openai-codex'}
                        onValueChange={(value: AIProviderId) => {
                          void saveProviderPreferences({ web_search_provider_id: value });
                        }}
                        disabled={providerPreferencesBusy}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select provider" />
                        </SelectTrigger>
                        <SelectContent>
                          {AI_PROVIDER_OPTIONS.map((opt) => (
                            <SelectItem
                              key={`web-${opt.value}`}
                              value={opt.value}
                              disabled={Boolean(
                                (opt.value === 'openai-api' && !openaiApiModeEnabled)
                                || (opt.value === 'openai-codex' && (!openaiCodexModeEnabled || !openaiCodexStatus?.connected))
                                || (opt.codexOnly && !openaiCodexStatus?.connected)
                              )}
                            >
                              <span className="inline-flex items-center gap-2">
                                <ProviderIcon provider={opt.provider} />
                                {opt.label}
                              </span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <div className="text-[11px] text-muted-foreground flex items-center gap-2">
                        <span>Effective:</span>
                        <ProviderBadge
                          provider={providerPreferences?.effective?.web_search_provider_id || providerPreferences?.web_search_provider_id || 'openai-codex'}
                          className="h-5 px-2 py-0 text-[10px]"
                        />
                      </div>
                    </div>
                  </div>
                </div>

                <div className="space-y-2">
                  <div className="mt-1 space-y-3 rounded-lg border border-border/60 bg-muted/30 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <Label>AI usage level</Label>
                      <span className="text-xs font-medium text-muted-foreground">{selectedAiLevelMeta.label}</span>
                    </div>
                    <Slider
                      min={0}
                      max={2}
                      step={1}
                      value={[selectedAiLevelIndex]}
                      onValueChange={(values) => {
                        const idx = Math.max(0, Math.min(2, Number(values?.[0] ?? 1)));
                        const level = AI_USAGE_LEVELS[idx]?.value ?? 'medium';
                        updateConfig({ AI_USAGE_LEVEL: level });
                      }}
                    />
                    <p className="text-xs text-muted-foreground">{selectedAiLevelMeta.description}</p>
                  </div>
                  <div className="rounded-lg border border-border/60 p-3 space-y-2">
                    <div className="flex items-center justify-between gap-3">
                      <div className="space-y-1">
                        <Label>Auto-generate soft-match album reviews</Label>
                        <p className="text-xs text-muted-foreground">
                          When enabled, PMDA can generate album descriptions for SOFT_MATCH albums (web + AI relevance checks).
                        </p>
                      </div>
                      <Switch
                        checked={Boolean(config.USE_AI_FOR_SOFT_MATCH_PROFILES)}
                        onCheckedChange={(checked) => updateConfig({ USE_AI_FOR_SOFT_MATCH_PROFILES: Boolean(checked) })}
                      />
                    </div>
                    <p className="text-[11px] text-muted-foreground">
                      Manual generation from album match detail remains available even when this is disabled.
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-providers" className={`scroll-mt-24 ${getSettingsSection('settings-providers').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-providers').iconClass}`}>
                    <Database className="w-4 h-4" />
                  </span>
                  Metadata providers
                </CardTitle>
                <CardDescription>
                  Optional credentials that improve matching/enrichment quality.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="text-xs text-muted-foreground">
                    Separation by provider, with live credential checks.
                  </p>
                  <div className="flex items-center gap-2">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={refreshProviderStatus}
                      disabled={providersChecking}
                    >
                      {providersChecking ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                      Check keys
                    </Button>
                    <span className="text-xs text-muted-foreground">
                      Last check: {providersPreflightAt ? new Date(providersPreflightAt).toLocaleTimeString() : 'not run yet'}
                    </span>
                  </div>
                </div>

                <Tabs defaultValue="discogs" className="w-full">
                  <TabsList className="grid h-auto w-full grid-cols-5">
                    <TabsTrigger value="discogs"><span className="inline-flex items-center gap-1.5"><ProviderIcon provider="discogs" />Discogs</span></TabsTrigger>
                    <TabsTrigger value="lastfm"><span className="inline-flex items-center gap-1.5"><ProviderIcon provider="lastfm" />Last.fm</span></TabsTrigger>
                    <TabsTrigger value="fanart"><span className="inline-flex items-center gap-1.5"><ProviderIcon provider="fanart" />Fanart</span></TabsTrigger>
                    <TabsTrigger value="serper"><span className="inline-flex items-center gap-1.5"><ProviderIcon provider="serper" />Serper</span></TabsTrigger>
                    <TabsTrigger value="acoustid"><span className="inline-flex items-center gap-1.5"><ProviderIcon provider="acoustid" />AcoustID</span></TabsTrigger>
                  </TabsList>

                  <TabsContent value="discogs" className="mt-3">
                    {(() => {
                      const state = providerState('discogs', Boolean(String(config.DISCOGS_USER_TOKEN || '').trim()));
                      return (
                        <div className="space-y-3 rounded-lg border border-border p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="space-y-1">
                              <Label htmlFor="discogs-token">Discogs user token</Label>
                              <p className="text-xs text-muted-foreground">{state.message}</p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant={state.variant}>{state.label}</Badge>
                              <a href="https://www.discogs.com/settings/developers" target="_blank" rel="noreferrer" className="text-xs text-primary inline-flex items-center gap-1 hover:underline">
                                Create token <ExternalLink className="w-3 h-3" />
                              </a>
                            </div>
                          </div>
                          <PasswordInput
                            id="discogs-token"
                            placeholder="Discogs user token"
                            value={config.DISCOGS_USER_TOKEN || ''}
                            onChange={(e) => updateConfig({ DISCOGS_USER_TOKEN: e.target.value })}
                          />
                        </div>
                      );
                    })()}
                  </TabsContent>

                  <TabsContent value="lastfm" className="mt-3">
                    {(() => {
                      const configured = Boolean(String(config.LASTFM_API_KEY || '').trim() && String(config.LASTFM_API_SECRET || '').trim());
                      const state = providerState('lastfm', configured);
                      return (
                        <div className="space-y-3 rounded-lg border border-border p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="space-y-1">
                              <Label htmlFor="lastfm-key">Last.fm credentials</Label>
                              <p className="text-xs text-muted-foreground">{state.message}</p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant={state.variant}>{state.label}</Badge>
                              <a href="https://www.last.fm/api/account/create" target="_blank" rel="noreferrer" className="text-xs text-primary inline-flex items-center gap-1 hover:underline">
                                Create keys <ExternalLink className="w-3 h-3" />
                              </a>
                            </div>
                          </div>
                          <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                            <PasswordInput
                              id="lastfm-key"
                              placeholder="Last.fm API key"
                              value={config.LASTFM_API_KEY || ''}
                              onChange={(e) => updateConfig({ LASTFM_API_KEY: e.target.value })}
                            />
                            <PasswordInput
                              id="lastfm-secret"
                              placeholder="Last.fm API secret"
                              value={config.LASTFM_API_SECRET || ''}
                              onChange={(e) => updateConfig({ LASTFM_API_SECRET: e.target.value })}
                            />
                          </div>
                        </div>
                      );
                    })()}
                  </TabsContent>

                  <TabsContent value="fanart" className="mt-3">
                    {(() => {
                      const state = providerState('fanart', Boolean(String(config.FANART_API_KEY || '').trim()));
                      return (
                        <div className="space-y-3 rounded-lg border border-border p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="space-y-1">
                              <Label htmlFor="fanart-key">Fanart.tv API key</Label>
                              <p className="text-xs text-muted-foreground">{state.message}</p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant={state.variant}>{state.label}</Badge>
                              <a href="https://fanart.tv/get-an-api-key/" target="_blank" rel="noreferrer" className="text-xs text-primary inline-flex items-center gap-1 hover:underline">
                                Create key <ExternalLink className="w-3 h-3" />
                              </a>
                            </div>
                          </div>
                          <PasswordInput
                            id="fanart-key"
                            placeholder="Fanart.tv API key"
                            value={String(config.FANART_API_KEY ?? '')}
                            onChange={(e) => updateConfig({ FANART_API_KEY: e.target.value })}
                          />
                        </div>
                      );
                    })()}
                  </TabsContent>

                  <TabsContent value="serper" className="mt-3">
                    {(() => {
                      const state = providerState('serper', Boolean(String(config.SERPER_API_KEY || '').trim()));
                      return (
                        <div className="space-y-3 rounded-lg border border-border p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="space-y-1">
                              <Label htmlFor="serper-key">Serper API key</Label>
                              <p className="text-xs text-muted-foreground">{state.message}</p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant={state.variant}>{state.label}</Badge>
                              <a href="https://serper.dev/" target="_blank" rel="noreferrer" className="text-xs text-primary inline-flex items-center gap-1 hover:underline">
                                Create key <ExternalLink className="w-3 h-3" />
                              </a>
                            </div>
                          </div>
                          <PasswordInput
                            id="serper-key"
                            placeholder="Serper.dev API key"
                            value={String(config.SERPER_API_KEY ?? '')}
                            onChange={(e) => updateConfig({ SERPER_API_KEY: e.target.value })}
                          />
                        </div>
                      );
                    })()}
                  </TabsContent>

                  <TabsContent value="acoustid" className="mt-3">
                    {(() => {
                      const state = providerState('acoustid', Boolean(String(config.ACOUSTID_API_KEY || '').trim()));
                      return (
                        <div className="space-y-3 rounded-lg border border-border p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="space-y-1">
                              <Label htmlFor="acoustid-key">AcoustID API key</Label>
                              <p className="text-xs text-muted-foreground">{state.message}</p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant={state.variant}>{state.label}</Badge>
                              <a href="https://acoustid.org/new-application" target="_blank" rel="noreferrer" className="text-xs text-primary inline-flex items-center gap-1 hover:underline">
                                Create key <ExternalLink className="w-3 h-3" />
                              </a>
                            </div>
                          </div>
                          <PasswordInput
                            id="acoustid-key"
                            placeholder="AcoustID API key"
                            value={String(config.ACOUSTID_API_KEY ?? '')}
                            onChange={(e) => updateConfig({ ACOUSTID_API_KEY: e.target.value })}
                          />
                        </div>
                      );
                    })()}
                  </TabsContent>
                </Tabs>
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-concerts" className={`scroll-mt-24 ${getSettingsSection('settings-concerts').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-concerts').iconClass}`}>
                    <MapPin className="w-4 h-4" />
                  </span>
                  Concerts
                </CardTitle>
                <CardDescription>
                  Filter upcoming concerts around a custom location (used on artist pages).
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="space-y-1">
                    <Label>Enable location filter</Label>
                    <p className="text-xs text-muted-foreground">
                      When enabled, only concerts within your radius are shown.
                    </p>
                  </div>
                  <Switch
                    checked={Boolean(config.CONCERTS_FILTER_ENABLED)}
                    onCheckedChange={(checked) => updateConfig({ CONCERTS_FILTER_ENABLED: Boolean(checked) })}
                  />
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                  <div className="space-y-1">
                    <Label htmlFor="concerts-lat">Latitude</Label>
                    <Input
                      id="concerts-lat"
                      inputMode="decimal"
                      placeholder="50.535"
                      value={String(config.CONCERTS_HOME_LAT ?? '')}
                      onChange={(e) => updateConfig({ CONCERTS_HOME_LAT: e.target.value })}
                    />
                  </div>
                  <div className="space-y-1">
                    <Label htmlFor="concerts-lon">Longitude</Label>
                    <Input
                      id="concerts-lon"
                      inputMode="decimal"
                      placeholder="5.567"
                      value={String(config.CONCERTS_HOME_LON ?? '')}
                      onChange={(e) => updateConfig({ CONCERTS_HOME_LON: e.target.value })}
                    />
                  </div>
                  <div className="space-y-1">
                    <Label htmlFor="concerts-radius">Radius (km)</Label>
                    <Input
                      id="concerts-radius"
                      type="number"
                      min={1}
                      max={2000}
                      placeholder="150"
                      value={String(config.CONCERTS_RADIUS_KM ?? '150')}
                      onChange={(e) => updateConfig({ CONCERTS_RADIUS_KM: e.target.value })}
                    />
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="gap-2"
                    onClick={() => {
                      if (!navigator?.geolocation?.getCurrentPosition) {
                        toast.error('Geolocation is not available in this browser.');
                        return;
                      }
                      navigator.geolocation.getCurrentPosition(
                        (pos) => {
                          const lat = pos.coords?.latitude;
                          const lon = pos.coords?.longitude;
                          if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
                            toast.error('Could not read your location.');
                            return;
                          }
                          updateConfig({
                            CONCERTS_HOME_LAT: String(lat),
                            CONCERTS_HOME_LON: String(lon),
                            CONCERTS_FILTER_ENABLED: true,
                          });
                          toast.success('Location saved');
                        },
                        () => toast.error('Location permission denied.'),
                        { enableHighAccuracy: false, maximumAge: 60_000, timeout: 10_000 }
                      );
                    }}
                  >
                    <MapPin className="w-4 h-4" />
                    Use my location
                  </Button>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => updateConfig({ CONCERTS_HOME_LAT: '', CONCERTS_HOME_LON: '' })}
                  >
                    Clear
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Separator />

            <Card id="settings-danger-zone" className={`scroll-mt-24 ${getSettingsSection('settings-danger-zone').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-destructive">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-danger-zone').iconClass}`}>
                    <AlertTriangle className="w-4 h-4" />
                  </span>
                  Danger Zone
                </CardTitle>
                <CardDescription>
                  Full maintenance reset. This clears library/cache/state data and restarts PMDA automatically, while keeping settings and credentials.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                {DANGER_PRESETS.map((action) => (
                  <div
                    key={action.id}
                    className="flex flex-col gap-3 rounded-md border border-destructive/20 bg-destructive/5 p-3 md:flex-row md:items-center md:justify-between"
                  >
                    <div className="space-y-1">
                      <p className="text-sm font-medium">{action.title}</p>
                      <p className="text-xs text-muted-foreground">{action.description}</p>
                    </div>
                    <Button
                      type="button"
                      variant="destructive"
                      className="gap-2 shrink-0"
                      onClick={() => openDangerConfirm(action.id)}
                      disabled={Boolean(dangerBusyPreset) || isRestarting}
                    >
                      {dangerBusyPreset === action.id ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
                      {action.buttonLabel}
                    </Button>
                  </div>
                ))}
              </CardContent>
            </Card>

            <div className="flex justify-end pt-4">
              <Button onClick={saveConfig} disabled={isSaving} variant="outline" className="gap-2">
                {isSaving ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  <>
                    <Save className="w-4 h-4" />
                    Save all now
                  </>
                )}
              </Button>
            </div>
          </div>
        </div>

        <AlertDialog
          open={dangerDialogOpen}
          onOpenChange={(open) => {
            if (dangerBusyPreset) return;
            setDangerDialogOpen(open);
            if (!open) setPendingDangerPreset(null);
          }}
        >
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>
                {activeDangerMeta ? `${activeDangerMeta.title}?` : 'Confirm maintenance action'}
              </AlertDialogTitle>
              <AlertDialogDescription>
                This will remove generated artwork cache, reset <span className="font-mono">cache.db</span>,
                reset <span className="font-mono">state.db</span>, and clear indexed library rows in PostgreSQL.
                The library view will stay empty until you run the next full scan. Settings, users, and API keys are kept.
                This action is destructive and cannot be undone.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel disabled={Boolean(dangerBusyPreset)}>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={(e) => {
                  e.preventDefault();
                  runDangerAction();
                }}
                disabled={Boolean(dangerBusyPreset)}
              >
                {dangerBusyPreset ? (
                  <span className="inline-flex items-center gap-2">
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Processing...
                  </span>
                ) : (
                  'Confirm and restart PMDA'
                )}
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>

        {isRestarting && (
          <>
            <div className="fixed inset-0 z-[10000] bg-black/80 backdrop-blur-md" />
            <div className="fixed left-1/2 top-1/2 z-[10001] w-full max-w-md -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-border bg-card p-6 shadow-2xl">
              <div className="flex flex-col items-center gap-4 text-center">
                <div className="rounded-full bg-primary/10 p-4">
                  <RefreshCw className="h-8 w-8 animate-spin text-primary" />
                </div>
                <div className="space-y-2">
                  <h3 className="text-lg font-semibold">PMDA is restarting</h3>
                  <p className="text-sm text-muted-foreground">
                    Please hold on. Auto-refresh in <span className="font-mono font-semibold text-primary">{rebootCountdown}</span> {rebootCountdown === 1 ? 'second' : 'seconds'}.
                  </p>
                </div>
                <div className="w-full space-y-2">
                  <Progress value={rebootProgress} className="h-2" />
                  <p className="text-xs text-muted-foreground">Waiting for PMDA container to come back…</p>
                </div>
              </div>
            </div>
          </>
        )}
    </div>
  );
}

export default SettingsPage;
