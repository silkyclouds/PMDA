import { useState, useEffect, useRef, useCallback, type ComponentType } from 'react';
import { Save, Loader2, Check, FolderOutput, RefreshCw, X, Database, Sparkles, ExternalLink, Copy, MapPin, ChevronDown, AlertTriangle, Trash2, SlidersHorizontal, Workflow, Download, ArrowRight, ArrowUp, ArrowDown, Server, Globe, Cpu } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import { IntegrationsSettings } from '@/components/settings/IntegrationsSettings';
import { ProfileSharingSettings } from '@/components/settings/ProfileSharingSettings';
import { SchedulerSettings } from '@/components/settings/SchedulerSettings';
import { SourcesAutonomySettings } from '@/components/settings/SourcesAutonomySettings';
import { LibraryWorkflowSettings } from '@/components/settings/LibraryWorkflowSettings';
import { OnboardingWizard } from '@/components/settings/OnboardingWizard';
import { GuidedOnboardingDialog } from '@/components/settings/GuidedOnboardingDialog';
import { ScalingSettings } from '@/components/settings/ScalingSettings';
import { ProviderBadge } from '@/components/providers/ProviderBadge';
import { ProviderIcon } from '@/components/providers/ProviderIcon';
import { useAuth } from '@/contexts/AuthContext';
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
    id: 'settings-onboarding',
    label: 'Onboarding',
    icon: Sparkles,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-library-workflow',
    label: 'Library workflow',
    icon: Workflow,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-files-export',
    label: 'Folders',
    icon: FolderOutput,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-scan-behavior',
    label: 'Scan behavior',
    icon: SlidersHorizontal,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-scaling',
    label: 'Scaling',
    icon: Server,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-pipeline',
    label: 'Pipeline',
    icon: Sparkles,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-ai',
    label: 'AI',
    icon: Sparkles,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-providers',
    label: 'Metadata providers',
    icon: Database,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-concerts',
    label: 'Concerts',
    icon: MapPin,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-border/60',
    iconClass: 'pmda-settings-section-icon',
  },
  {
    id: 'settings-danger-zone',
    label: 'Danger zone',
    icon: AlertTriangle,
    navClass: 'pmda-settings-nav-item',
    navActiveClass: '',
    cardClass: 'border-destructive/30 bg-destructive/[0.04]',
    iconClass: 'pmda-settings-section-icon',
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
    value: 'auto',
    label: 'Auto',
    description: 'Recommended. PMDA stays local-first, uses OCR/providers first, prefers self-hosted web search, and only escalates to paid AI when the case truly needs it.',
  },
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

const SCAN_AI_POLICY_OPTIONS: Array<{
  value: NonNullable<PMDAConfig['SCAN_AI_POLICY']>;
  label: string;
  description: string;
}> = [
  {
    value: 'local_only',
    label: 'Local only',
    description: 'Recommended default. Tags/OCR/providers first, then local web search + Ollama only. PMDA stays free to run unless you explicitly enable paid reinforcement.',
  },
  {
    value: 'local_then_paid',
    label: 'Local + paid fallback',
    description: 'Optional reinforcement mode. Local search and Ollama run first, then the paid chain only when the local path still cannot settle the case.',
  },
  {
    value: 'paid_only',
    label: 'Paid only',
    description: 'Skip Ollama and escalate directly to the paid fallback chain after providers/OCR.',
  },
];

const LOCAL_WEB_PROVIDER_IDS = ['serper'] as const;
const PAID_AI_PROVIDER_IDS = ['openai-api', 'openai-codex', 'anthropic', 'google'] as const;

const DANGER_PRESETS: DangerPresetMeta[] = [
  {
    id: 'reset_all_keep_settings',
    title: 'Reset PMDA (keep settings)',
    description:
      'Fully resets PMDA library data: media cache, scan/cache state, published library, playlists, likes, recommendations, notifications, concerts, assistant/RAG data, and playback history. Settings, users, OAuth/API credentials, and folder/provider configuration are preserved.',
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

function parseOrderedValues<T extends string>(value: unknown, allowed: readonly T[], fallback: readonly T[]): T[] {
  const out: T[] = [];
  const seen = new Set<string>();
  const queue: unknown[] = [value];
  const allowedSet = new Set<string>(allowed);

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
      if (s.startsWith('[')) {
        try {
          const parsed = JSON.parse(s) as unknown;
          if (parsed !== item) {
            queue.push(parsed);
            continue;
          }
        } catch {
          // Fall through to CSV parsing.
        }
      }
      if (s.includes(',')) {
        const parts = s.split(',').map((p) => p.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      const normalized = s.toLowerCase();
      if (allowedSet.has(normalized) && !seen.has(normalized)) {
        seen.add(normalized);
        out.push(normalized as T);
      }
      continue;
    }
    const normalized = String(item).trim().toLowerCase();
    if (allowedSet.has(normalized) && !seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized as T);
    }
  }

  for (const item of fallback) {
    if (!seen.has(item)) {
      seen.add(item);
      out.push(item);
    }
  }

  return out;
}

function moveOrderedItem<T>(items: readonly T[], index: number, direction: -1 | 1): T[] {
  const next = [...items];
  const target = index + direction;
  if (index < 0 || index >= next.length || target < 0 || target >= next.length) {
    return next;
  }
  const current = next[index]!;
  next[index] = next[target]!;
  next[target] = current;
  return next;
}

function SettingsPage() {
  const auth = useAuth();
  const [config, setConfig] = useState<Partial<PMDAConfig>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [activeSettingsSection, setActiveSettingsSection] = useState<string>('settings-onboarding');
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
  const [lastfmAuthStatus, setLastfmAuthStatus] = useState<api.LastfmAuthStatusResponse | null>(null);
  const [lastfmAuthBusy, setLastfmAuthBusy] = useState(false);
  const [providerPreferences, setProviderPreferences] = useState<api.AIProviderPreferencesResponse | null>(null);
  const [providerPreferencesBusy, setProviderPreferencesBusy] = useState(false);
  const [ollamaPullStatus, setOllamaPullStatus] = useState<api.OllamaPullStatus | null>(null);
  const [ollamaPullModel, setOllamaPullModel] = useState('');
  const [ollamaPullBusy, setOllamaPullBusy] = useState(false);
  const [ollamaDiscoveryBusy, setOllamaDiscoveryBusy] = useState(false);
  const [ollamaDiscoveryResults, setOllamaDiscoveryResults] = useState<api.OllamaDiscoveryRow[]>([]);
  const [ollamaConnectionBusy, setOllamaConnectionBusy] = useState(false);
  const [ollamaAvailableModels, setOllamaAvailableModels] = useState<string[]>([]);
  const [ollamaConnectionMessage, setOllamaConnectionMessage] = useState<string | null>(null);
  const [rebuildIndexLoading, setRebuildIndexLoading] = useState(false);
  const [advancedFoldersOpen, setAdvancedFoldersOpen] = useState(false);
  const [showGuidedOnboarding, setShowGuidedOnboarding] = useState(false);
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
  const configConfigured = (config as api.ConfigResponse).configured === true || Boolean(String(config.FILES_ROOTS || '').trim());

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

  const refreshOllamaPullStatus = useCallback(async () => {
    try {
      const status = await api.getOllamaPullStatus();
      setOllamaPullStatus(status);
      return status;
    } catch {
      return null;
    }
  }, []);

  useEffect(() => {
    if (auth.isAdmin) {
      void loadConfig();
      void refreshOllamaPullStatus();
      return;
    }
    setIsLoading(false);
  }, [auth.isAdmin, refreshOllamaPullStatus]);

  useEffect(() => {
    if (!ollamaPullStatus?.active) return;
    const t = setTimeout(() => {
      void refreshOllamaPullStatus();
    }, 1500);
    return () => clearTimeout(t);
  }, [ollamaPullStatus?.active, ollamaPullStatus?.updated_at, refreshOllamaPullStatus]);

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

  const refreshLastfmAuthStatus = useCallback(async () => {
    try {
      const status = await api.getLastfmAuthStatus();
      setLastfmAuthStatus(status);
      return status;
    } catch {
      return null;
    }
  }, []);

  const loadOptionalProviderState = useCallback(async () => {
    try {
      const [codexStatus, lastfmStatus, prefs] = await Promise.all([
        refreshOpenAICodexStatus(false),
        refreshLastfmAuthStatus(),
        api.getAIProviderPreferences().catch(() => null),
      ]);
      if (codexStatus) setOpenaiCodexStatus(codexStatus);
      if (lastfmStatus) setLastfmAuthStatus(lastfmStatus);
      if (prefs) setProviderPreferences(prefs);
    } catch {
      // Keep Settings responsive even if optional provider checks fail.
    }
  }, [refreshLastfmAuthStatus, refreshOpenAICodexStatus]);

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

  const startLastfmScrobbleAuth = useCallback(async () => {
    setLastfmAuthBusy(true);
    try {
      const result = await api.startLastfmAuth();
      if (!result.ok || !result.auth_url) {
        throw new Error('Failed to start Last.fm authorization');
      }
      const popup = window.open(result.auth_url, 'pmda-lastfm-auth', 'popup=yes,width=720,height=840,resizable=yes,scrollbars=yes');
      if (!popup) {
        window.location.href = result.auth_url;
        return;
      }
      popup.focus();
      toast.success('Authorize PMDA on Last.fm. The connection will finish automatically.');
      await refreshLastfmAuthStatus();
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to start Last.fm authorization'));
    } finally {
      setLastfmAuthBusy(false);
    }
  }, [getApiErrorMessage, refreshLastfmAuthStatus]);

  const disconnectLastfmScrobbleAuth = useCallback(async () => {
    setLastfmAuthBusy(true);
    try {
      const result = await api.disconnectLastfmAuth();
      if (!result.ok) {
        throw new Error(result.message || 'Failed to disconnect Last.fm');
      }
      toast.success(result.message || 'Last.fm disconnected');
      await refreshLastfmAuthStatus();
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to disconnect Last.fm'));
    } finally {
      setLastfmAuthBusy(false);
    }
  }, [getApiErrorMessage, refreshLastfmAuthStatus]);

  useEffect(() => {
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== window.location.origin) return;
      const data = event.data as { type?: string; ok?: boolean; message?: string; session_name?: string } | null;
      if (!data || data.type !== 'pmda:lastfm-auth-complete') return;
      void (async () => {
        const status = await refreshLastfmAuthStatus();
        if (status?.connected) {
          toast.success(status.message || `Last.fm connected${status.session_name ? `: ${status.session_name}` : ''}`);
          return;
        }
        if (data.ok) {
          toast.success(data.message || 'Last.fm connected');
        } else if (data.message) {
          toast.error(data.message);
        }
      })();
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [refreshLastfmAuthStatus]);

  useEffect(() => {
    if (!lastfmAuthStatus?.pending || lastfmAuthBusy) return;
    const t = setTimeout(() => {
      void refreshLastfmAuthStatus();
    }, 2000);
    return () => clearTimeout(t);
  }, [lastfmAuthBusy, lastfmAuthStatus?.pending, refreshLastfmAuthStatus]);

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

  const startOllamaModelPull = useCallback(async () => {
    const model = String(ollamaPullModel || '').trim();
    const url = String(config.OLLAMA_URL || '').trim();
    if (!url) {
      toast.error('Configure an Ollama URL first.');
      return;
    }
    if (!model) {
      toast.error('Enter a model name to download.');
      return;
    }
    setOllamaPullBusy(true);
    try {
      const status = await api.startOllamaPull({ OLLAMA_URL: url, model });
      setOllamaPullStatus(status);
      toast.success(status.active ? `Started downloading ${model}` : status.message || `${model} is ready`);
      const nextModels = await api.getAIModels('ollama', { url }).catch(() => null);
      if (Array.isArray(nextModels) && nextModels.length > 0) {
        // keep UI in sync if the model was already installed
        setOllamaPullModel((prev) => prev || model);
      }
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to start Ollama model pull'));
    } finally {
      setOllamaPullBusy(false);
    }
  }, [config.OLLAMA_URL, getApiErrorMessage, ollamaPullModel]);

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

  const testOllamaConnection = useCallback(async (candidateUrl?: string, persist = false) => {
    const url = String(candidateUrl || config.OLLAMA_URL || '').trim();
    if (!url) {
      toast.error('Configure an Ollama URL first.');
      return;
    }
    setOllamaConnectionBusy(true);
    try {
      const models = await api.getAIModels('ollama', { url });
      setOllamaAvailableModels(models);
      const currentModel = String(config.OLLAMA_MODEL || '').trim();
      const currentModelOk = !currentModel || models.includes(currentModel);
      const msg = currentModel
        ? currentModelOk
          ? `Ollama is reachable and model ${currentModel} is available.`
          : `Ollama is reachable, but ${currentModel} is not installed on this runtime.`
        : 'Ollama is reachable and returned models.';
      setOllamaConnectionMessage(msg);
      if (persist && url !== String(config.OLLAMA_URL || '').trim()) {
        updateConfig({ OLLAMA_URL: url });
      }
      toast.success(msg);
    } catch (e) {
      const message = getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to reach Ollama');
      setOllamaAvailableModels([]);
      setOllamaConnectionMessage(message);
      toast.error(message);
    } finally {
      setOllamaConnectionBusy(false);
    }
  }, [config.OLLAMA_MODEL, config.OLLAMA_URL, getApiErrorMessage, updateConfig]);

  const discoverOllamaHosts = useCallback(async () => {
    setOllamaDiscoveryBusy(true);
    try {
      const res = await api.discoverOllama(String(config.OLLAMA_URL || '').trim() || undefined);
      setOllamaDiscoveryResults(Array.isArray(res.results) ? res.results : []);
      const okCount = (Array.isArray(res.results) ? res.results : []).filter((row) => row.ok).length;
      toast.success(okCount > 0 ? `Found ${okCount} reachable Ollama endpoint${okCount > 1 ? 's' : ''}` : 'No reachable Ollama endpoints found');
    } catch (e) {
      toast.error(getApiErrorMessage(e) || (e instanceof Error ? e.message : 'Failed to discover Ollama on the local network'));
    } finally {
      setOllamaDiscoveryBusy(false);
    }
  }, [config.OLLAMA_URL, getApiErrorMessage]);

  const moveLocalWebProvider = useCallback((providerId: (typeof LOCAL_WEB_PROVIDER_IDS)[number], direction: -1 | 1) => {
    const current = parseOrderedValues(config.WEB_SEARCH_LOCAL_ORDER, LOCAL_WEB_PROVIDER_IDS, LOCAL_WEB_PROVIDER_IDS);
    const index = current.indexOf(providerId);
    const next = moveOrderedItem(current, index, direction);
    updateConfig({ WEB_SEARCH_LOCAL_ORDER: next.join(',') });
  }, [config.WEB_SEARCH_LOCAL_ORDER, updateConfig]);

  const movePaidAiProvider = useCallback((providerId: (typeof PAID_AI_PROVIDER_IDS)[number], direction: -1 | 1) => {
    const current = parseOrderedValues(config.SCAN_PAID_PROVIDER_ORDER, PAID_AI_PROVIDER_IDS, PAID_AI_PROVIDER_IDS);
    const index = current.indexOf(providerId);
    const next = moveOrderedItem(current, index, direction);
    updateConfig({ SCAN_PAID_PROVIDER_ORDER: next.join(',') });
  }, [config.SCAN_PAID_PROVIDER_ORDER, updateConfig]);

  const filesRoots = parsePathListValue(config.FILES_ROOTS);
  const selectedAiLevel = (() => {
    const raw = String(config.AI_USAGE_LEVEL || 'auto').trim().toLowerCase();
    return AI_USAGE_LEVELS.find((lvl) => lvl.value === raw)?.value || 'auto';
  })();
  const selectedAiLevelIndex = Math.max(0, AI_USAGE_LEVELS.findIndex((lvl) => lvl.value === selectedAiLevel));
  const selectedAiLevelMeta = AI_USAGE_LEVELS[selectedAiLevelIndex] || AI_USAGE_LEVELS[0];
  const selectedScanAiPolicy = (() => {
    const raw = String(config.SCAN_AI_POLICY || 'local_only').trim().toLowerCase();
    return SCAN_AI_POLICY_OPTIONS.find((option) => option.value === raw)?.value || 'local_only';
  })();
  const selectedScanAiPolicyMeta =
    SCAN_AI_POLICY_OPTIONS.find((option) => option.value === selectedScanAiPolicy) || SCAN_AI_POLICY_OPTIONS[0];
  const localWebOrder = parseOrderedValues(config.WEB_SEARCH_LOCAL_ORDER, LOCAL_WEB_PROVIDER_IDS, LOCAL_WEB_PROVIDER_IDS);
  const paidAiOrder = parseOrderedValues(config.SCAN_PAID_PROVIDER_ORDER, PAID_AI_PROVIDER_IDS, PAID_AI_PROVIDER_IDS);
  const effectiveScanBatchProvider = String(config.SCAN_AI_EFFECTIVE_BATCH || '').trim();
  const effectiveScanWebSearch = String(config.SCAN_AI_EFFECTIVE_WEB_SEARCH || '').trim();
  const ollamaConfiguredUrl = String(config.OLLAMA_URL || '').trim();
  const ollamaConfiguredModel = String(config.OLLAMA_MODEL || '').trim();
  const ollamaConfiguredHardModel = String(config.OLLAMA_COMPLEX_MODEL || '').trim();
  const ollamaConfigured = Boolean(ollamaConfiguredUrl && ollamaConfiguredModel);
  const ollamaModelInstalled =
    !ollamaConfiguredModel || ollamaAvailableModels.length === 0 || ollamaAvailableModels.includes(ollamaConfiguredModel);
  const ollamaHardModelInstalled =
    !ollamaConfiguredHardModel || ollamaAvailableModels.length === 0 || ollamaAvailableModels.includes(ollamaConfiguredHardModel);
  const localWebProviderConfigured = {
    serper: Boolean(String(config.SERPER_API_KEY || '').trim()),
  };
  const paidProviderConfigured = {
    'openai-api': Boolean(openaiApiModeEnabled && String(config.OPENAI_API_KEY || '').trim()),
    'openai-codex': Boolean(openaiCodexModeEnabled && openaiCodexStatus?.connected),
    anthropic: Boolean(String(config.ANTHROPIC_API_KEY || '').trim()),
    google: Boolean(String(config.GOOGLE_API_KEY || '').trim()),
  } as const;
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

  if (!auth.isAdmin) {
    return (
      <div className="pmda-page-shell">
        <div className="mb-6">
          <h1 className="pmda-page-title">Settings</h1>
          <p className="mt-1 pmda-meta-text">
            Personal profile preferences for sharing and identification inside PMDA.
          </p>
        </div>
        <ProfileSharingSettings />
      </div>
    );
  }

  return (
    <div className="pmda-page-shell">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="pmda-page-title">Settings</h1>
            <p className="pmda-meta-text mt-1">
              Changes are saved automatically. No restart needed.
            </p>
          </div>
          <div className="flex items-center gap-2">
            {lastSaved === true && (
              <span className="text-sm font-medium text-success flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-success/10">
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
            {SETTINGS_SECTIONS.map(({ id, label, icon: Icon }) => (
              <a
                key={id}
                href={`#${id}`}
                onClick={() => setActiveSettingsSection(id)}
                data-active={activeSettingsSection === id}
                className="pmda-settings-nav-item shrink-0 whitespace-nowrap"
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
              {SETTINGS_SECTIONS.map(({ id, label, icon: Icon }) => (
                <li key={id}>
                  <a
                    href={`#${id}`}
                    onClick={() => setActiveSettingsSection(id)}
                    data-active={activeSettingsSection === id}
                    className="pmda-settings-nav-item"
                  >
                    <Icon className="w-4 h-4" />
                    <span>{label}</span>
                  </a>
                </li>
              ))}
            </ul>
          </nav>

          <div className="min-w-0 flex-1 space-y-6">
            <ProfileSharingSettings compact />
            <OnboardingWizard
              config={config}
              updateConfig={updateConfig}
              configured={configConfigured}
              onOpenGuidedSetup={() => setShowGuidedOnboarding(true)}
            />
            <GuidedOnboardingDialog open={showGuidedOnboarding} onOpenChange={setShowGuidedOnboarding} />
            <LibraryWorkflowSettings
              config={config}
              updateConfig={updateConfig}
              onSwitchToCustom={() => {
                setAdvancedFoldersOpen(true);
                setActiveSettingsSection('settings-files-export');
              }}
            />
            <Card id="settings-files-export" className={`scroll-mt-24 ${getSettingsSection('settings-files-export').cardClass}`}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md ${getSettingsSection('settings-files-export').iconClass}`}>
                    <FolderOutput className="w-4 h-4" />
                  </span>
                  Folders
                </CardTitle>
                <CardDescription>
                  Low-level source, export and cache controls. In guided workflow modes, use this section only if you intentionally want to switch to raw configuration.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-5">
                {config.LIBRARY_WORKFLOW_MODE === 'custom' ? (
                  <>
                    <SourcesAutonomySettings />

                    <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                      <div className="rounded-xl border border-warning/20 bg-warning/[0.04] p-4 md:p-5 space-y-2">
                        <div className="flex items-center gap-2">
                          <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-warning/15 text-warning text-xs font-semibold">2</span>
                          <Label className="text-sm">Where should duplicates go?</Label>
                        </div>
                        <p className="text-xs text-muted-foreground">
                          When PMDA decides an album is a duplicate loser, it moves it here so you can review and restore it later if needed.
                        </p>
                        <FolderBrowserInput
                          value={config.DUPE_ROOT ?? '/dupes'}
                          onChange={(path) => updateConfig({ DUPE_ROOT: path || '/dupes' })}
                          placeholder="/dupes"
                          selectLabel="Select duplicates destination folder"
                        />
                      </div>

                      <div className="rounded-xl border border-warning/20 bg-warning/[0.04] p-4 md:p-5 space-y-2">
                        <div className="flex items-center gap-2">
                          <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-warning/15 text-warning text-xs font-semibold">3</span>
                          <Label className="text-sm">Where should incomplete albums go?</Label>
                        </div>
                        <p className="text-xs text-muted-foreground">
                          If PMDA detects missing tracks, it quarantines the album here instead of mixing it into your clean library.
                        </p>
                        <FolderBrowserInput
                          value={config.INCOMPLETE_ALBUMS_TARGET_DIR ?? '/dupes/incomplete_albums'}
                          onChange={(path) => updateConfig({ INCOMPLETE_ALBUMS_TARGET_DIR: path || '/dupes/incomplete_albums' })}
                          placeholder="/dupes/incomplete_albums"
                          selectLabel="Select incomplete albums destination folder"
                        />
                      </div>
                    </div>

                    <div className="rounded-xl border border-success/20 bg-success/[0.04] p-4 md:p-5 space-y-4">
                      <div className="space-y-1">
                        <div className="flex items-center gap-2">
                          <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-success/15 text-success text-xs font-semibold">4</span>
                          <Label className="text-sm">Where should PMDA build the clean exported library?</Label>
                        </div>
                        <p className="text-xs text-muted-foreground">
                          This folder is the clean library PMDA generates automatically. Point Plex, Navidrome or Jellyfin here rather than at your raw source or incoming folders.
                        </p>
                        <p className="text-[11px] text-muted-foreground">
                          If the folder does not exist yet, PMDA will create it when the export pipeline runs.
                        </p>
                      </div>
                      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr),320px] gap-3 items-start">
                        <FolderBrowserInput
                          value={config.EXPORT_ROOT ?? '/music/library'}
                          onChange={(path) => updateConfig({ EXPORT_ROOT: path || '/music/library' })}
                          placeholder="/music/library"
                          selectLabel="Select library export destination folder"
                        />
                        <div className="space-y-2">
                          <Label>Export method</Label>
                          <Select
                            value={(config.EXPORT_LINK_STRATEGY as 'hardlink' | 'symlink' | 'copy' | 'move' | undefined) ?? 'hardlink'}
                            onValueChange={(value: 'hardlink' | 'symlink' | 'copy' | 'move') => updateConfig({ EXPORT_LINK_STRATEGY: value })}
                          >
                            <SelectTrigger className="w-full">
                              <SelectValue placeholder="Select strategy" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="hardlink">Hardlink: fastest, no extra space</SelectItem>
                              <SelectItem value="symlink">Symlink: references original files</SelectItem>
                              <SelectItem value="copy">Copy: safest but duplicates files</SelectItem>
                              <SelectItem value="move">Move: relocates files physically</SelectItem>
                            </SelectContent>
                          </Select>
                          <p className="text-[11px] text-muted-foreground">
                            Recommended for most users: <span className="text-foreground">Hardlink</span>.
                          </p>
                        </div>
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        <div className="flex items-center justify-between rounded-lg border border-border/40 bg-muted/30 px-3 py-2.5">
                          <div className="space-y-0.5 pr-4">
                            <div className="text-sm font-medium">Include album format in folder name</div>
                            <div className="text-[11px] text-muted-foreground">
                              Example: <span className="text-foreground">Desert Solitaire (Flac)</span>
                            </div>
                          </div>
                          <Switch
                            checked={Boolean(config.EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER)}
                            onCheckedChange={(checked) => updateConfig({ EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER: checked })}
                          />
                        </div>
                        <div className="flex items-center justify-between rounded-lg border border-border/40 bg-muted/30 px-3 py-2.5">
                          <div className="space-y-0.5 pr-4">
                            <div className="text-sm font-medium">Include album type in folder name</div>
                            <div className="text-[11px] text-muted-foreground">
                              Example: <span className="text-foreground">Desert Solitaire (Flac,  Album)</span>
                            </div>
                          </div>
                          <Switch
                            checked={Boolean(config.EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER)}
                            onCheckedChange={(checked) => updateConfig({ EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER: checked })}
                          />
                        </div>
                      </div>
                    </div>
                  </>
                ) : (
                  <div className="rounded-xl border border-dashed border-border/70 bg-muted/20 p-4 text-sm text-muted-foreground">
                    Guided workflow mode is active. PMDA now treats Library / Inbox / Dupes as product concepts and translates them into the raw source/export settings automatically.
                    Use <span className="text-foreground">Switch to Custom / Advanced</span> in the Library workflow card if you want to edit source roots and export controls directly.
                  </div>
                )}

                <div className="rounded-xl border border-primary/20 bg-primary/[0.04] p-4 md:p-5 space-y-3">
                  <div className="space-y-1">
                    <div className="flex items-center gap-2">
                      <span className="inline-flex h-7 w-7 items-center justify-center rounded-full bg-primary/15 text-primary text-xs font-semibold">5</span>
                      <Label className="text-sm">Where should PMDA keep its artwork cache?</Label>
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Use an <span className="text-foreground">NVMe or SSD</span> folder here. PMDA pre-generates and serves artwork thumbnails from this location for fast browsing.
                    </p>
                    <p className="text-[11px] text-muted-foreground">
                      Do <span className="text-foreground">not</span> use a mechanical HDD unless you accept slower artwork rendering and higher latency.
                    </p>
                  </div>
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

                <Collapsible>
                  <div className="rounded-xl border border-border/70 bg-muted/20">
                    <CollapsibleTrigger asChild>
                      <Button
                        type="button"
                        variant="ghost"
                        className="w-full justify-between rounded-none px-4 py-3 text-left"
                      >
                        <span className="text-sm font-medium">Folder maintenance</span>
                        <ChevronDown className="w-4 h-4" />
                      </Button>
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <div className="border-t border-border/60 p-4">
                        <div className="flex items-center justify-between gap-3">
                          <div className="space-y-1">
                            <Label>Library index</Label>
                            <p className="text-xs text-muted-foreground">
                              Rebuild indexed files from the currently enabled source folders.
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
                    </CollapsibleContent>
                  </div>
                </Collapsible>
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
                  These switches decide when PMDA may start scans, whether the full pipeline stays attached to the scan, and whether provider-only hole-filling may continue after the scan for things like artist images, label logos, bios, similar artists, and album descriptions.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="rounded-lg border border-border p-4 space-y-3">
                  <div className="flex items-start justify-between gap-3 rounded-md bg-muted/40 p-3">
                    <div className="space-y-1">
                      <Label>Scan-first mode (recommended)</Label>
                      <p className="text-xs text-muted-foreground">
                        Best for predictable runs. PMDA only scans when you launch it manually, and the scan keeps ownership of the main pipeline: match, tags, covers, dedupe, incomplete moves, export, and provider metadata collection.
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
                        Lets PMDA start the same full scan pipeline automatically on schedule. This only controls scan launch timing; it does not change what the scan itself collects.
                      </p>
                    </div>
                    <Switch
                      checked={!schedulerPaused}
                      onCheckedChange={(checked) => updateConfig({ SCHEDULER_PAUSED: !checked })}
                    />
                  </div>
                  <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
                    <div className="space-y-1">
                      <Label>Allow non-scan background jobs</Label>
                      <p className="text-xs text-muted-foreground">
                        Lets PMDA keep filling remaining holes even when no scan is running. This is for provider-only follow-up like artist images, label logos, bios, similar artists, and album descriptions that may arrive after a page is opened or after a scan has already published the library.
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
                        Enabled = PMDA may declare the scan finished first and let the remaining tail finish in background. Disabled = the scan only reaches 100% after those trailing pipeline steps are done too.
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

            <ScalingSettings
              config={config}
              updateConfig={updateConfig}
            />

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
                  PMDA is optimized to run locally by default with Ollama plus self-hosted web search. Paid AI is optional and only needed if you want external reinforcement.
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
                    <p className="text-xs text-warning">ChatGPT OAuth mode is disabled. Interactive OAuth routing is inactive.</p>
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
                    <p className="text-xs text-warning">API key mode is disabled. Batch/web-search routing to `openai-api` is inactive.</p>
                  ) : null}
                </div>

                <div className="rounded-lg border border-border/60 p-4 space-y-4">
                  <div className="space-y-1">
                    <Label>Scan AI policy</Label>
                    <p className="text-xs text-muted-foreground">
                      Choose whether batch scans stay fully local, use local-first with paid reinforcement, or skip local AI entirely.
                    </p>
                  </div>
                  <div className="grid grid-cols-1 gap-3 xl:grid-cols-3">
                    {SCAN_AI_POLICY_OPTIONS.map((option) => {
                      const active = option.value === selectedScanAiPolicy;
                      return (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => updateConfig({ SCAN_AI_POLICY: option.value })}
                          className={`rounded-lg border p-3 text-left transition ${
                            active
                              ? 'border-primary bg-primary/10 shadow-[0_0_0_1px_rgba(45,212,191,0.18)]'
                              : 'border-border/60 bg-background/30 hover:border-border'
                          }`}
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-medium text-foreground">{option.label}</div>
                            {active ? <Badge variant="default">Active</Badge> : <Badge variant="outline">Available</Badge>}
                          </div>
                          <p className="mt-2 text-xs text-muted-foreground">{option.description}</p>
                        </button>
                      );
                    })}
                  </div>

                  <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,1.4fr)_auto_minmax(0,1fr)_auto_minmax(0,1fr)]">
                    <div className="rounded-lg border border-border/60 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                        <Database className="h-4 w-4 text-info" />
                        Deterministic core
                      </div>
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        Tags, file structure, OCR, AcousticID and metadata providers always run first.
                      </p>
                    </div>
                    <div className="hidden xl:flex items-center justify-center text-muted-foreground">
                      <ArrowRight className="h-4 w-4" />
                    </div>
                    <div className="rounded-lg border border-border/60 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                        <Globe className="h-4 w-4 text-success" />
                        Local web search
                      </div>
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {localWebOrder.map((providerId) => (
                          <ProviderBadge key={providerId} provider={providerId} className="h-6 px-2 py-0 text-[11px]" />
                        ))}
                      </div>
                      <p className="mt-2 text-[11px] text-muted-foreground">
                        Effective web step: <span className="text-foreground">{effectiveScanWebSearch || 'none yet'}</span>
                      </p>
                    </div>
                    <div className="hidden xl:flex items-center justify-center text-muted-foreground">
                      <ArrowRight className="h-4 w-4" />
                    </div>
                    <div className="rounded-lg border border-border/60 bg-background/30 p-3">
                      <div className="flex items-center gap-2 text-sm font-medium text-foreground">
                        <Cpu className="h-4 w-4 text-primary" />
                        AI arbitration
                      </div>
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {selectedScanAiPolicy !== 'paid_only' ? (
                          <ProviderBadge provider="ollama" className="h-6 px-2 py-0 text-[11px]" />
                        ) : null}
                        {selectedScanAiPolicy !== 'local_only'
                          ? paidAiOrder.map((providerId) => (
                              <ProviderBadge key={providerId} provider={providerId} className="h-6 px-2 py-0 text-[11px]" />
                            ))
                          : null}
                      </div>
                      <p className="mt-2 text-[11px] text-muted-foreground">
                        Effective batch AI: <span className="text-foreground">{effectiveScanBatchProvider || 'deterministic only'}</span>
                      </p>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
                    <div className="rounded-lg border border-border/60 bg-background/20 p-3 space-y-3">
                      <div className="flex items-center justify-between gap-3">
                        <div className="space-y-1">
                          <Label>Local web search order</Label>
                          <p className="text-[11px] text-muted-foreground">
                            Serper is the hosted search layer before local Ollama web search fallback. Keep paid AI web-search exceptional.
                          </p>
                        </div>
                        <Badge variant="outline">{String(config.WEB_SEARCH_PROVIDER || 'auto')}</Badge>
                      </div>
                      <div className="space-y-2">
                        {localWebOrder.map((providerId, index) => (
                          <div key={providerId} className="flex items-center justify-between gap-3 rounded-md border border-border/60 bg-background/40 px-3 py-2">
                            <div className="flex items-center gap-2">
                              <ProviderBadge provider={providerId} className="h-6 px-2 py-0 text-[11px]" />
                              <span className="text-[11px] text-muted-foreground">
                                {localWebProviderConfigured.serper ? 'Configured' : 'Set Serper API key'}
                              </span>
                            </div>
                            <div className="flex items-center gap-1">
                              <Button
                                type="button"
                                size="icon"
                                variant="ghost"
                                className="h-7 w-7"
                                onClick={() => moveLocalWebProvider(providerId, -1)}
                                disabled={index === 0}
                              >
                                <ArrowUp className="h-3.5 w-3.5" />
                              </Button>
                              <Button
                                type="button"
                                size="icon"
                                variant="ghost"
                                className="h-7 w-7"
                                onClick={() => moveLocalWebProvider(providerId, 1)}
                                disabled={index === localWebOrder.length - 1}
                              >
                                <ArrowDown className="h-3.5 w-3.5" />
                              </Button>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="rounded-lg border border-border/60 bg-background/20 p-3 space-y-3">
                      <div className="flex items-center justify-between gap-3">
                        <div className="space-y-1">
                          <Label>Paid fallback order</Label>
                          <p className="text-[11px] text-muted-foreground">
                            Only used when the selected policy allows paid escalation.
                          </p>
                        </div>
                        <Badge variant={selectedScanAiPolicy === 'local_only' ? 'outline' : 'secondary'}>
                          {selectedScanAiPolicy === 'local_only' ? 'Disabled' : 'Active on fallback'}
                        </Badge>
                      </div>
                      <div className="space-y-2">
                        {paidAiOrder.map((providerId, index) => (
                          <div key={providerId} className="flex items-center justify-between gap-3 rounded-md border border-border/60 bg-background/40 px-3 py-2">
                            <div className="flex items-center gap-2">
                              <ProviderBadge provider={providerId} className="h-6 px-2 py-0 text-[11px]" />
                              <span className="text-[11px] text-muted-foreground">
                                {paidProviderConfigured[providerId] ? 'Configured' : 'Credentials missing'}
                              </span>
                            </div>
                            <div className="flex items-center gap-1">
                              <Button
                                type="button"
                                size="icon"
                                variant="ghost"
                                className="h-7 w-7"
                                onClick={() => movePaidAiProvider(providerId, -1)}
                                disabled={index === 0 || selectedScanAiPolicy === 'local_only'}
                              >
                                <ArrowUp className="h-3.5 w-3.5" />
                              </Button>
                              <Button
                                type="button"
                                size="icon"
                                variant="ghost"
                                className="h-7 w-7"
                                onClick={() => movePaidAiProvider(providerId, 1)}
                                disabled={index === paidAiOrder.length - 1 || selectedScanAiPolicy === 'local_only'}
                              >
                                <ArrowDown className="h-3.5 w-3.5" />
                              </Button>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="rounded-lg border border-border/60 p-4 space-y-3">
                  <div className="space-y-1">
                    <Label>Advanced provider routing overrides</Label>
                    <p className="text-xs text-muted-foreground">
                      These are runtime overrides for manual/interactive operations. Batch scans follow the scan policy above and only use these overrides if they escalate into a paid provider path.
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
                      <Label>Advanced AI depth override</Label>
                      <span className="text-xs font-medium text-muted-foreground">{selectedAiLevelMeta.label}</span>
                    </div>
                    <Slider
                      min={0}
                      max={AI_USAGE_LEVELS.length - 1}
                      step={1}
                      value={[selectedAiLevelIndex]}
                      onValueChange={(values) => {
                        const idx = Math.max(0, Math.min(AI_USAGE_LEVELS.length - 1, Number(values?.[0] ?? 0)));
                        const level = AI_USAGE_LEVELS[idx]?.value ?? 'auto';
                        updateConfig({ AI_USAGE_LEVEL: level });
                      }}
                    />
                    <p className="text-xs text-muted-foreground">
                      {selectedAiLevelMeta.description} Leave this on <span className="text-foreground">Auto</span> for normal scans. Raise it only for repair or deep-enrichment runs.
                    </p>
                  </div>
                  <div className="rounded-lg border border-border/60 p-3 space-y-3">
                    <div className="space-y-1">
                      <Label>Web search backend</Label>
                      <p className="text-xs text-muted-foreground">
                        Serper is the hosted search backend. Ollama web search is the local AI fallback. Paid AI web-search should stay exceptional.
                      </p>
                    </div>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-[220px_minmax(0,1fr)]">
                      <Select
                        value={String(config.WEB_SEARCH_PROVIDER || 'auto')}
                        onValueChange={(value: NonNullable<PMDAConfig['WEB_SEARCH_PROVIDER']>) => updateConfig({ WEB_SEARCH_PROVIDER: value })}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Choose backend" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="auto">Auto (Serper → Ollama)</SelectItem>
                          <SelectItem value="serper">Serper only</SelectItem>
                          <SelectItem value="ollama">Ollama only</SelectItem>
                          <SelectItem value="ai_only">Paid AI only</SelectItem>
                          <SelectItem value="disabled">Disabled</SelectItem>
                        </SelectContent>
                      </Select>
                      <div className="flex items-center justify-between rounded-md border border-border/70 bg-background/40 px-3 py-2">
                        <div className="space-y-0.5">
                          <div className="text-sm font-medium">Allow paid AI web-search fallback</div>
                          <p className="text-[11px] text-muted-foreground">
                            Keep this off for bulk scans. It is ignored automatically in local-only mode.
                          </p>
                        </div>
                        <Switch
                          checked={Boolean(config.USE_AI_WEB_SEARCH_FALLBACK)}
                          onCheckedChange={(checked) => updateConfig({ USE_AI_WEB_SEARCH_FALLBACK: Boolean(checked) })}
                          disabled={selectedScanAiPolicy === 'local_only'}
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                      <div className="rounded-md border border-border/60 bg-background/30 px-3 py-2 text-[11px] text-muted-foreground">
                        Effective scan web step: <span className="text-foreground">{effectiveScanWebSearch || 'deterministic only'}</span>
                      </div>
                      <div className="rounded-md border border-border/60 bg-background/30 px-3 py-2 text-[11px] text-muted-foreground">
                        Provider search order: <span className="text-foreground">{localWebOrder.join(' → ') || 'none'}</span>
                      </div>
                    </div>
                  </div>
                  <div className="rounded-lg border border-border/60 p-3 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div className="space-y-1">
                        <Label>Ollama runtime</Label>
                        <p className="text-xs text-muted-foreground">
                          Detect a local Ollama runtime on your LAN, verify it, then pull models directly from PMDA.
                        </p>
                      </div>
                      <div className="flex items-center gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="gap-2"
                          onClick={() => void discoverOllamaHosts()}
                          disabled={ollamaDiscoveryBusy}
                        >
                          {ollamaDiscoveryBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Server className="w-4 h-4" />}
                          Discover on LAN
                        </Button>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="gap-2"
                          onClick={() => void refreshOllamaPullStatus()}
                        >
                          <RefreshCw className="w-4 h-4" />
                          Refresh
                        </Button>
                      </div>
                    </div>
                    <div className="rounded-lg border border-border/60 bg-background/20 p-3 space-y-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <Cpu className="h-4 w-4 text-primary" />
                        <div className="text-sm font-medium text-foreground">Automatic local model routing</div>
                        <Badge variant="secondary">Auto</Badge>
                      </div>
                      <p className="text-[11px] text-muted-foreground">
                        PMDA keeps the fast bulk scan on <span className="text-foreground">{String(config.SCAN_AI_LOCAL_BULK_MODEL || ollamaConfiguredModel || 'qwen3:4b')}</span>
                        {' '}and only escalates ambiguous or long-form local AI work to{' '}
                        <span className="text-foreground">{String(config.SCAN_AI_LOCAL_HARD_MODEL || ollamaConfiguredHardModel || 'qwen3:14b')}</span>
                        {config.SCAN_AI_LOCAL_HARD_AVAILABLE === false ? ' when that model becomes available on the Ollama runtime.' : '.'}
                      </p>
                    </div>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(220px,320px)]">
                      <div className="space-y-2">
                        <Label>Bulk local model</Label>
                        <Input
                          placeholder="qwen3:4b"
                          value={String(config.OLLAMA_MODEL || '')}
                          onChange={(e) => updateConfig({ OLLAMA_MODEL: e.target.value })}
                        />
                        <p className="text-[11px] text-muted-foreground">
                          Fast path for bulk scan matching, provider arbitration and dedupe in local-first mode.
                        </p>
                      </div>
                      <div className="space-y-2">
                        <Label>Hard-cases local model</Label>
                        <Input
                          placeholder="qwen3:14b"
                          value={String(config.OLLAMA_COMPLEX_MODEL || '')}
                          onChange={(e) => updateConfig({ OLLAMA_COMPLEX_MODEL: e.target.value })}
                        />
                        <p className="text-[11px] text-muted-foreground">
                          Used automatically only for ambiguous disambiguation, tough arbitration and long-form local AI work.
                        </p>
                      </div>
                      <div className="space-y-2">
                        <Label>Ollama URL</Label>
                        <Input
                          placeholder="http://localhost:11434"
                          value={String(config.OLLAMA_URL || '')}
                          onChange={(e) => updateConfig({ OLLAMA_URL: e.target.value })}
                        />
                        <p className="text-[11px] text-muted-foreground">
                          PMDA will use this runtime first when the selected scan policy includes local AI.
                        </p>
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        className="gap-2"
                        onClick={() => void testOllamaConnection()}
                        disabled={ollamaConnectionBusy}
                      >
                        {ollamaConnectionBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Check className="w-4 h-4" />}
                        Test connection
                      </Button>
                      <Badge variant={ollamaConfigured ? 'secondary' : 'outline'}>
                        {ollamaConfigured ? 'Configured' : 'URL/model missing'}
                      </Badge>
                      <Badge variant={ollamaModelInstalled ? 'outline' : 'destructive'}>
                        {ollamaModelInstalled ? 'Bulk model available or not checked yet' : 'Bulk model missing on runtime'}
                      </Badge>
                      <Badge variant={ollamaHardModelInstalled ? 'outline' : 'destructive'}>
                        {ollamaHardModelInstalled ? 'Hard-cases model available or not checked yet' : 'Hard-cases model missing on runtime'}
                      </Badge>
                    </div>
                    {ollamaConnectionMessage ? (
                      <div className="rounded-md border border-border/60 bg-background/30 px-3 py-2 text-[11px] text-muted-foreground">
                        {ollamaConnectionMessage}
                      </div>
                    ) : null}
                    {ollamaAvailableModels.length > 0 ? (
                      <div className="rounded-md border border-border/60 bg-background/20 p-3 space-y-2">
                        <div className="text-xs font-medium text-foreground">Detected models</div>
                        <div className="flex flex-wrap gap-1.5">
                          {ollamaAvailableModels.slice(0, 12).map((model) => (
                            <Badge
                              key={model}
                              variant={model === ollamaConfiguredModel || model === ollamaConfiguredHardModel ? 'default' : 'outline'}
                            >
                              {model}
                              {model === ollamaConfiguredModel ? ' · bulk' : ''}
                              {model === ollamaConfiguredHardModel ? ' · hard' : ''}
                            </Badge>
                          ))}
                          {ollamaAvailableModels.length > 12 ? <Badge variant="outline">+{ollamaAvailableModels.length - 12} more</Badge> : null}
                        </div>
                      </div>
                    ) : null}
                    {ollamaDiscoveryResults.length > 0 ? (
                      <div className="rounded-md border border-border/60 bg-background/20 p-3 space-y-2">
                        <div className="text-xs font-medium text-foreground">Discovered endpoints</div>
                        <div className="space-y-2">
                          {ollamaDiscoveryResults.map((row) => (
                            <div key={row.url} className="flex flex-col gap-2 rounded-md border border-border/60 bg-background/40 px-3 py-2 md:flex-row md:items-center md:justify-between">
                              <div className="space-y-1">
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="text-sm text-foreground">{row.url}</span>
                                  <Badge variant={row.ok ? 'default' : 'outline'}>{row.ok ? 'Reachable' : 'Unavailable'}</Badge>
                                  {row.ok ? <Badge variant="outline">{row.model_count} model{row.model_count === 1 ? '' : 's'}</Badge> : null}
                                </div>
                                <p className="text-[11px] text-muted-foreground">{row.message}</p>
                              </div>
                              <Button
                                type="button"
                                variant="outline"
                                size="sm"
                                onClick={() => {
                                  updateConfig({ OLLAMA_URL: row.url });
                                  void testOllamaConnection(row.url, true);
                                }}
                                disabled={!row.ok}
                              >
                                Use this runtime
                              </Button>
                            </div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
                      <Input
                        placeholder={String(config.OLLAMA_MODEL || 'qwen3:4b')}
                        value={ollamaPullModel}
                        onChange={(e) => setOllamaPullModel(e.target.value)}
                      />
                      <Button
                        type="button"
                        className="gap-2"
                        onClick={() => void startOllamaModelPull()}
                        disabled={ollamaPullBusy || Boolean(ollamaPullStatus?.active)}
                      >
                        {ollamaPullBusy || ollamaPullStatus?.active ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
                        Download model
                      </Button>
                    </div>
                    {ollamaPullStatus ? (
                      <div className="space-y-2 rounded-md border border-border/60 bg-background/30 px-3 py-2">
                        <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
                          <span className="font-medium text-foreground">
                            {ollamaPullStatus.model || 'No recent model pull'}
                          </span>
                          <Badge variant={ollamaPullStatus.status === 'error' ? 'destructive' : ollamaPullStatus.active ? 'secondary' : 'outline'}>
                            {ollamaPullStatus.status}
                          </Badge>
                        </div>
                        <p className="text-[11px] text-muted-foreground">{ollamaPullStatus.message || 'No model pull running.'}</p>
                        <Progress value={Number(ollamaPullStatus.progress || 0)} />
                        <div className="flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
                          <span>URL: <span className="text-foreground">{ollamaPullStatus.url || String(config.OLLAMA_URL || 'not set')}</span></span>
                          <span>Downloaded: <span className="text-foreground">{Number(ollamaPullStatus.completed || 0).toLocaleString()}</span></span>
                          <span>Total: <span className="text-foreground">{Number(ollamaPullStatus.total || 0).toLocaleString()}</span></span>
                        </div>
                        {ollamaPullStatus.error ? <p className="text-xs text-destructive">{ollamaPullStatus.error}</p> : null}
                      </div>
                    ) : null}
                    <p className="text-[11px] text-muted-foreground">
                      Recommended local scan model: <span className="text-foreground">qwen3:4b</span> for throughput, <span className="text-foreground">qwen3:14b</span> for harder repair runs.
                    </p>
                  </div>
                  <div className="rounded-lg border border-border/60 p-3 space-y-2">
                    <div className="flex items-center justify-between gap-3">
                      <div className="space-y-1">
                        <Label>Auto-fetch soft-match album profiles</Label>
                        <p className="text-xs text-muted-foreground">
                          When enabled, PMDA can fetch provider album metadata for SOFT_MATCH albums such as Last.fm descriptions and community pulse signals.
                        </p>
                      </div>
                      <Switch
                        checked={Boolean(config.USE_AI_FOR_SOFT_MATCH_PROFILES)}
                        onCheckedChange={(checked) => updateConfig({ USE_AI_FOR_SOFT_MATCH_PROFILES: Boolean(checked) })}
                      />
                    </div>
                    <p className="text-[11px] text-muted-foreground">
                      Web+AI review generation is no longer automatic during scans. This only affects provider profile fetches for soft matches.
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
                  <TabsList className="grid h-auto w-full grid-cols-2 gap-2 md:grid-cols-3 xl:grid-cols-5">
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
                      const scrobbleConnected = Boolean(lastfmAuthStatus?.connected);
                      const scrobblePending = Boolean(lastfmAuthStatus?.pending);
                      const scrobbleReconnectRequired = Boolean(lastfmAuthStatus?.reconnect_required);
                      const scrobbleReady = configured && scrobbleConnected;
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
                          <Separator />
                          <div className="space-y-3">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div className="space-y-1">
                              <Label>Last.fm scrobbling</Label>
                              <p className="text-xs text-muted-foreground">
                                Connect a Last.fm user session to scrobble finished tracks and optionally update now playing. After you authorize PMDA on Last.fm, the connection completes automatically.
                              </p>
                              {lastfmAuthStatus?.message ? (
                                <p className={`text-xs ${scrobbleReconnectRequired ? 'text-warning' : 'text-muted-foreground'}`}>
                                  {lastfmAuthStatus.message}
                                </p>
                              ) : null}
                            </div>
                            <div className="flex flex-wrap items-center gap-2">
                              <Badge variant={scrobbleConnected ? 'default' : scrobbleReconnectRequired ? 'destructive' : scrobblePending ? 'secondary' : 'outline'}>
                                  {scrobbleConnected ? `Connected${lastfmAuthStatus?.session_name ? `: ${lastfmAuthStatus.session_name}` : ''}` : scrobbleReconnectRequired ? 'Reconnect required' : scrobblePending ? 'Authorization pending' : 'Not connected'}
                              </Badge>
                                {lastfmAuthStatus?.auth_url ? (
                                  <Button
                                    type="button"
                                    size="sm"
                                    variant="outline"
                                    className="h-8 gap-2"
                                    onClick={() => window.open(lastfmAuthStatus.auth_url, '_blank', 'noopener,noreferrer')}
                                  >
                                    <ExternalLink className="w-3.5 h-3.5" />
                                    Open Last.fm
                                  </Button>
                                ) : null}
                                <Button
                                  type="button"
                                  size="sm"
                                  variant="outline"
                                  className="h-8 gap-2"
                                  onClick={() => void refreshLastfmAuthStatus()}
                                  disabled={lastfmAuthBusy}
                                >
                                  {lastfmAuthBusy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RefreshCw className="w-3.5 h-3.5" />}
                                  Check status
                                </Button>
                              </div>
                            </div>
                            <div className="flex flex-wrap items-center gap-2">
                              <Button
                                type="button"
                                size="sm"
                                className="h-8 gap-2"
                                onClick={() => void startLastfmScrobbleAuth()}
                                disabled={!configured || lastfmAuthBusy}
                              >
                                {lastfmAuthBusy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
                                {scrobblePending ? 'Reconnect' : 'Connect'}
                              </Button>
                              <Button
                                type="button"
                                size="sm"
                                variant="outline"
                                className="h-8 gap-2"
                                onClick={() => void disconnectLastfmScrobbleAuth()}
                                disabled={!scrobbleConnected || lastfmAuthBusy}
                              >
                                <X className="w-3.5 h-3.5" />
                                Disconnect
                              </Button>
                            </div>
                            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                              <div className="rounded-lg border border-border/70 p-3">
                                <div className="flex items-center justify-between gap-3">
                                  <div className="space-y-1">
                                  <div className="text-sm font-medium">Scrobble completed tracks</div>
                                    <p className="text-xs text-muted-foreground">Send qualifying completed listens to Last.fm. This preference is saved even before reconnecting.</p>
                                  </div>
                                  <Switch
                                    checked={Boolean(config.LASTFM_SCROBBLE_ENABLED)}
                                    onCheckedChange={(checked) => updateConfig({ LASTFM_SCROBBLE_ENABLED: checked })}
                                    disabled={!configured}
                                  />
                                </div>
                              </div>
                              <div className="rounded-lg border border-border/70 p-3">
                                <div className="flex items-center justify-between gap-3">
                                  <div className="space-y-1">
                                    <div className="text-sm font-medium">Update now playing</div>
                                    <p className="text-xs text-muted-foreground">Push live current-track status to Last.fm on playback start. This preference is saved even before reconnecting.</p>
                                  </div>
                                  <Switch
                                    checked={Boolean(config.LASTFM_NOW_PLAYING_ENABLED)}
                                    onCheckedChange={(checked) => updateConfig({ LASTFM_NOW_PLAYING_ENABLED: checked })}
                                    disabled={!configured}
                                  />
                                </div>
                              </div>
                            </div>
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
                  Full maintenance reset. This clears PMDA library data and restarts automatically, while keeping settings, users, and stored credentials.
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
            <div className="fixed inset-0 z-[10000] bg-foreground/80 backdrop-blur-md" />
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
