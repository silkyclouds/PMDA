import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  ChevronDown,
  FolderOutput,
  Globe,
  Loader2,
  PlayCircle,
  Plus,
  RefreshCw,
  Server,
  Sparkles,
  Trash2,
  Workflow,
} from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import type {
  BootstrapStatus,
  ManagedRuntimeBundleStatus,
  ManagedRuntimeStatusResponse,
  PMDAConfig,
  ScanPreflightResult,
  ScanProgress,
} from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { FolderBrowserInput } from '@/components/settings/FolderBrowserInput';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { PasswordInput } from '@/components/ui/password-input';
import { Progress } from '@/components/ui/progress';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';

type Props = {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  configured?: boolean;
  presentation?: 'embedded' | 'modal';
  dirty?: boolean;
  isSaving?: boolean;
  onSave?: () => Promise<boolean>;
  onClose?: () => void;
  onOpenGuidedSetup?: () => void;
};

type WorkflowMode = NonNullable<PMDAConfig['LIBRARY_WORKFLOW_MODE']>;
type StackMode = 'online' | 'local';
type ExternalAiProvider = 'openai-api' | 'anthropic' | 'google';
type StepId = 'workflow' | 'folders' | 'metadata' | 'setup' | 'pipeline' | 'review';
type DedupeMode = 'ignore' | 'detect' | 'move';
type IncompleteMode = 'keep' | 'move';
type ProviderStatus = {
  variant: 'default' | 'secondary' | 'outline' | 'destructive';
  label: string;
  message: string;
};

type Blocker = {
  key: string;
  title: string;
  detail: string;
  sectionId?: string;
  stepId?: StepId;
};

type ManagedRuntimeLogEntry = {
  log_id: number;
  bundle_type: string;
  service_name: string;
  level: string;
  message: string;
  created_at: number;
};

const STEP_ORDER: Array<{ id: StepId; label: string; title: string }> = [
  { id: 'workflow', label: 'Step 1', title: 'Workflow' },
  { id: 'folders', label: 'Step 2', title: 'Folders' },
  { id: 'metadata', label: 'Step 3', title: 'Metadata mode' },
  { id: 'setup', label: 'Step 4', title: 'Sources & runtime' },
  { id: 'pipeline', label: 'Step 5', title: 'Pipeline' },
  { id: 'review', label: 'Step 6', title: 'Review' },
];

const WORKFLOW_OPTIONS: Array<{
  value: WorkflowMode;
  label: string;
  modeName: string;
  description: string;
  whenToUse: string;
  asksFor: string;
  diagram: string[];
}> = [
  {
    value: 'managed',
    label: 'Sort new arrivals',
    modeName: 'Managed',
    description: 'Use this if new albums arrive in a separate intake folder first.',
    whenToUse: 'You have one folder for new arrivals and another one for the final clean library.',
    asksFor: 'PMDA will ask for: intake folder, clean library, dupes, incomplete albums.',
    diagram: ['New arrivals', 'PMDA sorts', 'Final library'],
  },
  {
    value: 'mirror',
    label: 'Build a clean copy',
    modeName: 'Mirror',
    description: 'Use this if you already have a source library and want a separate clean copy.',
    whenToUse: 'Your current library stays untouched. PMDA writes the cleaned result somewhere else.',
    asksFor: 'PMDA will ask for: source library, clean library, dupes, incomplete albums.',
    diagram: ['Current library', 'PMDA checks', 'Clean copy'],
  },
  {
    value: 'inplace',
    label: 'Use the current library',
    modeName: 'In place',
    description: 'Use this if the folder you already have is the final library.',
    whenToUse: 'PMDA works directly in the current library. No second clean library is created.',
    asksFor: 'PMDA will ask for: current library, dupes, incomplete albums.',
    diagram: ['Current library', 'PMDA checks', 'Serve here'],
  },
];

const EXTERNAL_AI_OPTIONS: Array<{
  value: ExternalAiProvider;
  label: string;
  description: string;
  docsUrl: string;
}> = [
  {
    value: 'openai-api',
    label: 'OpenAI',
    description: 'Best when you want the broadest model choice and the smoothest validation path.',
    docsUrl: 'https://platform.openai.com/api-keys',
  },
  {
    value: 'anthropic',
    label: 'Anthropic',
    description: 'Useful if Claude is already your paid provider for higher-context reasoning.',
    docsUrl: 'https://console.anthropic.com/settings/keys',
  },
  {
    value: 'google',
    label: 'Google',
    description: 'Useful if Gemini is already your paid provider and you want one billing surface.',
    docsUrl: 'https://aistudio.google.com/app/apikey',
  },
];

const MATERIALIZATION_OPTIONS: Array<{
  value: NonNullable<PMDAConfig['EXPORT_LINK_STRATEGY']>;
  label: string;
  description: string;
}> = [
  { value: 'hardlink', label: 'Hardlink', description: 'Fastest and usually the best default. No extra copy if the filesystem supports it.' },
  { value: 'symlink', label: 'Symlink', description: 'Keeps references to original files. Useful when hardlinks are not appropriate.' },
  { value: 'copy', label: 'Copy', description: 'Safest but duplicates data on disk.' },
  { value: 'move', label: 'Move', description: 'Relocates files physically into the published library.' },
];

const PLAYER_TARGET_OPTIONS: Array<{
  value: NonNullable<PMDAConfig['PIPELINE_PLAYER_TARGET']>;
  label: string;
  description: string;
}> = [
  { value: 'none', label: 'Not now', description: 'Skip player sync for the first scan.' },
  { value: 'plex', label: 'Plex', description: 'Refresh Plex after the pipeline completes.' },
  { value: 'jellyfin', label: 'Jellyfin', description: 'Refresh Jellyfin after the pipeline completes.' },
  { value: 'navidrome', label: 'Navidrome', description: 'Refresh Navidrome after the pipeline completes.' },
];

const LOCAL_STACK_RECOMMENDED_FREE_GB = 40;
const MANAGED_RUNTIME_ACTIVE_STATES = new Set(['preflight', 'pulling', 'creating', 'importing', 'waiting_health', 'updating']);
const RECOVERABLE_MUSICBRAINZ_BOOTSTRAP_ERRORS = [
  'Provision script not found:',
  'Missing required command: curl',
  'Command failed (1): /app/scripts/provision_musicbrainz_mirror_unraid.sh',
];

function normalizeFolderPath(input: string): string {
  const raw = String(input || '').trim();
  if (!raw) return '';
  if (raw === '/') return '/';
  return raw.replace(/\/+$/, '') || raw;
}

function parsePathList(value: unknown): string[] {
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
          // fall through
        }
      }
      if (s.includes(',')) {
        const parts = s.split(',').map((part) => part.trim()).filter(Boolean);
        if (parts.length > 1) {
          queue.push(...parts);
          continue;
        }
      }
      const normalized = normalizeFolderPath(s);
      if (normalized && !seen.has(normalized)) {
        seen.add(normalized);
        out.push(normalized);
      }
      continue;
    }
    const normalized = normalizeFolderPath(String(item));
    if (normalized && !seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized);
    }
  }

  return out;
}

function stringifyPathList(paths: string[]): string {
  return paths
    .map((path) => normalizeFolderPath(path))
    .filter(Boolean)
    .join(', ');
}

function isBundleReady(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  return bundle.state === 'ready' && Boolean(bundle.health?.available);
}

function bundleLatestActionStatus(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  return String(bundle?.latest_action?.status || '').trim().toLowerCase();
}

function bundleFailureMessage(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  return String(
    bundle?.latest_action?.error
    || bundle?.last_error
    || bundle?.phase_message
    || '',
  ).trim();
}

function bundleHasRecoverableMusicBrainzFailure(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  const text = bundleFailureMessage(bundle);
  if (!text) return false;
  return RECOVERABLE_MUSICBRAINZ_BOOTSTRAP_ERRORS.some((needle) => text.includes(needle));
}

function effectiveBundleState(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  if (!bundle) return 'absent';
  const state = String(bundle.state || '').trim().toLowerCase() || 'idle';
  const latestStatus = bundleLatestActionStatus(bundle);
  const hasReachableRuntime = Boolean(bundle.effective_url || bundle.health?.available);
  if (
    !hasReachableRuntime
    && latestStatus === 'failed'
    && (MANAGED_RUNTIME_ACTIVE_STATES.has(state) || state === 'failed')
  ) {
    return bundleHasRecoverableMusicBrainzFailure(bundle) ? 'idle' : 'failed';
  }
  return state;
}

function effectiveBundlePhase(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  const state = effectiveBundleState(bundle);
  if (state === 'idle' || state === 'failed' || state === 'ready') return state;
  return String(bundle?.phase || '').trim().toLowerCase() || state;
}

function effectiveBundleMessage(
  bundle: ManagedRuntimeBundleStatus | null | undefined,
  fallbackIdleMessage = 'Not started yet.',
): string {
  if (!bundle) return fallbackIdleMessage;
  const state = effectiveBundleState(bundle);
  if (state === 'idle') return fallbackIdleMessage;
  if (state === 'failed') return bundleFailureMessage(bundle) || 'Provisioning failed.';
  return String(bundle.phase_message || '').trim() || fallbackIdleMessage;
}

function bundleModels(bundle: ManagedRuntimeBundleStatus | null | undefined): string[] {
  const metaModels = Array.isArray(bundle?.meta?.models) ? bundle.meta.models : [];
  const healthModels = Array.isArray(bundle?.health?.models) ? bundle.health.models : [];
  return Array.from(new Set([...healthModels, ...metaModels].map((value) => String(value || '').trim()).filter(Boolean)));
}

function managedBundleProgress(
  bundle: ManagedRuntimeBundleStatus | null | undefined,
  options?: { requiredModels?: string[] },
): number {
  if (!bundle) return 0;
  const mode = String(bundle.mode || '').trim().toLowerCase();
  const requiredModels = options?.requiredModels || [];
  const models = bundleModels(bundle);
  const hasRequiredModels = requiredModels.length === 0 || requiredModels.every((model) => models.includes(model));
  if (isBundleReady(bundle) && hasRequiredModels) return 100;

  const state = effectiveBundleState(bundle);
  const phase = effectiveBundlePhase(bundle);
  const combined = `${state} ${phase}`;
  const metaProgressRaw = Number((bundle.meta as Record<string, unknown> | undefined)?.progress);
  if (Number.isFinite(metaProgressRaw) && metaProgressRaw > 0 && state !== 'idle' && state !== 'failed') {
    return Math.max(0, Math.min(100, Math.round(metaProgressRaw)));
  }

  if (mode === 'absent' || state === 'idle' || combined.trim() === '') return 0;
  if (combined.includes('failed')) return 100;
  if (combined.includes('ready')) return hasRequiredModels ? 100 : 88;
  if (combined.includes('pull')) return 40;
  if (combined.includes('updat') || combined.includes('import')) return 58;
  if (combined.includes('start') || combined.includes('wait')) return 76;
  if (combined.includes('creat') || combined.includes('provision')) return 24;
  if (combined.includes('preflight') || combined.includes('check')) return 18;
  return 8;
}

function managedBundleDisplayProgress(
  bundle: ManagedRuntimeBundleStatus | null | undefined,
  options?: { requiredModels?: string[] },
): number {
  if (!bundle) return 0;
  const meta = (bundle.meta || {}) as Record<string, unknown>;
  if (bundle.bundle_type === 'musicbrainz_local' && effectiveBundlePhase(bundle) === 'importing') {
    const rawDownloadProgress = Number(meta.download_progress);
    if (Number.isFinite(rawDownloadProgress) && rawDownloadProgress > 0) {
      return Math.max(0, Math.min(100, Math.round(rawDownloadProgress)));
    }
  }
  return managedBundleProgress(bundle, options);
}

function hasManagedBundleStarted(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  if (bundle.effective_url || bundle.health?.available) return true;
  const mode = String(bundle.mode || '').trim().toLowerCase();
  const state = effectiveBundleState(bundle);
  const phase = effectiveBundlePhase(bundle);
  if (mode && mode !== 'absent') {
    if ((state === 'idle' || state === 'failed') && !bundle.effective_url && !bundle.health?.available) {
      return false;
    }
    return true;
  }
  if (state && state !== 'idle' && state !== 'absent') return true;
  if (phase && phase !== 'idle') return true;
  return false;
}

function formatEtaSeconds(totalSeconds: number): string {
  const seconds = Math.max(0, Math.round(Number(totalSeconds) || 0));
  if (!seconds) return '';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function managedBundleEta(bundle: ManagedRuntimeBundleStatus | null | undefined): string {
  const meta = (bundle?.meta || {}) as Record<string, unknown>;
  const explicit = String(meta.eta_text || '').trim();
  if (explicit) return explicit;
  const progress = Number(meta.progress);
  const startedAt = Number(meta.started_at);
  if (!Number.isFinite(progress) || progress <= 0 || progress >= 100 || !Number.isFinite(startedAt) || startedAt <= 0) {
    return '';
  }
  const elapsed = Math.max(1, (Date.now() / 1000) - startedAt);
  const remaining = elapsed * ((100 - progress) / progress);
  return formatEtaSeconds(remaining);
}

function WorkflowDiagram({ nodes }: { nodes: string[] }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
      {nodes.map((node, index) => (
        <div key={node} className="flex items-center gap-2">
          <span className="rounded-full border border-border/70 bg-background/70 px-3 py-1.5 font-medium text-foreground">
            {node}
          </span>
          {index < nodes.length - 1 ? <ArrowRight className="h-3.5 w-3.5 text-primary/70" /> : null}
        </div>
      ))}
    </div>
  );
}

function managedBundleLabel(bundleType: string): string {
  return bundleType === 'musicbrainz_local' ? 'MusicBrainz mirror' : 'Ollama';
}

function compactManagedLogMessage(message: string): string {
  const raw = String(message || '').trim();
  if (!raw) return '';
  return raw
    .replace(/^[A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4}\s*:\s*/g, '')
    .replace(/^\d{4}\/\d{2}\/\d{2}\s+\d{2}:\d{2}:\d{2}\s*/g, '')
    .replace(/^Command finished successfully\.\s*/i, 'Command finished successfully. ')
    .trim();
}

function formatRuntimeLogTime(createdAt: number): string {
  const value = Number(createdAt);
  if (!Number.isFinite(value) || value <= 0) return '';
  try {
    return new Date(value * 1000).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  } catch {
    return '';
  }
}

function normalizeExternalProvider(value: unknown): ExternalAiProvider {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'anthropic' || raw === 'google') return raw;
  return 'openai-api';
}

function providerCredential(config: Partial<PMDAConfig>, provider: ExternalAiProvider): string {
  if (provider === 'anthropic') return String(config.ANTHROPIC_API_KEY || '').trim();
  if (provider === 'google') return String(config.GOOGLE_API_KEY || '').trim();
  return String(config.OPENAI_API_KEY || '').trim();
}

function workflowNeedsPublishedLibrary(mode: WorkflowMode): boolean {
  return mode === 'managed' || mode === 'mirror';
}

function PathListEditor({
  label,
  description,
  paths,
  onChange,
  placeholder,
  selectLabel,
  browseRoot,
  lockToBrowseRoot = false,
  allowManualEntry = true,
}: {
  label: string;
  description: string;
  paths: string[];
  onChange: (paths: string[]) => void;
  placeholder: string;
  selectLabel: string;
  browseRoot?: string;
  lockToBrowseRoot?: boolean;
  allowManualEntry?: boolean;
}) {
  const [draftPaths, setDraftPaths] = useState<string[]>(paths.length > 0 ? [...paths] : ['']);

  useEffect(() => {
    const normalizedIncoming = paths.map((path) => normalizeFolderPath(path)).filter(Boolean);
    setDraftPaths((prev) => {
      const normalizedPrev = prev.map((path) => normalizeFolderPath(path)).filter(Boolean);
      return JSON.stringify(normalizedPrev) === JSON.stringify(normalizedIncoming)
        ? prev
        : (normalizedIncoming.length > 0 ? [...normalizedIncoming] : ['']);
    });
  }, [paths]);

  const setPath = (index: number, value: string) => {
    const next = [...draftPaths];
    next[index] = normalizeFolderPath(value);
    setDraftPaths(next);
    onChange(next.filter(Boolean));
  };

  const addPath = () => {
    if (draftPaths.every((value) => value.trim())) {
      setDraftPaths([...draftPaths, '']);
    }
  };

  const removePath = (index: number) => {
    const next = [...draftPaths];
    next.splice(index, 1);
    const safeNext = next.length > 0 ? next : [''];
    setDraftPaths(safeNext);
    onChange(next.filter(Boolean));
  };

  return (
    <div className="space-y-3 rounded-2xl border border-border/70 bg-background/50 p-4">
      <div className="space-y-1">
        <Label>{label}</Label>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>
      <div className="space-y-2">
        {draftPaths.map((path, index) => (
          <div key={`${label}-${index}`} className="flex items-start gap-2">
            <div className="min-w-0 flex-1">
              <FolderBrowserInput
                value={path}
                onChange={(next) => setPath(index, next || '')}
                placeholder={placeholder}
                selectLabel={selectLabel}
                compact
                browseRoot={browseRoot}
                lockToBrowseRoot={lockToBrowseRoot}
                allowManualEntry={allowManualEntry}
              />
            </div>
            {draftPaths.length > 1 ? (
              <Button type="button" variant="outline" size="icon" className="mt-0.5 shrink-0" onClick={() => removePath(index)}>
                <Trash2 className="h-4 w-4" />
              </Button>
            ) : null}
          </div>
        ))}
      </div>
      <Button type="button" variant="outline" size="sm" className="gap-2" onClick={addPath}>
        <Plus className="h-3.5 w-3.5" />
        Add another folder
      </Button>
    </div>
  );
}

function SingleFolderField({
  label,
  description,
  value,
  onChange,
  placeholder,
  selectLabel,
  browseRoot,
  lockToBrowseRoot = false,
  allowManualEntry = true,
}: {
  label: string;
  description: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  selectLabel: string;
  browseRoot?: string;
  lockToBrowseRoot?: boolean;
  allowManualEntry?: boolean;
}) {
  return (
    <div className="space-y-3 rounded-2xl border border-border/70 bg-background/50 p-4">
      <div className="space-y-1">
        <Label>{label}</Label>
        <p className="text-xs text-muted-foreground">{description}</p>
      </div>
      <FolderBrowserInput
        value={value}
        onChange={(next) => onChange(next || '')}
        placeholder={placeholder}
        selectLabel={selectLabel}
        compact
        browseRoot={browseRoot}
        lockToBrowseRoot={lockToBrowseRoot}
        allowManualEntry={allowManualEntry}
      />
    </div>
  );
}

function scrollToSection(id: string): void {
  const node = document.getElementById(id);
  if (!node) return;
  node.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

export function OnboardingWizard({
  config,
  updateConfig,
  configured = false,
  presentation = 'embedded',
  dirty = false,
  isSaving = false,
  onSave,
  onClose,
  onOpenGuidedSetup,
}: Props) {
  const navigate = useNavigate();
  const isModalPresentation = presentation === 'modal';
  const [activeStep, setActiveStep] = useState<number>(0);
  const [bootstrap, setBootstrap] = useState<BootstrapStatus | null>(null);
  const [managedStatus, setManagedStatus] = useState<ManagedRuntimeStatusResponse | null>(null);
  const [managedRuntimeLogs, setManagedRuntimeLogs] = useState<ManagedRuntimeLogEntry[]>([]);
  const [providersPreflight, setProvidersPreflight] = useState<ScanPreflightResult | null>(null);
  const [scanProgress, setScanProgress] = useState<ScanProgress | null>(null);
  const [statusLoading, setStatusLoading] = useState<boolean>(true);
  const [providersChecking, setProvidersChecking] = useState<boolean>(false);
  const [scanActionBusy, setScanActionBusy] = useState<boolean>(false);
  const [runtimeActionBusy, setRuntimeActionBusy] = useState<boolean>(false);
  const [localStackConfirmOpen, setLocalStackConfirmOpen] = useState<boolean>(false);
  const [showRuntimeDetails, setShowRuntimeDetails] = useState<boolean>(false);
  const [externalModels, setExternalModels] = useState<string[]>([]);
  const [externalValidationState, setExternalValidationState] = useState<'idle' | 'loading' | 'valid' | 'error'>('idle');
  const [externalValidationMessage, setExternalValidationMessage] = useState<string>('');
  const [pipelineStepConfirmed, setPipelineStepConfirmed] = useState<boolean>(Boolean(configured));
  const localStackAutoPromptedRef = useRef(false);
  const initializedDefaultsRef = useRef(false);
  const providerValidationRequestRef = useRef(0);
  const reviewFingerprintRef = useRef<string>('');

  const workflowMode = (config.LIBRARY_WORKFLOW_MODE || 'managed') as WorkflowMode;
  const workflowMeta = useMemo(
    () => WORKFLOW_OPTIONS.find((option) => option.value === workflowMode) || WORKFLOW_OPTIONS[0]!,
    [workflowMode],
  );

  const intakeRoots = parsePathList(config.LIBRARY_INTAKE_ROOTS);
  const sourceRoots = parsePathList(config.LIBRARY_SOURCE_ROOTS);
  const servingRoot = String(config.LIBRARY_SERVING_ROOT || config.EXPORT_ROOT || '').trim();
  const dupesRoot = String(config.LIBRARY_DUPES_ROOT || config.DUPE_ROOT || '').trim();
  const incompleteRoot = String(config.LIBRARY_INCOMPLETE_ROOT || config.INCOMPLETE_ALBUMS_TARGET_DIR || '').trim();

  const selectedStackMode: StackMode = useMemo(() => {
    const aiProvider = String(config.AI_PROVIDER || '').trim().toLowerCase();
    const webSearchProvider = String(config.WEB_SEARCH_PROVIDER || '').trim().toLowerCase();
    if (config.MUSICBRAINZ_MIRROR_ENABLED || aiProvider === 'ollama' || webSearchProvider === 'ollama') {
      return 'local';
    }
    if (
      aiProvider === 'openai'
      || aiProvider === 'openai-api'
      || aiProvider === 'anthropic'
      || aiProvider === 'google'
      || Boolean(String(config.OPENAI_API_KEY || '').trim())
      || Boolean(String(config.ANTHROPIC_API_KEY || '').trim())
      || Boolean(String(config.GOOGLE_API_KEY || '').trim())
    ) {
      return 'online';
    }
    return 'local';
  }, [config.AI_PROVIDER, config.ANTHROPIC_API_KEY, config.GOOGLE_API_KEY, config.MUSICBRAINZ_MIRROR_ENABLED, config.OPENAI_API_KEY, config.WEB_SEARCH_PROVIDER]);

  const selectedExternalProvider = useMemo(
    () => normalizeExternalProvider(config.AI_PROVIDER),
    [config.AI_PROVIDER],
  );

  const ollamaUrl = String(config.OLLAMA_URL || '').trim() || 'http://localhost:11434';
  const ollamaModel = String(config.OLLAMA_MODEL || '').trim() || 'qwen2.5:3b-instruct';
  const ollamaHardModel = String(config.OLLAMA_COMPLEX_MODEL || '').trim() || 'qwen2.5:14b-instruct';
  const managedConfigRoot = String(config.MANAGED_RUNTIME_CONFIG_ROOT || '').trim();
  const managedDataRoot = String(config.MANAGED_RUNTIME_DATA_ROOT || '').trim();
  const resolvedManagedConfigRoot = managedConfigRoot || '/config/managed-runtime';
  const resolvedManagedDataRoot = managedDataRoot || '/config/managed-runtime-data';
  const managedMbBundle = managedStatus?.bundles?.musicbrainz_local || null;
  const managedOllamaBundle = managedStatus?.bundles?.ollama_local || null;
  const availableManagedModels = useMemo(() => bundleModels(managedOllamaBundle), [managedOllamaBundle]);
  const managedMbCandidate = useMemo(
    () => (managedMbBundle?.candidates || []).find((candidate) => Boolean(candidate.adoptable) && Boolean(candidate.health?.available)) || null,
    [managedMbBundle],
  );
  const managedOllamaCandidate = useMemo(
    () => (managedOllamaBundle?.candidates || []).find((candidate) => Boolean(candidate.adoptable)) || null,
    [managedOllamaBundle],
  );

  const wantsPublishedLibrary = workflowNeedsPublishedLibrary(workflowMode);
  const publishLibraryEnabled = wantsPublishedLibrary ? Boolean(config.PIPELINE_ENABLE_EXPORT) : false;
  const materializationMode = (config.EXPORT_LINK_STRATEGY || config.LIBRARY_MATERIALIZATION_MODE || 'hardlink') as NonNullable<PMDAConfig['EXPORT_LINK_STRATEGY']>;
  const dedupeMode: DedupeMode = !Boolean(config.PIPELINE_ENABLE_DEDUPE)
    ? 'ignore'
    : Boolean(config.AUTO_MOVE_DUPES)
      ? 'move'
      : 'detect';
  const incompleteMode: IncompleteMode = Boolean(config.PIPELINE_ENABLE_INCOMPLETE_MOVE) ? 'move' : 'keep';
  const playerTarget = (Boolean(config.PIPELINE_ENABLE_PLAYER_SYNC)
    ? (config.PIPELINE_PLAYER_TARGET || 'none')
    : 'none') as NonNullable<PMDAConfig['PIPELINE_PLAYER_TARGET']>;

  const shouldFetchManagedCandidates = selectedStackMode === 'local'
    && activeStep === 3
    && !runtimeActionBusy;
  const shouldFetchManagedLogs = selectedStackMode === 'local' && activeStep === 3 && showRuntimeDetails;

  const persistIfNeeded = useCallback(async () => {
    if (!isModalPresentation || !dirty || !onSave) return true;
    return onSave();
  }, [dirty, isModalPresentation, onSave]);

  const refreshStatus = useCallback(async () => {
    setStatusLoading(true);
    try {
      const [bootstrapData, managedData, progressData, managedLogsData] = await Promise.all([
        api.getPipelineBootstrapStatus().catch(() => null),
        api.getManagedRuntimeStatus({ skipCandidates: !shouldFetchManagedCandidates }).catch(() => null),
        api.getScanProgress().catch(() => null),
        shouldFetchManagedLogs ? api.getManagedRuntimeLogs({ limit: 18 }).catch(() => null) : Promise.resolve(null),
      ]);
      if (bootstrapData) setBootstrap(bootstrapData);
      if (managedData) setManagedStatus(managedData);
      if (progressData) setScanProgress(progressData);
      if (managedLogsData?.logs) setManagedRuntimeLogs(managedLogsData.logs);
      else if (!shouldFetchManagedLogs) setManagedRuntimeLogs([]);
    } finally {
      setStatusLoading(false);
    }
  }, [shouldFetchManagedCandidates, shouldFetchManagedLogs]);

  const refreshProviderStatus = useCallback(async (options?: { quiet?: boolean }) => {
    setProvidersChecking(true);
    try {
      const next = await api.getProvidersPreflight();
      setProvidersPreflight(next);
      return next;
    } catch (error) {
      if (!options?.quiet) {
        toast.error(error instanceof Error ? error.message : 'Failed to check provider credentials');
      }
      return null;
    } finally {
      setProvidersChecking(false);
    }
  }, []);

  useEffect(() => {
    void refreshStatus();
    const intervalMs = localStackProvisioningActive ? 3000 : 15000;
    const timer = window.setInterval(() => {
      void refreshStatus();
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [refreshStatus]);

  useEffect(() => {
    if (!isModalPresentation) return;
    setActiveStep(0);
  }, [isModalPresentation]);

  useEffect(() => {
    if (initializedDefaultsRef.current) return;
    const updates: Partial<PMDAConfig> = {};

    if (config.USE_MUSICBRAINZ === undefined) updates.USE_MUSICBRAINZ = true;
    if (config.USE_DISCOGS === undefined) updates.USE_DISCOGS = true;
    if (config.USE_LASTFM === undefined) updates.USE_LASTFM = true;
    if (config.USE_BANDCAMP === undefined) updates.USE_BANDCAMP = true;
    if (config.USE_ACOUSTID === undefined) updates.USE_ACOUSTID = true;

    const hasExplicitMetadataChoice = Boolean(
      config.MUSICBRAINZ_MIRROR_ENABLED !== undefined
      || String(config.AI_PROVIDER || '').trim()
      || String(config.WEB_SEARCH_PROVIDER || '').trim()
      || String(config.OLLAMA_RUNTIME_MODE || '').trim()
      || String(config.MUSICBRAINZ_RUNTIME_MODE || '').trim(),
    );

    if (!hasExplicitMetadataChoice) {
      Object.assign(updates, {
        MUSICBRAINZ_MIRROR_ENABLED: true,
        MUSICBRAINZ_RUNTIME_MODE: 'managed',
        PROVIDER_GATEWAY_ENABLED: true,
        PROVIDER_GATEWAY_CACHE_ENABLED: true,
        AI_PROVIDER: 'ollama',
        OLLAMA_RUNTIME_MODE: 'managed',
        OLLAMA_URL: ollamaUrl,
        OLLAMA_MODEL: ollamaModel,
        OLLAMA_COMPLEX_MODEL: ollamaHardModel,
        SCAN_AI_POLICY: 'local_only',
        WEB_SEARCH_PROVIDER: 'ollama',
        AI_USAGE_LEVEL: 'auto',
      });
    }

    if (Object.keys(updates).length > 0) {
      updateConfig(updates);
    }
    initializedDefaultsRef.current = true;
  }, [config.AI_PROVIDER, config.MUSICBRAINZ_MIRROR_ENABLED, config.MUSICBRAINZ_RUNTIME_MODE, config.OLLAMA_RUNTIME_MODE, config.OLLAMA_URL, config.USE_ACOUSTID, config.USE_BANDCAMP, config.USE_DISCOGS, config.USE_LASTFM, config.USE_MUSICBRAINZ, config.WEB_SEARCH_PROVIDER, ollamaHardModel, ollamaModel, ollamaUrl, updateConfig]);

  const applyWorkflowPreset = useCallback((mode: WorkflowMode) => {
    const nextUpdates: Partial<PMDAConfig> = {
      LIBRARY_MODE: 'files',
      LIBRARY_WORKFLOW_MODE: mode,
      PIPELINE_ENABLE_EXPORT: workflowNeedsPublishedLibrary(mode),
      EXPORT_LINK_STRATEGY: (config.EXPORT_LINK_STRATEGY || 'hardlink') as NonNullable<PMDAConfig['EXPORT_LINK_STRATEGY']>,
      LIBRARY_MATERIALIZATION_MODE: (config.LIBRARY_MATERIALIZATION_MODE || config.EXPORT_LINK_STRATEGY || 'hardlink') as NonNullable<PMDAConfig['LIBRARY_MATERIALIZATION_MODE']>,
    };
    updateConfig(nextUpdates);
  }, [config.EXPORT_LINK_STRATEGY, config.LIBRARY_MATERIALIZATION_MODE, updateConfig]);

  const applyStackPreset = useCallback((mode: StackMode) => {
    if (mode === 'local') {
      updateConfig({
        MUSICBRAINZ_MIRROR_ENABLED: true,
        MUSICBRAINZ_RUNTIME_MODE: 'managed',
        PROVIDER_GATEWAY_ENABLED: true,
        PROVIDER_GATEWAY_CACHE_ENABLED: true,
        AI_PROVIDER: 'ollama',
        OLLAMA_RUNTIME_MODE: 'managed',
        OLLAMA_URL: ollamaUrl,
        OLLAMA_MODEL: ollamaModel,
        OLLAMA_COMPLEX_MODEL: ollamaHardModel,
        SCAN_AI_POLICY: 'local_only',
        WEB_SEARCH_PROVIDER: 'ollama',
        AI_USAGE_LEVEL: 'auto',
        USE_BANDCAMP: config.USE_BANDCAMP ?? true,
      });
      return;
    }

    updateConfig({
      MUSICBRAINZ_MIRROR_ENABLED: false,
      MUSICBRAINZ_RUNTIME_MODE: 'absent',
      PROVIDER_GATEWAY_ENABLED: true,
      PROVIDER_GATEWAY_CACHE_ENABLED: true,
      AI_PROVIDER: normalizeExternalProvider(config.AI_PROVIDER),
      OLLAMA_RUNTIME_MODE: 'absent',
      SCAN_AI_POLICY: 'paid_only',
      WEB_SEARCH_PROVIDER: 'auto',
      AI_USAGE_LEVEL: 'auto',
      USE_BANDCAMP: config.USE_BANDCAMP ?? true,
    });
  }, [config.AI_PROVIDER, config.USE_BANDCAMP, ollamaHardModel, ollamaModel, ollamaUrl, updateConfig]);

  const setExternalProvider = useCallback((provider: ExternalAiProvider) => {
    const updates: Partial<PMDAConfig> = {
      AI_PROVIDER: provider,
      MUSICBRAINZ_MIRROR_ENABLED: false,
      MUSICBRAINZ_RUNTIME_MODE: 'absent',
      OLLAMA_RUNTIME_MODE: 'absent',
      SCAN_AI_POLICY: 'paid_only',
      WEB_SEARCH_PROVIDER: 'auto',
      AI_USAGE_LEVEL: 'auto',
    };
    if (provider === 'openai-api') {
      updates.OPENAI_API_KEY = String(config.OPENAI_API_KEY || '');
    }
    updateConfig(updates);
  }, [config.OPENAI_API_KEY, updateConfig]);

  const localStackActivity = useMemo(() => {
    if (selectedStackMode !== 'local') return false;
    return [managedMbBundle, managedOllamaBundle].some((bundle) => {
      if (!bundle) return false;
      const state = String(bundle.state || '').trim().toLowerCase();
      return state !== 'ready' && state !== 'failed' && state !== 'absent' && state !== 'idle';
    });
  }, [managedMbBundle, managedOllamaBundle, selectedStackMode]);

  const localStackProvisioningActive = selectedStackMode === 'local' && (runtimeActionBusy || localStackActivity);

  const missingFolderItems = useMemo(() => {
    const missing: string[] = [];

    if (workflowMode === 'managed' && intakeRoots.length === 0) {
      missing.push('Add at least one intake folder.');
    }
    if ((workflowMode === 'mirror' || workflowMode === 'inplace') && sourceRoots.length === 0) {
      missing.push(workflowMode === 'mirror' ? 'Add at least one source library folder.' : 'Add at least one library folder.');
    }
    if ((workflowMode === 'managed' || workflowMode === 'mirror') && !servingRoot) {
      missing.push('Choose the clean serving library folder.');
    }
    if (!dupesRoot) {
      missing.push('Choose the duplicates folder.');
    }
    if (!incompleteRoot) {
      missing.push('Choose the incomplete albums folder.');
    }

    return missing;
  }, [dupesRoot, incompleteRoot, intakeRoots.length, servingRoot, sourceRoots.length, workflowMode]);

  const foldersReady = missingFolderItems.length === 0;
  const managedRootsReady = selectedStackMode === 'online' ? true : Boolean(resolvedManagedConfigRoot) && Boolean(resolvedManagedDataRoot);
  const managedPreflightReady = selectedStackMode === 'online' ? true : Boolean(managedStatus?.preflight.available);
  const musicbrainzReady = selectedStackMode === 'online' ? true : isBundleReady(managedMbBundle);
  const ollamaReady = selectedStackMode === 'online'
    ? true
    : isBundleReady(managedOllamaBundle)
      && availableManagedModels.includes(ollamaModel)
      && availableManagedModels.includes(ollamaHardModel);
  const localRuntimeReady = selectedStackMode === 'online'
    ? true
    : managedRootsReady && managedPreflightReady && musicbrainzReady && ollamaReady;

  useEffect(() => {
    if (activeStep !== 3 || selectedStackMode !== 'local') {
      localStackAutoPromptedRef.current = false;
      return;
    }
    if (localRuntimeReady || localStackProvisioningActive || statusLoading || localStackConfirmOpen) {
      return;
    }
    if (!managedPreflightReady) {
      localStackAutoPromptedRef.current = false;
      return;
    }
    if (localStackAutoPromptedRef.current) return;
    localStackAutoPromptedRef.current = true;
    setLocalStackConfirmOpen(true);
  }, [activeStep, localRuntimeReady, localStackConfirmOpen, localStackProvisioningActive, managedPreflightReady, selectedStackMode, statusLoading]);

  useEffect(() => {
    if (activeStep !== 3) return;
    void refreshProviderStatus({ quiet: true });
  }, [activeStep, refreshProviderStatus]);

  useEffect(() => {
    if (selectedStackMode !== 'online') {
      setExternalValidationState('idle');
      setExternalValidationMessage('');
      setExternalModels([]);
      return;
    }

    const provider = selectedExternalProvider;
    const credential = providerCredential(config, provider);
    if (!credential) {
      setExternalValidationState('idle');
      setExternalValidationMessage('Add an API key to validate the provider and fetch compatible models.');
      setExternalModels([]);
      return;
    }

    const requestId = providerValidationRequestRef.current + 1;
    providerValidationRequestRef.current = requestId;
    setExternalValidationState('loading');
    setExternalValidationMessage('Checking provider credentials and loading models…');

    const timer = window.setTimeout(async () => {
      try {
        const models = await api.getAIModels(provider, { apiKey: credential });
        if (providerValidationRequestRef.current !== requestId) return;
        setExternalModels(models);
        setExternalValidationState('valid');
        setExternalValidationMessage(models.length > 0 ? 'Provider validated. Choose the model PMDA should use during the scan.' : 'Provider validated, but no compatible model was returned.');
        if (models.length > 0 && !models.includes(String(config.OPENAI_MODEL || '').trim())) {
          updateConfig({ OPENAI_MODEL: models[0] });
        }
      } catch (error) {
        if (providerValidationRequestRef.current !== requestId) return;
        setExternalModels([]);
        setExternalValidationState('error');
        setExternalValidationMessage(error instanceof Error ? error.message : 'Failed to validate this provider');
      }
    }, 450);

    return () => window.clearTimeout(timer);
  }, [config, selectedExternalProvider, selectedStackMode, updateConfig]);

  const providerState = useCallback((
    key: 'discogs' | 'lastfm' | 'acoustid',
    configuredValue: boolean,
    enabled: boolean,
  ): ProviderStatus => {
    if (!enabled) {
      return {
        variant: 'outline',
        label: 'Off',
        message: 'Disabled for this first scan.',
      };
    }
    if (!configuredValue) {
      return {
        variant: 'outline',
        label: 'Needs credentials',
        message: 'Add the required credentials or switch this source off.',
      };
    }
    if (!providersPreflight) {
      return {
        variant: 'secondary',
        label: 'Configured',
        message: 'Ready to verify on the next provider check.',
      };
    }
    const result = providersPreflight[key];
    if (!result) {
      return {
        variant: 'secondary',
        label: 'Configured',
        message: 'No check result available yet.',
      };
    }
    if (result.ok) {
      return {
        variant: 'default',
        label: 'Valid',
        message: result.message || 'Credentials look good.',
      };
    }
    return {
      variant: 'destructive',
      label: 'Issue',
      message: result.message || 'Credential check failed.',
    };
  }, [providersPreflight]);

  const musicbrainzEnabled = config.USE_MUSICBRAINZ !== false;
  const discogsEnabled = config.USE_DISCOGS !== false;
  const lastfmEnabled = config.USE_LASTFM !== false;
  const bandcampEnabled = config.USE_BANDCAMP !== false;
  const acoustidEnabled = config.USE_ACOUSTID !== false;

  const musicbrainzConfigured = Boolean(String(config.MUSICBRAINZ_EMAIL || '').trim());
  const discogsConfigured = Boolean(String(config.DISCOGS_USER_TOKEN || '').trim());
  const lastfmConfigured = Boolean(String(config.LASTFM_API_KEY || '').trim() && String(config.LASTFM_API_SECRET || '').trim());
  const acoustidConfigured = Boolean(String(config.ACOUSTID_API_KEY || '').trim());

  const musicbrainzStatus: ProviderStatus = useMemo(() => {
    if (!musicbrainzEnabled) {
      return {
        variant: 'outline',
        label: 'Off',
        message: 'Disabled for the metadata pipeline.',
      };
    }
    if (!musicbrainzConfigured) {
      return {
        variant: 'outline',
        label: 'Needs email',
        message: 'Add a MusicBrainz contact email before the first scan.',
      };
    }
    const result = providersPreflight?.musicbrainz;
    if (!result) {
      return {
        variant: 'secondary',
        label: 'Configured',
        message: selectedStackMode === 'local'
          ? 'MusicBrainz will use the managed local mirror when it becomes ready.'
          : 'Ready to verify on the next provider check.',
      };
    }
    if (result.ok) {
      return {
        variant: 'default',
        label: 'Valid',
        message: result.message || 'MusicBrainz identity is ready.',
      };
    }
    return {
      variant: 'destructive',
      label: 'Issue',
      message: result.message || 'MusicBrainz check failed.',
    };
  }, [musicbrainzConfigured, musicbrainzEnabled, providersPreflight, selectedStackMode]);

  const discogsStatus = providerState('discogs', discogsConfigured, discogsEnabled);
  const lastfmStatus = providerState('lastfm', lastfmConfigured, lastfmEnabled);
  const acoustidStatus = providerState('acoustid', acoustidConfigured, acoustidEnabled);
  const bandcampStatus: ProviderStatus = bandcampEnabled
    ? {
      variant: 'secondary',
      label: 'Enabled',
      message: 'No key required. Used as a last-resort fallback provider.',
    }
    : {
      variant: 'outline',
      label: 'Off',
      message: 'Bandcamp fallback is disabled for this scan.',
    };

  const sourceIssues = useMemo(() => {
    const issues: string[] = [];
    if (musicbrainzEnabled && !musicbrainzConfigured) issues.push('Add a MusicBrainz contact email or switch MusicBrainz off.');
    if (discogsEnabled && !discogsConfigured) issues.push('Add a Discogs user token or switch Discogs off.');
    if (lastfmEnabled && !lastfmConfigured) issues.push('Add Last.fm API key + secret or switch Last.fm off.');
    if (acoustidEnabled && !acoustidConfigured) issues.push('Add an AcoustID API key or switch AcoustID off.');

    if (musicbrainzEnabled && musicbrainzConfigured && providersPreflight?.musicbrainz && !providersPreflight.musicbrainz.ok) {
      issues.push(providersPreflight.musicbrainz.message || 'Fix the MusicBrainz identity check.');
    }
    if (discogsEnabled && discogsConfigured && providersPreflight?.discogs && !providersPreflight.discogs.ok) {
      issues.push(providersPreflight.discogs.message || 'Fix the Discogs credentials.');
    }
    if (lastfmEnabled && lastfmConfigured && providersPreflight?.lastfm && !providersPreflight.lastfm.ok) {
      issues.push(providersPreflight.lastfm.message || 'Fix the Last.fm credentials.');
    }
    if (acoustidEnabled && acoustidConfigured && providersPreflight?.acoustid && !providersPreflight.acoustid.ok) {
      issues.push(providersPreflight.acoustid.message || 'Fix the AcoustID credentials.');
    }

    return issues;
  }, [acoustidConfigured, acoustidEnabled, discogsConfigured, discogsEnabled, lastfmConfigured, lastfmEnabled, musicbrainzConfigured, musicbrainzEnabled, providersPreflight]);

  const sourcesConfigured = sourceIssues.length === 0;
  const externalModel = String(config.OPENAI_MODEL || '').trim();
  const externalAiReady = selectedStackMode === 'online'
    && externalValidationState === 'valid'
    && externalModels.length > 0
    && externalModels.includes(externalModel);

  const metadataSetupReady = selectedStackMode === 'local'
    ? localRuntimeReady && sourcesConfigured
    : externalAiReady && sourcesConfigured;

  const localStackDetectedCount = (managedMbCandidate ? 1 : 0) + (managedOllamaCandidate ? 1 : 0);
  const localStackActionLabel = localStackDetectedCount > 0
    ? (localStackDetectedCount === 2 ? 'Use detected local services' : 'Use detected service and create the rest')
    : 'Create local stack';
  const localStackActionDescription = localStackDetectedCount === 2
    ? 'PMDA found an existing MusicBrainz mirror and an existing Ollama runtime on this server. It can adopt them directly after your confirmation.'
    : localStackDetectedCount === 1
      ? 'PMDA found one existing local service on this server. It can adopt it and create only the missing part after your confirmation.'
      : 'No local metadata service was detected yet. PMDA will create the MusicBrainz mirror and the Ollama runtime after your confirmation.';
  const localStackNeedsConfirmation = selectedStackMode === 'local'
    && !localRuntimeReady
    && managedPreflightReady
    && !localStackProvisioningActive;

  const localStackChecklist = useMemo(() => {
    if (selectedStackMode !== 'local') return [];
    const musicbrainzIdleMessage = managedMbCandidate
      ? `Existing MusicBrainz mirror detected${managedMbCandidate?.published_url ? ` at ${managedMbCandidate.published_url}` : ''}. It will be adopted when you confirm local setup.`
      : 'Not started yet. PMDA will provision MusicBrainz after you confirm local setup.';
    const ollamaIdleMessage = managedOllamaCandidate
      ? `Existing Ollama runtime detected${managedOllamaCandidate?.url ? ` at ${managedOllamaCandidate.url}` : ''}. It will be adopted when you confirm local setup and PMDA will ensure ${ollamaModel} and ${ollamaHardModel}.`
      : `Not started yet. PMDA will provision Ollama and install ${ollamaModel} and ${ollamaHardModel} after you confirm local setup.`;
    const musicbrainzState = effectiveBundleState(managedMbBundle);
    const ollamaState = effectiveBundleState(managedOllamaBundle);
    const musicbrainzNotStarted = String(managedMbBundle?.mode || 'absent').trim().toLowerCase() === 'absent' || musicbrainzState === 'idle';
    const ollamaNotStarted = String(managedOllamaBundle?.mode || 'absent').trim().toLowerCase() === 'absent' || ollamaState === 'idle';
    const musicbrainzDetail = musicbrainzReady
      ? 'Local MusicBrainz mirror is ready.'
      : musicbrainzNotStarted
        ? musicbrainzIdleMessage
        : effectiveBundleMessage(managedMbBundle, musicbrainzIdleMessage);
    const ollamaDetail = ollamaReady
      ? `Ollama is ready with ${ollamaModel} and ${ollamaHardModel}.`
      : ollamaNotStarted
        ? ollamaIdleMessage
        : effectiveBundleMessage(managedOllamaBundle, ollamaIdleMessage);

    return [
      {
        key: 'musicbrainz',
        label: 'MusicBrainz mirror',
        done: musicbrainzReady,
        detail: musicbrainzDetail,
        progress: musicbrainzNotStarted ? 0 : managedBundleProgress(managedMbBundle),
        displayProgress: musicbrainzNotStarted ? 0 : managedBundleDisplayProgress(managedMbBundle),
        eta: musicbrainzNotStarted || musicbrainzState === 'failed' ? '' : managedBundleEta(managedMbBundle),
        phase: effectiveBundlePhase(managedMbBundle),
      },
      {
        key: 'ollama',
        label: 'Ollama + required models',
        done: ollamaReady,
        detail: ollamaDetail,
        progress: ollamaNotStarted ? 0 : managedBundleProgress(managedOllamaBundle, { requiredModels: [ollamaModel, ollamaHardModel] }),
        displayProgress: ollamaNotStarted ? 0 : managedBundleDisplayProgress(managedOllamaBundle, { requiredModels: [ollamaModel, ollamaHardModel] }),
        eta: ollamaNotStarted || ollamaState === 'failed' ? '' : managedBundleEta(managedOllamaBundle),
        phase: effectiveBundlePhase(managedOllamaBundle),
      },
    ];
  }, [managedMbBundle, managedMbCandidate, managedOllamaBundle, managedOllamaCandidate, musicbrainzReady, ollamaHardModel, ollamaModel, ollamaReady, selectedStackMode]);

  const localStackProgress = useMemo(() => {
    if (selectedStackMode !== 'local' || localStackChecklist.length === 0) return 0;
    const total = localStackChecklist.reduce((sum, item) => sum + item.progress, 0);
    return Math.round(total / localStackChecklist.length);
  }, [localStackChecklist, selectedStackMode]);

  const musicbrainzDisplayProgress = useMemo(
    () => managedBundleDisplayProgress(managedMbBundle),
    [managedMbBundle],
  );

  const musicbrainzDisplayEta = useMemo(
    () => managedBundleEta(managedMbBundle),
    [managedMbBundle],
  );

  const showMusicbrainzDownloadProgress = selectedStackMode === 'local'
    && effectiveBundlePhase(managedMbBundle) === 'importing'
    && !musicbrainzReady
    && musicbrainzDisplayProgress > 0;

  const localStackServices = useMemo(() => {
    if (selectedStackMode !== 'local' || !showRuntimeDetails) return [];
    const musicbrainzServices = (managedMbBundle?.services || []).map((service) => ({
      bundleLabel: 'MusicBrainz mirror',
      name: service.name,
      status: service.status,
      message: service.message,
    }));
    const ollamaServices = (managedOllamaBundle?.services || []).map((service) => ({
      bundleLabel: 'Ollama',
      name: service.name,
      status: service.status,
      message: service.message,
    }));
    return [...musicbrainzServices, ...ollamaServices];
  }, [managedMbBundle?.services, managedOllamaBundle?.services, selectedStackMode, showRuntimeDetails]);

  const localStackRecentLogs = useMemo(() => {
    if (selectedStackMode !== 'local' || !showRuntimeDetails) return [];
    return managedRuntimeLogs
      .filter((entry) => entry.bundle_type === 'musicbrainz_local' || entry.bundle_type === 'ollama_local')
      .slice(0, 6);
  }, [managedRuntimeLogs, selectedStackMode, showRuntimeDetails]);

  const firstScanStarted = Boolean(
    scanProgress && (
      scanProgress.scanning
      || scanProgress.scan_starting
      || scanProgress.resume_available
      || scanProgress.scan_resume_run_id
      || scanProgress.scan_start_time != null
      || (scanProgress.artists_total ?? 0) > 0
      || (scanProgress.detected_albums_total ?? 0) > 0
      || (scanProgress.scan_run_scope_total ?? 0) > 0
      || scanProgress.last_scan_summary
    ),
  );

  const playerSyncReady = useMemo(() => {
    if (playerTarget === 'none') return true;
    if (playerTarget === 'plex') {
      return Boolean(String(config.PLEX_HOST || '').trim() && String(config.PLEX_TOKEN || '').trim());
    }
    if (playerTarget === 'jellyfin') {
      return Boolean(String(config.JELLYFIN_URL || '').trim() && String(config.JELLYFIN_API_KEY || '').trim());
    }
    return Boolean(
      String(config.NAVIDROME_URL || '').trim()
      && (
        String(config.NAVIDROME_API_KEY || '').trim()
        || (
          String(config.NAVIDROME_USERNAME || '').trim()
          && String(config.NAVIDROME_PASSWORD || '').trim()
        )
      ),
    );
  }, [config.JELLYFIN_API_KEY, config.JELLYFIN_URL, config.NAVIDROME_API_KEY, config.NAVIDROME_PASSWORD, config.NAVIDROME_URL, config.NAVIDROME_USERNAME, config.PLEX_HOST, config.PLEX_TOKEN, playerTarget]);

  const pipelineConfigReady = useMemo(() => {
    if (wantsPublishedLibrary && publishLibraryEnabled && !materializationMode) return false;
    if (!playerSyncReady) return false;
    return true;
  }, [materializationMode, playerSyncReady, publishLibraryEnabled, wantsPublishedLibrary]);

  const launchReady = foldersReady && metadataSetupReady && pipelineConfigReady && pipelineStepConfirmed;

  const reviewFingerprint = JSON.stringify({
    workflowMode,
    selectedStackMode,
    selectedExternalProvider,
    externalModel,
    sourcesConfigured,
    musicbrainzEnabled,
    discogsEnabled,
    lastfmEnabled,
    bandcampEnabled,
    acoustidEnabled,
    publishLibraryEnabled,
    materializationMode,
    dedupeMode,
    incompleteMode,
    playerTarget,
    playerSyncReady,
    localRuntimeReady,
    externalAiReady,
  });

  useEffect(() => {
    if (!reviewFingerprintRef.current) {
      reviewFingerprintRef.current = reviewFingerprint;
      return;
    }
    if (reviewFingerprintRef.current !== reviewFingerprint) {
      reviewFingerprintRef.current = reviewFingerprint;
      setPipelineStepConfirmed(false);
    }
  }, [reviewFingerprint]);

  const blockers = useMemo(() => {
    const next: Blocker[] = [];

    if (!foldersReady) {
      next.push({
        key: 'folders',
        title: 'Required folders are missing',
        detail: missingFolderItems[0] || 'Choose the mandatory folders for this workflow.',
        sectionId: 'settings-files-export',
        stepId: 'folders',
      });
    }

    if (!sourcesConfigured) {
      next.push({
        key: 'sources',
        title: 'Metadata sources still need configuration',
        detail: sourceIssues[0] || 'Add the missing provider credentials or switch the source off.',
        sectionId: 'settings-providers-advanced',
        stepId: 'setup',
      });
    }

    if (selectedStackMode === 'local' && !localRuntimeReady) {
      next.push({
        key: 'local-runtime',
        title: localStackProvisioningActive ? 'Local metadata stack is still preparing' : 'Local metadata stack is not ready yet',
        detail: !managedPreflightReady
          ? (managedStatus?.preflight.message || 'Docker socket or runtime tooling is not ready.')
          : localStackNeedsConfirmation
            ? 'PMDA is waiting for your confirmation to create or adopt the local MusicBrainz + Ollama services.'
            : !musicbrainzReady
              ? (managedMbBundle?.phase_message || 'Managed MusicBrainz is not ready yet.')
              : (managedOllamaBundle?.phase_message || 'Managed Ollama or the required models are not ready yet.'),
        sectionId: 'settings-scaling',
        stepId: 'setup',
      });
    }

    if (selectedStackMode === 'online' && !externalAiReady) {
      next.push({
        key: 'external-ai',
        title: 'External AI provider is not ready',
        detail: externalValidationMessage || 'Validate the external provider and choose a compatible model.',
        sectionId: 'settings-ai',
        stepId: 'setup',
      });
    }

    if (!pipelineConfigReady) {
      next.push({
        key: 'pipeline',
        title: 'Pipeline choices are incomplete',
        detail: !playerSyncReady
          ? 'Complete the selected player sync credentials or switch player sync off.'
          : 'Finish the required pipeline choices before launching the first scan.',
        sectionId: 'settings-outputs',
        stepId: 'pipeline',
      });
    }

    if (!pipelineStepConfirmed) {
      next.push({
        key: 'pipeline-review',
        title: 'Pipeline step has not been reviewed yet',
        detail: 'Open the pipeline step and confirm how PMDA should publish, move duplicates, and handle incompletes.',
        stepId: 'pipeline',
      });
    }

    return next;
  }, [externalAiReady, externalValidationMessage, foldersReady, localRuntimeReady, localStackNeedsConfirmation, localStackProvisioningActive, managedMbBundle?.phase_message, managedOllamaBundle?.phase_message, managedPreflightReady, managedStatus?.preflight.message, metadataSetupReady, missingFolderItems, musicbrainzReady, pipelineConfigReady, pipelineStepConfirmed, playerSyncReady, selectedStackMode, sourceIssues, sourcesConfigured]);

  const completionCount = useMemo(() => {
    let count = 0;
    if (workflowMode) count += 1;
    if (foldersReady) count += 1;
    if (selectedStackMode === 'local' || selectedStackMode === 'online') count += 1;
    if (metadataSetupReady) count += 1;
    if (pipelineConfigReady && pipelineStepConfirmed) count += 1;
    if (launchReady) count += 1;
    return count;
  }, [foldersReady, launchReady, metadataSetupReady, pipelineConfigReady, pipelineStepConfirmed, selectedStackMode, workflowMode]);

  const wizardPercent = Math.round(((activeStep + 1) / STEP_ORDER.length) * 100);

  const scanProgressPercent = Number.isFinite(Number(scanProgress?.stage_progress_percent))
    ? Number(scanProgress?.stage_progress_percent)
    : Number.isFinite(Number(scanProgress?.overall_progress_percent))
      ? Number(scanProgress?.overall_progress_percent)
      : Number.isFinite(Number(scanProgress?.progress)) && Number(scanProgress?.total)
        ? (Number(scanProgress?.progress) / Math.max(1, Number(scanProgress?.total))) * 100
        : 0;

  const metadataLabel = selectedStackMode === 'local' ? 'Local stack' : 'External AI';
  const readinessLabel = launchReady ? 'Ready to scan' : blockers[0]?.title || 'Setup still needs attention';
  const firstScanLabel = scanProgress?.scanning
    ? 'Scan running'
    : scanProgress?.resume_available
      ? 'Resume available'
      : firstScanStarted || !Boolean(bootstrap?.bootstrap_required)
        ? 'Already started'
        : 'Not started';

  const goToStep = useCallback(async (nextStep: number) => {
    const bounded = Math.max(0, Math.min(STEP_ORDER.length - 1, nextStep));
    if (bounded > activeStep) {
      const persisted = await persistIfNeeded();
      if (!persisted) return;
    }
    setActiveStep(bounded);
    requestAnimationFrame(() => {
      const container = document.querySelector<HTMLElement>('[data-guided-onboarding-scroll="true"]');
      if (container) container.scrollTo({ top: 0, behavior: 'smooth' });
    });
  }, [activeStep, persistIfNeeded]);

  const bootstrapLocalStack = useCallback(async (): Promise<boolean> => {
    setRuntimeActionBusy(true);
    try {
      const persisted = await persistIfNeeded();
      if (!persisted) return false;

      updateConfig({
        MANAGED_RUNTIME_CONFIG_ROOT: resolvedManagedConfigRoot,
        MANAGED_RUNTIME_DATA_ROOT: resolvedManagedDataRoot,
        MUSICBRAINZ_MIRROR_NAME: String(config.MUSICBRAINZ_MIRROR_NAME || 'Managed local MusicBrainz').trim() || 'Managed local MusicBrainz',
      });

      const result = await api.bootstrapManagedRuntime({
        config_root: resolvedManagedConfigRoot,
        data_root: resolvedManagedDataRoot,
        bundles: {
          musicbrainz_local: {
            action: 'auto',
            mirror_name: String(config.MUSICBRAINZ_MIRROR_NAME || 'Managed local MusicBrainz').trim() || 'Managed local MusicBrainz',
          },
          ollama_local: {
            action: 'auto',
            fast_model: ollamaModel,
            hard_model: ollamaHardModel,
          },
        },
      });
      setManagedStatus(result.snapshot);
      toast.success('Local metadata stack creation started');
      void refreshStatus();
      return true;
    } catch (error) {
      const fallbackStatus = await api.getManagedRuntimeStatus({ skipCandidates: true }).catch(() => null);
      if (fallbackStatus) {
        setManagedStatus(fallbackStatus);
        const musicbrainzState = String(fallbackStatus.bundles?.musicbrainz_local?.state || '').trim().toLowerCase();
        const ollamaState = String(fallbackStatus.bundles?.ollama_local?.state || '').trim().toLowerCase();
        if (
          ['preflight', 'creating', 'pulling', 'starting', 'waiting_health'].includes(musicbrainzState)
          || ['preflight', 'creating', 'pulling', 'starting', 'waiting_health', 'ready'].includes(ollamaState)
        ) {
          toast('Local stack bootstrap is running in the background. Staying on the wizard and following live progress.');
          return true;
        }
      }
      toast.error(error instanceof Error ? error.message : 'Failed to create the local metadata stack');
      return false;
    } finally {
      setRuntimeActionBusy(false);
    }
  }, [config.MUSICBRAINZ_MIRROR_NAME, ollamaHardModel, ollamaModel, persistIfNeeded, refreshStatus, resolvedManagedConfigRoot, resolvedManagedDataRoot, updateConfig]);

  const confirmAndStartLocalStack = useCallback(async () => {
    const started = await bootstrapLocalStack();
    if (!started) return;
    setLocalStackConfirmOpen(false);
    await goToStep(3);
  }, [bootstrapLocalStack, goToStep]);

  const handleNextStep = useCallback(async () => {
    if (activeStep >= STEP_ORDER.length - 1) return;

    if (activeStep === 2 && selectedStackMode === 'local' && localStackNeedsConfirmation) {
      setLocalStackConfirmOpen(true);
      return;
    }

    if (activeStep === 3 && !metadataSetupReady) {
      toast.error(blockers.find((blocker) => blocker.stepId === 'setup')?.detail || 'Finish the metadata setup first.');
      return;
    }

    if (activeStep === 4) {
      if (!pipelineConfigReady) {
        toast.error(blockers.find((blocker) => blocker.stepId === 'pipeline')?.detail || 'Finish the pipeline choices first.');
        return;
      }
      setPipelineStepConfirmed(true);
    }

    await goToStep(activeStep + 1);
  }, [activeStep, blockers, goToStep, localStackNeedsConfirmation, metadataSetupReady, pipelineConfigReady, selectedStackMode]);

  const startOrResumeScan = useCallback(async () => {
    setScanActionBusy(true);
    try {
      const persisted = await persistIfNeeded();
      if (!persisted) return;

      if (scanProgress?.scanning || (firstScanStarted && !Boolean(bootstrap?.bootstrap_required) && !scanProgress?.resume_available)) {
        navigate('/scan');
      } else if (scanProgress?.resume_available && !scanProgress?.scanning) {
        await api.resumeScan();
        toast.success('Scan resumed');
        navigate('/scan');
      } else {
        await api.startScan({ scan_type: 'full', run_improve_after: true });
        toast.success('First full scan started');
        navigate('/scan');
      }

      if (isModalPresentation && onClose) onClose();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to start scan');
    } finally {
      setScanActionBusy(false);
    }
  }, [bootstrap?.bootstrap_required, firstScanStarted, isModalPresentation, navigate, onClose, persistIfNeeded, scanProgress?.resume_available, scanProgress?.scanning]);

  const openAdvancedSection = useCallback((sectionId: string) => {
    if (isModalPresentation && onClose) onClose();
    if (window.location.pathname === '/settings') {
      requestAnimationFrame(() => {
        scrollToSection(sectionId);
        window.history.replaceState(null, '', `#${sectionId}`);
      });
      return;
    }
    navigate(`/settings#${sectionId}`);
  }, [isModalPresentation, navigate, onClose]);

  const setDedupeMode = useCallback((mode: DedupeMode) => {
    if (mode === 'ignore') {
      updateConfig({ PIPELINE_ENABLE_DEDUPE: false, AUTO_MOVE_DUPES: false });
      return;
    }
    if (mode === 'detect') {
      updateConfig({ PIPELINE_ENABLE_DEDUPE: true, AUTO_MOVE_DUPES: false });
      return;
    }
    updateConfig({ PIPELINE_ENABLE_DEDUPE: true, AUTO_MOVE_DUPES: true });
  }, [updateConfig]);

  const setIncompleteHandling = useCallback((mode: IncompleteMode) => {
    updateConfig({ PIPELINE_ENABLE_INCOMPLETE_MOVE: mode === 'move' });
  }, [updateConfig]);

  const setPlayerTarget = useCallback((target: NonNullable<PMDAConfig['PIPELINE_PLAYER_TARGET']>) => {
    updateConfig({
      PIPELINE_PLAYER_TARGET: target,
      PIPELINE_ENABLE_PLAYER_SYNC: target !== 'none',
    });
  }, [updateConfig]);

  const sourceSummary = [
    musicbrainzEnabled ? 'MusicBrainz' : null,
    discogsEnabled ? 'Discogs' : null,
    lastfmEnabled ? 'Last.fm' : null,
    bandcampEnabled ? 'Bandcamp' : null,
    acoustidEnabled ? 'AcoustID' : null,
  ].filter(Boolean).join(', ');

  const reviewRows = [
    {
      label: 'Workflow',
      value: workflowMeta.label,
      detail: workflowMeta.whenToUse,
      step: 0,
    },
    {
      label: 'Metadata',
      value: selectedStackMode === 'local' ? 'Local stack' : EXTERNAL_AI_OPTIONS.find((option) => option.value === selectedExternalProvider)?.label || 'External AI',
      detail: selectedStackMode === 'local'
        ? (localRuntimeReady ? 'MusicBrainz mirror + Ollama ready.' : 'Local stack still needs to finish.')
        : (externalValidationState === 'valid' ? `${selectedExternalProvider} validated with ${externalModel}.` : externalValidationMessage || 'Provider still needs validation.'),
      step: 3,
    },
    {
      label: 'Sources',
      value: sourceSummary || 'No source enabled',
      detail: sourcesConfigured ? 'All enabled sources are configured.' : sourceIssues[0] || 'A source still needs configuration.',
      step: 3,
    },
    {
      label: 'Publish library',
      value: wantsPublishedLibrary ? (publishLibraryEnabled ? `On · ${materializationMode}` : 'Off') : 'Not needed',
      detail: wantsPublishedLibrary ? `Serving root: ${servingRoot || 'not set yet'}` : 'PMDA works directly inside the current library.',
      step: 4,
    },
    {
      label: 'Duplicates',
      value: dedupeMode === 'ignore' ? 'Ignore' : dedupeMode === 'detect' ? 'Detect only' : 'Detect and move',
      detail: dupesRoot ? `Dupes folder: ${dupesRoot}` : 'No duplicates folder set yet.',
      step: 4,
    },
    {
      label: 'Incompletes',
      value: incompleteMode === 'move' ? 'Move to incomplete folder' : 'Leave in place',
      detail: incompleteRoot ? `Incomplete folder: ${incompleteRoot}` : 'No incomplete folder set yet.',
      step: 4,
    },
    {
      label: 'Player sync',
      value: playerTarget === 'none' ? 'Not now' : PLAYER_TARGET_OPTIONS.find((option) => option.value === playerTarget)?.label || playerTarget,
      detail: playerTarget === 'none' ? 'No external player refresh after the first scan.' : (playerSyncReady ? 'Credentials are present.' : 'Credentials still need to be completed.'),
      step: 4,
    },
  ];

  if (!isModalPresentation) {
    return (
      <Card id="settings-onboarding" className="scroll-mt-24 border-primary/25 bg-primary/[0.04]">
        <CardHeader>
          <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
            <div className="space-y-1.5">
              <CardTitle className="flex items-center gap-2">
                <span className="inline-flex h-8 w-8 items-center justify-center rounded-md bg-primary/20 text-primary">
                  <Sparkles className="h-4 w-4" />
                </span>
                Guided setup
              </CardTitle>
              <CardDescription>
                Use the short setup flow to lock workflow, folders, metadata, pipeline behavior and the first scan review in one place.
              </CardDescription>
            </div>
            <Button type="button" className="gap-2 self-start" onClick={onOpenGuidedSetup}>
              <Sparkles className="h-4 w-4" />
              Open guided setup
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 md:grid-cols-4">
            <div className="rounded-2xl border border-border/70 bg-background/60 p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Workflow</div>
              <div className="mt-2 text-sm font-semibold text-foreground">{workflowMeta.label}</div>
              <p className="mt-1 text-xs text-muted-foreground">{workflowMeta.description}</p>
            </div>
            <div className="rounded-2xl border border-border/70 bg-background/60 p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Metadata</div>
              <div className="mt-2 text-sm font-semibold text-foreground">{metadataLabel}</div>
              <p className="mt-1 text-xs text-muted-foreground">
                {selectedStackMode === 'local' ? 'Local-first path, no recurring paid AI by default.' : 'External AI provider selected for metadata escalation.'}
              </p>
            </div>
            <div className="rounded-2xl border border-border/70 bg-background/60 p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Readiness</div>
              <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
                {statusLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : launchReady ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <AlertTriangle className="h-4 w-4 text-amber-500" />}
                {readinessLabel}
              </div>
              <p className="mt-1 text-xs text-muted-foreground">{blockers[0]?.detail || 'No blocking issue detected.'}</p>
            </div>
            <div className="rounded-2xl border border-border/70 bg-background/60 p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">First scan</div>
              <div className="mt-2 text-sm font-semibold text-foreground">{firstScanLabel}</div>
              <p className="mt-1 text-xs text-muted-foreground">
                {scanProgress?.scanning ? `Current phase: ${scanProgress.phase || 'running'}` : configured ? 'PMDA already has a configured library state.' : 'Use the guided setup to reach a first valid scan.'}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-5">
      <div className="rounded-[28px] border border-white/10 bg-[linear-gradient(160deg,rgba(15,23,42,0.98),rgba(9,14,31,0.96))] p-5 text-white shadow-[0_24px_80px_rgba(2,6,23,0.42)] md:p-6">
        <div className="flex flex-col gap-5">
          <div className="space-y-2">
            <div className="inline-flex items-center gap-2 rounded-full border border-primary/25 bg-primary/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-primary/90">
              Guided setup
            </div>
            <h2 className="text-2xl font-semibold tracking-tight">Set the essentials, then launch the first scan.</h2>
            <p className="max-w-2xl text-sm text-slate-300">
              Six steps only: choose the workflow, point PMDA at the right folders, pick local or external metadata, configure the required sources, decide the pipeline behavior, then review exactly what the first scan will do.
            </p>
          </div>
          <div className="space-y-3">
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>{STEP_ORDER[activeStep]?.label}</span>
              <span>{launchReady ? 'Ready to scan' : `${completionCount}/${STEP_ORDER.length} steps ready`}</span>
            </div>
            <Progress value={wizardPercent} className="h-2 bg-white/10" />
            <div className="grid gap-2 sm:grid-cols-3 xl:grid-cols-6">
              {STEP_ORDER.map((step, index) => {
                const complete = (
                  (step.id === 'workflow' && Boolean(workflowMode))
                  || (step.id === 'folders' && foldersReady)
                  || (step.id === 'metadata' && Boolean(selectedStackMode))
                  || (step.id === 'setup' && metadataSetupReady)
                  || (step.id === 'pipeline' && pipelineConfigReady && pipelineStepConfirmed)
                  || (step.id === 'review' && launchReady)
                );
                const active = index === activeStep;
                return (
                  <div
                    key={step.id}
                    className={`rounded-2xl border px-3 py-3 ${active ? 'border-primary/45 bg-primary/12' : complete ? 'border-emerald-500/25 bg-emerald-500/10' : 'border-white/10 bg-white/[0.03]'}`}
                  >
                    <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">{step.label}</div>
                    <div className="mt-1 text-sm font-semibold text-white">{step.title}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      <Card className="border-white/10 bg-slate-950/72 text-white shadow-[0_24px_80px_rgba(2,6,23,0.34)]">
        <CardContent data-guided-onboarding-scroll="true" className="space-y-6 p-5 md:p-6">
          {activeStep === 0 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">How should PMDA work with your music folders?</div>
                <p className="text-sm text-slate-400">Pick the situation that matches your current setup. PMDA will only ask for the folders required by that workflow.</p>
              </div>
              <div className="grid gap-3 md:grid-cols-3">
                {WORKFLOW_OPTIONS.map((option) => {
                  const selected = workflowMode === option.value;
                  return (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => applyWorkflowPreset(option.value)}
                      className={`rounded-3xl border p-4 text-left transition ${selected ? 'border-primary/50 bg-primary/12 shadow-[0_0_0_1px_rgba(59,130,246,0.18)]' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-white/5 text-primary">
                          <Workflow className="h-5 w-5" />
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge variant="outline" className="border-white/15 bg-white/5 text-slate-200">{option.modeName}</Badge>
                          {selected ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                        </div>
                      </div>
                      <div className="mt-4 text-lg font-semibold text-white">{option.label}</div>
                      <p className="mt-2 text-sm leading-6 text-slate-300">{option.description}</p>
                      <p className="mt-3 text-xs leading-5 text-slate-400">{option.whenToUse}</p>
                      <p className="mt-3 text-xs font-medium text-slate-200">{option.asksFor}</p>
                      <div className="mt-4">
                        <WorkflowDiagram nodes={option.diagram} />
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>
          ) : null}

          {activeStep === 1 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">Set the required folders</div>
                <p className="text-sm text-slate-400">Only existing folders are shown here. Music folders stay inside <span className="font-mono text-slate-200">/music</span> so the wizard cannot invent the wrong path.</p>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                {workflowMode === 'managed' ? (
                  <PathListEditor
                    label="Intake folders"
                    description="New albums arrive here before PMDA validates and publishes them."
                    paths={intakeRoots}
                    onChange={(paths) => updateConfig({ LIBRARY_INTAKE_ROOTS: stringifyPathList(paths), FILES_ROOTS: stringifyPathList(paths) })}
                    placeholder="/music"
                    selectLabel="Select intake folder"
                    browseRoot="/music"
                    lockToBrowseRoot
                  />
                ) : null}

                {(workflowMode === 'mirror' || workflowMode === 'inplace') ? (
                  <PathListEditor
                    label={workflowMode === 'mirror' ? 'Source library folders' : 'Library folders'}
                    description={workflowMode === 'mirror' ? 'PMDA reads these folders and builds a clean serving copy elsewhere.' : 'PMDA validates and serves directly from these folders.'}
                    paths={sourceRoots}
                    onChange={(paths) => updateConfig({ LIBRARY_SOURCE_ROOTS: stringifyPathList(paths), FILES_ROOTS: stringifyPathList(paths) })}
                    placeholder="/music"
                    selectLabel={workflowMode === 'mirror' ? 'Select source library folder' : 'Select library folder'}
                    browseRoot="/music"
                    lockToBrowseRoot
                  />
                ) : null}

                {(workflowMode === 'managed' || workflowMode === 'mirror') ? (
                  <SingleFolderField
                    label="Serving library"
                    description="This is the clean library PMDA will expose to Plex, Navidrome or Jellyfin."
                    value={servingRoot}
                    onChange={(value) => updateConfig({ LIBRARY_SERVING_ROOT: normalizeFolderPath(value), EXPORT_ROOT: normalizeFolderPath(value) })}
                    placeholder="/music"
                    selectLabel="Select serving library folder"
                    browseRoot="/music"
                    lockToBrowseRoot
                  />
                ) : null}

                <SingleFolderField
                  label="Duplicates"
                  description="Duplicate losers go here for later review."
                  value={dupesRoot}
                  onChange={(value) => updateConfig({ LIBRARY_DUPES_ROOT: normalizeFolderPath(value), DUPE_ROOT: normalizeFolderPath(value) })}
                  placeholder="/dupes"
                  selectLabel="Select duplicates folder"
                />

                <SingleFolderField
                  label="Incomplete albums"
                  description="Albums flagged as incomplete are quarantined here."
                  value={incompleteRoot}
                  onChange={(value) => updateConfig({ LIBRARY_INCOMPLETE_ROOT: normalizeFolderPath(value), INCOMPLETE_ALBUMS_TARGET_DIR: normalizeFolderPath(value) })}
                  placeholder="/dupes/incomplete_albums"
                  selectLabel="Select incomplete albums folder"
                />
              </div>

              {missingFolderItems.length > 0 ? (
                <div className="rounded-2xl border border-amber-500/25 bg-amber-500/10 p-4 text-sm text-amber-100">
                  <div className="flex items-center gap-2 font-medium">
                    <AlertTriangle className="h-4 w-4" />
                    Still missing
                  </div>
                  <ul className="mt-2 space-y-1 text-xs text-amber-50/90">
                    {missingFolderItems.map((item) => (
                      <li key={item}>• {item}</li>
                    ))}
                  </ul>
                </div>
              ) : (
                <div className="rounded-2xl border border-emerald-500/25 bg-emerald-500/10 p-4 text-sm text-emerald-100">
                  <div className="flex items-center gap-2 font-medium">
                    <CheckCircle2 className="h-4 w-4" />
                    Required folders are set
                  </div>
                </div>
              )}
            </div>
          ) : null}

          {activeStep === 2 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">Choose the metadata mode</div>
                <p className="text-sm text-slate-400">The local stack is the default path. It avoids recurring paid AI costs and keeps metadata work stable on your server.</p>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <button
                  type="button"
                  onClick={() => applyStackPreset('local')}
                  className={`rounded-3xl border p-5 text-left transition ${selectedStackMode === 'local' ? 'border-primary/50 bg-primary/12 shadow-[0_0_0_1px_rgba(59,130,246,0.18)]' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-white/5 text-primary">
                      <Server className="h-5 w-5" />
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="border-emerald-500/35 bg-emerald-500/10 text-emerald-200">Recommended</Badge>
                      {selectedStackMode === 'local' ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                    </div>
                  </div>
                  <div className="mt-4 text-lg font-semibold text-white">Local stack</div>
                  <p className="mt-2 text-sm leading-6 text-slate-300">
                    Uses a local MusicBrainz mirror and a local Ollama runtime on this server. Best when you want stable throughput and no recurring API bill for the core setup.
                  </p>
                </button>
                <button
                  type="button"
                  onClick={() => applyStackPreset('online')}
                  className={`rounded-3xl border p-5 text-left transition ${selectedStackMode === 'online' ? 'border-primary/50 bg-primary/12 shadow-[0_0_0_1px_rgba(59,130,246,0.18)]' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-white/5 text-primary">
                      <Globe className="h-5 w-5" />
                    </div>
                    {selectedStackMode === 'online' ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                  </div>
                  <div className="mt-4 text-lg font-semibold text-white">External AI</div>
                  <p className="mt-2 text-sm leading-6 text-slate-300">
                    Uses public metadata services and a paid external AI provider. Faster to bootstrap if you already have API keys, but more expensive over time.
                  </p>
                </button>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 text-sm text-slate-300">
                {selectedStackMode === 'local'
                  ? 'Local-first is active. When you continue, PMDA will confirm local runtime creation or adoption before moving on.'
                  : 'External AI is active. Next, configure the provider, validate the key, then choose the model PMDA should use.'}
              </div>
            </div>
          ) : null}

          {activeStep === 3 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">Configure the metadata setup</div>
                <p className="text-sm text-slate-400">Keep only the sources you really want for the first scan. If a source is on, it must be configured here.</p>
              </div>

              {selectedStackMode === 'local' ? (
                <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                    <div className="space-y-1">
                      <div className="text-sm font-semibold text-white">Local stack on this server</div>
                      <p className="text-xs text-slate-400">
                        PMDA will use <span className="font-mono text-slate-200">{resolvedManagedConfigRoot}</span> for runtime config and <span className="font-mono text-slate-200">{resolvedManagedDataRoot}</span> for runtime data.
                      </p>
                      <p className="text-xs text-slate-400">{localStackActionDescription}</p>
                      <div className="flex flex-wrap items-center gap-2 pt-1">
                        <Badge variant={localRuntimeReady ? 'default' : localStackProvisioningActive ? 'secondary' : managedPreflightReady ? 'outline' : 'destructive'}>
                          {localRuntimeReady ? 'Local stack ready' : localStackProvisioningActive ? 'Provisioning in progress' : managedPreflightReady ? 'Waiting for confirmation' : 'Docker not ready'}
                        </Badge>
                        {managedPreflightReady ? <Badge variant="outline">Docker reachable</Badge> : <Badge variant="destructive">Docker not ready</Badge>}
                      </div>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      {!localRuntimeReady && localStackNeedsConfirmation ? (
                        <Button type="button" size="sm" onClick={() => setLocalStackConfirmOpen(true)} disabled={runtimeActionBusy}>
                          {runtimeActionBusy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                          Confirm local setup
                        </Button>
                      ) : null}
                      <Button type="button" variant="outline" size="sm" onClick={() => void refreshStatus()} disabled={statusLoading}>
                        {statusLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                        Refresh status
                      </Button>
                      <Button type="button" variant="outline" size="sm" onClick={() => setShowRuntimeDetails((current) => !current)}>
                        {showRuntimeDetails ? 'Hide runtime details' : 'View runtime details'}
                        <ChevronDown className={`ml-2 h-4 w-4 transition ${showRuntimeDetails ? 'rotate-180' : ''}`} />
                      </Button>
                      <Button type="button" variant="outline" size="sm" onClick={() => openAdvancedSection('settings-scaling')}>
                        Open advanced settings
                      </Button>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-xs text-slate-400">
                      <span>
                        {localRuntimeReady
                          ? 'All local services are ready'
                          : showMusicbrainzDownloadProgress
                            ? 'MusicBrainz dump download'
                            : localStackProvisioningActive
                              ? 'Local stack creation progress'
                              : managedPreflightReady
                                ? 'Waiting for local setup confirmation'
                                : 'Docker is not ready yet'}
                      </span>
                      <span>{localRuntimeReady ? '100%' : `${showMusicbrainzDownloadProgress ? musicbrainzDisplayProgress : localStackProgress}%`}</span>
                    </div>
                    <Progress
                      value={Math.max(0, Math.min(100, showMusicbrainzDownloadProgress ? musicbrainzDisplayProgress : localStackProgress))}
                      className="h-2 bg-white/10"
                    />
                    {!localRuntimeReady && showMusicbrainzDownloadProgress && musicbrainzDisplayEta ? (
                      <div className="text-[11px] uppercase tracking-[0.14em] text-primary/80">ETA {musicbrainzDisplayEta}</div>
                    ) : null}
                  </div>

                  <div className="grid gap-3 md:grid-cols-2">
                    {localStackChecklist.map((item) => {
                      const itemDisplayProgress = Math.max(0, Math.min(100, Math.round(Number(item.displayProgress ?? item.progress) || 0)));
                      return (
                        <div key={item.key} className="rounded-2xl border border-white/10 bg-black/10 p-4">
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-white">{item.label}</div>
                            <Badge variant={item.done ? 'default' : itemDisplayProgress > 0 ? 'secondary' : 'outline'}>
                              {item.done ? 'Ready' : itemDisplayProgress > 0 ? `${itemDisplayProgress}%` : item.phase || 'Idle'}
                            </Badge>
                          </div>
                          <p className="mt-2 text-xs leading-5 text-slate-400">{item.detail}</p>
                          {!item.done && itemDisplayProgress > 0 ? (
                            <Progress value={itemDisplayProgress} className="mt-3 h-1.5 bg-white/10" />
                          ) : null}
                          {!item.done && item.eta ? (
                            <div className="mt-2 text-[11px] uppercase tracking-[0.14em] text-primary/80">ETA {item.eta}</div>
                          ) : null}
                        </div>
                      );
                    })}
                  </div>

                  {showRuntimeDetails ? (
                    <div className="space-y-3 rounded-2xl border border-white/10 bg-black/10 p-4">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-white">Runtime details</div>
                          <p className="mt-1 text-xs text-slate-400">Scaling & orchestration stays hidden by default. Use this only when you need to inspect the local setup state.</p>
                        </div>
                        <Badge variant="outline" className="border-white/15 bg-white/5 text-slate-200">
                          {managedPreflightReady ? 'Docker ready' : 'Docker unavailable'}
                        </Badge>
                      </div>
                      <p className="text-xs text-slate-400">{managedStatus?.preflight.message || 'Docker socket and runtime tooling are being checked.'}</p>
                      {localStackServices.length > 0 ? (
                        <div className="grid gap-2 md:grid-cols-2">
                          {localStackServices.map((service) => (
                            <div key={`${service.bundleLabel}-${service.name}`} className="rounded-2xl border border-white/10 bg-white/[0.02] px-3 py-3">
                              <div className="flex items-center justify-between gap-3">
                                <div className="text-sm font-medium text-white">{service.name}</div>
                                <Badge variant="outline" className="border-white/15 bg-white/5 text-slate-200">
                                  {service.status}
                                </Badge>
                              </div>
                              <div className="mt-1 text-[11px] font-medium uppercase tracking-[0.14em] text-slate-500">{service.bundleLabel}</div>
                              <p className="mt-2 text-xs leading-5 text-slate-400">{service.message || 'Detected on this server.'}</p>
                            </div>
                          ))}
                        </div>
                      ) : null}
                      {localStackRecentLogs.length > 0 ? (
                        <div className="space-y-2">
                          {localStackRecentLogs.map((entry) => (
                            <div key={entry.log_id} className="rounded-2xl border border-white/10 bg-white/[0.02] px-3 py-2.5">
                              <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.14em] text-slate-500">
                                <span>{managedBundleLabel(entry.bundle_type)}</span>
                                {entry.service_name ? <span>• {entry.service_name}</span> : null}
                                {formatRuntimeLogTime(entry.created_at) ? <span>• {formatRuntimeLogTime(entry.created_at)}</span> : null}
                              </div>
                              <p className="mt-1 text-xs leading-5 text-slate-300">{compactManagedLogMessage(entry.message)}</p>
                            </div>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              ) : (
                <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="space-y-1">
                    <div className="text-sm font-semibold text-white">External AI provider</div>
                    <p className="text-xs text-slate-400">Pick the paid provider, add the API key, then wait for PMDA to validate it and fetch the compatible models automatically.</p>
                  </div>
                  <div className="grid gap-3 md:grid-cols-3">
                    {EXTERNAL_AI_OPTIONS.map((option) => {
                      const active = selectedExternalProvider === option.value;
                      return (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => setExternalProvider(option.value)}
                          className={`rounded-2xl border p-4 text-left transition ${active ? 'border-primary/45 bg-primary/12' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-white">{option.label}</div>
                            {active ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                          </div>
                          <p className="mt-2 text-xs leading-5 text-slate-400">{option.description}</p>
                        </button>
                      );
                    })}
                  </div>

                  <div className="grid gap-4 lg:grid-cols-2">
                    <div className="space-y-2 rounded-2xl border border-white/10 bg-black/10 p-4">
                      <Label className="text-sm text-white">API key</Label>
                      <PasswordInput
                        value={selectedExternalProvider === 'openai-api' ? String(config.OPENAI_API_KEY || '') : selectedExternalProvider === 'anthropic' ? String(config.ANTHROPIC_API_KEY || '') : String(config.GOOGLE_API_KEY || '')}
                        onChange={(event) => {
                          const value = event.target.value;
                          if (selectedExternalProvider === 'openai-api') updateConfig({ OPENAI_API_KEY: value, AI_PROVIDER: 'openai-api' });
                          if (selectedExternalProvider === 'anthropic') updateConfig({ ANTHROPIC_API_KEY: value, AI_PROVIDER: 'anthropic' });
                          if (selectedExternalProvider === 'google') updateConfig({ GOOGLE_API_KEY: value, AI_PROVIDER: 'google' });
                        }}
                        placeholder={selectedExternalProvider === 'openai-api' ? 'sk-...' : 'Enter your API key'}
                      />
                      <p className="text-[11px] text-slate-500">{EXTERNAL_AI_OPTIONS.find((option) => option.value === selectedExternalProvider)?.docsUrl}</p>
                    </div>
                    <div className="space-y-2 rounded-2xl border border-white/10 bg-black/10 p-4">
                      <Label className="text-sm text-white">Validation</Label>
                      <div className="flex items-center gap-2">
                        <Badge variant={externalValidationState === 'valid' ? 'default' : externalValidationState === 'error' ? 'destructive' : externalValidationState === 'loading' ? 'secondary' : 'outline'}>
                          {externalValidationState === 'valid' ? 'Valid' : externalValidationState === 'error' ? 'Issue' : externalValidationState === 'loading' ? 'Checking' : 'Waiting for key'}
                        </Badge>
                      </div>
                      <p className="text-xs leading-5 text-slate-400">{externalValidationMessage || 'Add a valid API key to continue.'}</p>
                    </div>
                  </div>

                  <div className="space-y-2 rounded-2xl border border-white/10 bg-black/10 p-4">
                    <Label className="text-sm text-white">Model</Label>
                    <Select
                      value={externalModel}
                      onValueChange={(value) => updateConfig({ OPENAI_MODEL: value, AI_PROVIDER: selectedExternalProvider })}
                      disabled={externalValidationState !== 'valid' || externalModels.length === 0}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder={externalValidationState === 'valid' ? 'Choose a model' : 'Validate the provider first'} />
                      </SelectTrigger>
                      <SelectContent>
                        {externalModels.map((model) => (
                          <SelectItem key={model} value={model}>{model}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-slate-400">PMDA only enables the next step after the provider is valid and one compatible model has been selected.</p>
                  </div>
                </div>
              )}

              <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                  <div>
                    <div className="text-sm font-semibold text-white">Metadata sources</div>
                    <p className="mt-1 text-xs text-slate-400">These are the source providers PMDA will use during the first scan. If a source is on, configure it here or turn it off explicitly.</p>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={sourcesConfigured ? 'default' : 'outline'}>{sourcesConfigured ? 'Sources ready' : 'Sources need attention'}</Badge>
                    <Button type="button" variant="outline" size="sm" onClick={() => void refreshProviderStatus()} disabled={providersChecking}>
                      {providersChecking ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                      Check source keys
                    </Button>
                  </div>
                </div>

                <div className="grid gap-3 lg:grid-cols-2">
                  <div className="rounded-2xl border border-white/10 bg-black/10 p-4 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">MusicBrainz</div>
                        <p className="text-xs text-slate-400">Required base source. Uses the local mirror when local mode is selected.</p>
                      </div>
                      <Switch checked={musicbrainzEnabled} onCheckedChange={(checked) => updateConfig({ USE_MUSICBRAINZ: checked })} />
                    </div>
                    <Badge variant={musicbrainzStatus.variant}>{musicbrainzStatus.label}</Badge>
                    <p className="text-xs leading-5 text-slate-400">{musicbrainzStatus.message}</p>
                    {musicbrainzEnabled ? (
                      <div className="space-y-2">
                        <Label className="text-xs text-slate-200">Contact email</Label>
                        <Input value={String(config.MUSICBRAINZ_EMAIL || '')} onChange={(event) => updateConfig({ MUSICBRAINZ_EMAIL: event.target.value })} placeholder="name@example.com" />
                      </div>
                    ) : null}
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-black/10 p-4 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">Discogs</div>
                        <p className="text-xs text-slate-400">Useful for release variants, labels and covers when MusicBrainz is not enough.</p>
                      </div>
                      <Switch checked={discogsEnabled} onCheckedChange={(checked) => updateConfig({ USE_DISCOGS: checked })} />
                    </div>
                    <Badge variant={discogsStatus.variant}>{discogsStatus.label}</Badge>
                    <p className="text-xs leading-5 text-slate-400">{discogsStatus.message}</p>
                    {discogsEnabled ? (
                      <div className="space-y-2">
                        <Label className="text-xs text-slate-200">User token</Label>
                        <PasswordInput value={String(config.DISCOGS_USER_TOKEN || '')} onChange={(event) => updateConfig({ DISCOGS_USER_TOKEN: event.target.value })} placeholder="Discogs user token" />
                      </div>
                    ) : null}
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-black/10 p-4 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">Last.fm</div>
                        <p className="text-xs text-slate-400">Useful for genres and fallback artist metadata when other providers are incomplete.</p>
                      </div>
                      <Switch checked={lastfmEnabled} onCheckedChange={(checked) => updateConfig({ USE_LASTFM: checked })} />
                    </div>
                    <Badge variant={lastfmStatus.variant}>{lastfmStatus.label}</Badge>
                    <p className="text-xs leading-5 text-slate-400">{lastfmStatus.message}</p>
                    {lastfmEnabled ? (
                      <div className="grid gap-2 md:grid-cols-2">
                        <div className="space-y-2">
                          <Label className="text-xs text-slate-200">API key</Label>
                          <Input value={String(config.LASTFM_API_KEY || '')} onChange={(event) => updateConfig({ LASTFM_API_KEY: event.target.value })} placeholder="Last.fm API key" />
                        </div>
                        <div className="space-y-2">
                          <Label className="text-xs text-slate-200">API secret</Label>
                          <PasswordInput value={String(config.LASTFM_API_SECRET || '')} onChange={(event) => updateConfig({ LASTFM_API_SECRET: event.target.value })} placeholder="Last.fm API secret" />
                        </div>
                      </div>
                    ) : null}
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-black/10 p-4 space-y-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">AcoustID</div>
                        <p className="text-xs text-slate-400">Useful when tags are missing and PMDA needs fingerprint-based identification.</p>
                      </div>
                      <Switch checked={acoustidEnabled} onCheckedChange={(checked) => updateConfig({ USE_ACOUSTID: checked })} />
                    </div>
                    <Badge variant={acoustidStatus.variant}>{acoustidStatus.label}</Badge>
                    <p className="text-xs leading-5 text-slate-400">{acoustidStatus.message}</p>
                    {acoustidEnabled ? (
                      <div className="space-y-2">
                        <Label className="text-xs text-slate-200">API key</Label>
                        <PasswordInput value={String(config.ACOUSTID_API_KEY || '')} onChange={(event) => updateConfig({ ACOUSTID_API_KEY: event.target.value })} placeholder="AcoustID API key" />
                      </div>
                    ) : null}
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-black/10 p-4 space-y-3 lg:col-span-2">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">Bandcamp</div>
                        <p className="text-xs text-slate-400">Enabled by default. No key required. Keeps an additional fallback path for independent releases.</p>
                      </div>
                      <Switch checked={bandcampEnabled} onCheckedChange={(checked) => updateConfig({ USE_BANDCAMP: checked })} />
                    </div>
                    <Badge variant={bandcampStatus.variant}>{bandcampStatus.label}</Badge>
                    <p className="text-xs leading-5 text-slate-400">{bandcampStatus.message}</p>
                  </div>
                </div>

                {sourceIssues.length > 0 ? (
                  <div className="rounded-2xl border border-amber-500/25 bg-amber-500/10 p-4 text-sm text-amber-100">
                    <div className="flex items-center gap-2 font-medium">
                      <AlertTriangle className="h-4 w-4" />
                      Sources still need attention
                    </div>
                    <ul className="mt-2 space-y-1 text-xs text-amber-50/90">
                      {sourceIssues.map((issue) => (
                        <li key={issue}>• {issue}</li>
                      ))}
                    </ul>
                  </div>
                ) : (
                  <div className="rounded-2xl border border-emerald-500/25 bg-emerald-500/10 p-4 text-sm text-emerald-100">
                    <div className="flex items-center gap-2 font-medium">
                      <CheckCircle2 className="h-4 w-4" />
                      Enabled sources are configured
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : null}

          {activeStep === 4 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">Choose the pipeline behavior</div>
                <p className="text-sm text-slate-400">This is the minimum PMDA behavior that must be explicit before the first scan can start.</p>
              </div>

              {wantsPublishedLibrary ? (
                <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div>
                    <div className="text-sm font-semibold text-white">Publish a clean library</div>
                    <p className="mt-1 text-xs text-slate-400">Because this workflow builds a clean result, PMDA should publish it by default. You can still switch it off explicitly.</p>
                  </div>
                  <div className="grid gap-3 md:grid-cols-2">
                    {[
                      { value: true, label: 'Yes, publish it', description: 'Build the clean serving library after the scan.' },
                      { value: false, label: 'No, keep publish off', description: 'Only analyze and organize without building the clean serving tree.' },
                    ].map((option) => {
                      const active = publishLibraryEnabled === option.value;
                      return (
                        <button
                          key={String(option.value)}
                          type="button"
                          onClick={() => updateConfig({ PIPELINE_ENABLE_EXPORT: option.value })}
                          className={`rounded-2xl border p-4 text-left transition ${active ? 'border-primary/45 bg-primary/12' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-white">{option.label}</div>
                            {active ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                          </div>
                          <p className="mt-2 text-xs leading-5 text-slate-400">{option.description}</p>
                        </button>
                      );
                    })}
                  </div>

                  {publishLibraryEnabled ? (
                    <div className="space-y-3">
                      <div>
                        <div className="text-sm font-semibold text-white">Materialization method</div>
                        <p className="mt-1 text-xs text-slate-400">Choose how PMDA should build the published library on disk.</p>
                      </div>
                      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                        {MATERIALIZATION_OPTIONS.map((option) => {
                          const active = materializationMode === option.value;
                          return (
                            <button
                              key={option.value}
                              type="button"
                              onClick={() => updateConfig({ EXPORT_LINK_STRATEGY: option.value, LIBRARY_MATERIALIZATION_MODE: option.value })}
                              className={`rounded-2xl border p-4 text-left transition ${active ? 'border-primary/45 bg-primary/12' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                            >
                              <div className="flex items-center justify-between gap-3">
                                <div className="text-sm font-semibold text-white">{option.label}</div>
                                {active ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                              </div>
                              <p className="mt-2 text-xs leading-5 text-slate-400">{option.description}</p>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : (
                <div className="rounded-3xl border border-white/10 bg-white/[0.03] p-4 text-sm text-slate-300">
                  This workflow serves the current library directly, so PMDA will not build a second published tree.
                </div>
              )}

              <div className="grid gap-4 lg:grid-cols-2">
                <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div>
                    <div className="text-sm font-semibold text-white">Duplicates</div>
                    <p className="mt-1 text-xs text-slate-400">Choose whether PMDA should ignore duplicates, detect them only, or move duplicate losers automatically.</p>
                  </div>
                  {[
                    { value: 'ignore' as const, label: 'Ignore', description: 'Do not run the dedupe stage.' },
                    { value: 'detect' as const, label: 'Detect only', description: 'Keep duplicates visible but do not move anything automatically.' },
                    { value: 'move' as const, label: 'Detect and move', description: 'Move duplicate losers to the duplicates folder automatically.' },
                  ].map((option) => {
                    const active = dedupeMode === option.value;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        onClick={() => setDedupeMode(option.value)}
                        className={`rounded-2xl border p-4 text-left transition ${active ? 'border-primary/45 bg-primary/12' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-white">{option.label}</div>
                          {active ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                        </div>
                        <p className="mt-2 text-xs leading-5 text-slate-400">{option.description}</p>
                      </button>
                    );
                  })}
                </div>

                <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div>
                    <div className="text-sm font-semibold text-white">Incomplete albums</div>
                    <p className="mt-1 text-xs text-slate-400">Choose whether incomplete releases should stay in place or move to the quarantine folder.</p>
                  </div>
                  {[
                    { value: 'move' as const, label: 'Move to incomplete folder', description: 'Keep the clean library isolated from releases with missing tracks.' },
                    { value: 'keep' as const, label: 'Leave in place', description: 'Do not move incomplete albums automatically.' },
                  ].map((option) => {
                    const active = incompleteMode === option.value;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        onClick={() => setIncompleteHandling(option.value)}
                        className={`rounded-2xl border p-4 text-left transition ${active ? 'border-primary/45 bg-primary/12' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-white">{option.label}</div>
                          {active ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                        </div>
                        <p className="mt-2 text-xs leading-5 text-slate-400">{option.description}</p>
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="space-y-4 rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                <div>
                  <div className="text-sm font-semibold text-white">Player sync</div>
                  <p className="mt-1 text-xs text-slate-400">Optional. Choose a target only if you want PMDA to refresh a player right after the pipeline.</p>
                </div>
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  {PLAYER_TARGET_OPTIONS.map((option) => {
                    const active = playerTarget === option.value;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        onClick={() => setPlayerTarget(option.value)}
                        className={`rounded-2xl border p-4 text-left transition ${active ? 'border-primary/45 bg-primary/12' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-semibold text-white">{option.label}</div>
                          {active ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                        </div>
                        <p className="mt-2 text-xs leading-5 text-slate-400">{option.description}</p>
                      </button>
                    );
                  })}
                </div>

                {playerTarget === 'plex' ? (
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">Plex URL</Label>
                      <Input value={String(config.PLEX_HOST || '')} onChange={(event) => updateConfig({ PLEX_HOST: event.target.value })} placeholder="http://plex:32400" />
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">Plex token</Label>
                      <PasswordInput value={String(config.PLEX_TOKEN || '')} onChange={(event) => updateConfig({ PLEX_TOKEN: event.target.value })} placeholder="Plex token" />
                    </div>
                  </div>
                ) : null}

                {playerTarget === 'jellyfin' ? (
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">Jellyfin URL</Label>
                      <Input value={String(config.JELLYFIN_URL || '')} onChange={(event) => updateConfig({ JELLYFIN_URL: event.target.value })} placeholder="http://jellyfin:8096" />
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">Jellyfin API key</Label>
                      <PasswordInput value={String(config.JELLYFIN_API_KEY || '')} onChange={(event) => updateConfig({ JELLYFIN_API_KEY: event.target.value })} placeholder="Jellyfin API key" />
                    </div>
                  </div>
                ) : null}

                {playerTarget === 'navidrome' ? (
                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">Navidrome URL</Label>
                      <Input value={String(config.NAVIDROME_URL || '')} onChange={(event) => updateConfig({ NAVIDROME_URL: event.target.value })} placeholder="http://navidrome:4533" />
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">API key (optional)</Label>
                      <PasswordInput value={String(config.NAVIDROME_API_KEY || '')} onChange={(event) => updateConfig({ NAVIDROME_API_KEY: event.target.value })} placeholder="Navidrome API key" />
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs text-slate-200">Username / password fallback</Label>
                      <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-1">
                        <Input value={String(config.NAVIDROME_USERNAME || '')} onChange={(event) => updateConfig({ NAVIDROME_USERNAME: event.target.value })} placeholder="Username" />
                        <PasswordInput value={String(config.NAVIDROME_PASSWORD || '')} onChange={(event) => updateConfig({ NAVIDROME_PASSWORD: event.target.value })} placeholder="Password" />
                      </div>
                    </div>
                  </div>
                ) : null}

                {!playerSyncReady ? (
                  <div className="rounded-2xl border border-amber-500/25 bg-amber-500/10 p-4 text-sm text-amber-100">
                    <div className="flex items-center gap-2 font-medium">
                      <AlertTriangle className="h-4 w-4" />
                      Player sync still needs credentials
                    </div>
                    <p className="mt-2 text-xs text-amber-50/90">Complete the credentials for the selected target or switch player sync off for the first scan.</p>
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}

          {activeStep === 5 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">Review before the first full scan</div>
                <p className="text-sm text-slate-400">This is the compact summary of what PMDA will actually do when you launch the first scan.</p>
              </div>

              <div className="rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                <div className="space-y-3">
                  {reviewRows.map((row) => (
                    <div key={row.label} className="flex flex-col gap-3 rounded-2xl border border-white/10 bg-black/10 p-4 lg:flex-row lg:items-center lg:justify-between">
                      <div className="min-w-0">
                        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{row.label}</div>
                        <div className="mt-1 text-sm font-semibold text-white">{row.value}</div>
                        <p className="mt-1 text-xs leading-5 text-slate-400">{row.detail}</p>
                      </div>
                      <Button type="button" variant="outline" size="sm" onClick={() => void goToStep(row.step)}>
                        Modify
                      </Button>
                    </div>
                  ))}
                </div>
              </div>

              {blockers.length > 0 ? (
                <div className="rounded-3xl border border-amber-500/25 bg-amber-500/10 p-4 text-sm text-amber-100">
                  <div className="flex items-center gap-2 font-medium">
                    <AlertTriangle className="h-4 w-4" />
                    One last thing needs attention
                  </div>
                  <p className="mt-2 text-xs text-amber-50/90">{blockers[0]?.detail}</p>
                </div>
              ) : (
                <div className="rounded-3xl border border-emerald-500/25 bg-emerald-500/10 p-4 text-sm text-emerald-100">
                  <div className="flex items-center gap-2 font-medium">
                    <CheckCircle2 className="h-4 w-4" />
                    Configuration minimale prête
                  </div>
                  <p className="mt-2 text-xs text-emerald-50/90">PMDA has the minimum safe setup to launch or resume the first full scan.</p>
                </div>
              )}

              {firstScanStarted ? (
                <div className="rounded-3xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="flex items-center justify-between gap-3 text-sm text-white">
                    <div className="font-medium">
                      {scanProgress?.scanning ? `Current phase: ${scanProgress.phase || 'running'}` : 'A scan has already been started'}
                    </div>
                    <Badge variant="outline" className="border-white/15 bg-white/5 text-slate-200">
                      {Math.round(scanProgressPercent)}%
                    </Badge>
                  </div>
                  <Progress value={Math.max(0, Math.min(100, scanProgressPercent))} className="mt-3 h-2 bg-white/10" />
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="flex flex-col gap-3 border-t border-white/10 pt-5 sm:flex-row sm:items-center sm:justify-between">
            <Button type="button" variant="outline" onClick={() => void goToStep(activeStep - 1)} disabled={activeStep === 0 || isSaving}>
              Previous
            </Button>
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              {activeStep < STEP_ORDER.length - 1 ? (
                <Button
                  type="button"
                  className="gap-2"
                  onClick={() => void handleNextStep()}
                  disabled={
                    isSaving
                    || runtimeActionBusy
                    || (activeStep === 1 && !foldersReady)
                    || (activeStep === 3 && !metadataSetupReady)
                    || (activeStep === 4 && !pipelineConfigReady)
                  }
                >
                  {isSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                  {activeStep === 2 && localStackNeedsConfirmation ? 'Continue' : activeStep === 4 ? 'Review first scan' : 'Next'}
                </Button>
              ) : (
                <Button type="button" className="gap-2" onClick={() => void startOrResumeScan()} disabled={scanActionBusy || !launchReady}>
                  {scanActionBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <PlayCircle className="h-4 w-4" />}
                  {scanProgress?.resume_available && !scanProgress?.scanning ? 'Resume scan' : scanProgress?.scanning ? 'Open scan' : firstScanStarted && !Boolean(bootstrap?.bootstrap_required) ? 'Open scan' : 'Start first full scan'}
                </Button>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      <AlertDialog open={localStackConfirmOpen} onOpenChange={setLocalStackConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Create the local metadata stack now?</AlertDialogTitle>
            <AlertDialogDescription>
              PMDA will create or adopt the local MusicBrainz mirror and Ollama runtime, then download the required AI models
              <span className="font-mono"> {ollamaModel}</span>
              {' '}and
              <span className="font-mono"> {ollamaHardModel}</span>.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="space-y-3 text-sm text-muted-foreground">
            <p>Make sure <span className="font-mono text-foreground">{resolvedManagedDataRoot}</span> has at least <span className="font-semibold text-foreground">{LOCAL_STACK_RECOMMENDED_FREE_GB} GB</span> free. The MusicBrainz import is disk-intensive and the full setup can take a while depending on your connection and storage.</p>
            <p>After confirmation, the next step becomes a compact live progress view with the real MusicBrainz / Ollama status.</p>
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={runtimeActionBusy}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={runtimeActionBusy}
              onClick={(event) => {
                event.preventDefault();
                void confirmAndStartLocalStack();
              }}
            >
              {runtimeActionBusy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              {localStackActionLabel}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
