import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  FolderOutput,
  Loader2,
  PlayCircle,
  Plus,
  RefreshCw,
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
import { Label } from '@/components/ui/label';
import { Progress } from '@/components/ui/progress';

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
type StepId = 'workflow' | 'folders' | 'metadata' | 'ready';

type Blocker = {
  key: string;
  title: string;
  detail: string;
  sectionId: string;
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
  { id: 'ready', label: 'Step 4', title: 'Ready' },
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

const LOCAL_STACK_RECOMMENDED_FREE_GB = 40;

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

  const state = String(bundle.state || '').trim().toLowerCase();
  const phase = String(bundle.phase || '').trim().toLowerCase();
  const combined = `${state} ${phase}`;
  const metaProgressRaw = Number((bundle.meta as Record<string, unknown> | undefined)?.progress);
  if (Number.isFinite(metaProgressRaw) && metaProgressRaw > 0) {
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

function hasManagedBundleStarted(bundle: ManagedRuntimeBundleStatus | null | undefined): boolean {
  if (!bundle) return false;
  if (bundle.effective_url || bundle.health?.available) return true;
  const mode = String(bundle.mode || '').trim().toLowerCase();
  const state = String(bundle.state || '').trim().toLowerCase();
  const phase = String(bundle.phase || '').trim().toLowerCase();
  if (mode && mode !== 'absent') return true;
  if (state && state !== 'idle' && state !== 'absent') return true;
  if (phase && phase !== 'idle') return true;
  return false;
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
  const [scanProgress, setScanProgress] = useState<ScanProgress | null>(null);
  const [statusLoading, setStatusLoading] = useState<boolean>(true);
  const [scanActionBusy, setScanActionBusy] = useState<boolean>(false);
  const [runtimeActionBusy, setRuntimeActionBusy] = useState<boolean>(false);
  const [localStackConfirmOpen, setLocalStackConfirmOpen] = useState<boolean>(false);
  const localStackAutoPromptedRef = useRef(false);

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

  const selectedStackMode: StackMode = Boolean(
    config.MUSICBRAINZ_MIRROR_ENABLED
      || String(config.WEB_SEARCH_PROVIDER || '').trim().toLowerCase() === 'ollama'
      || String(config.AI_PROVIDER || '').trim().toLowerCase() === 'ollama',
  )
    ? 'local'
    : 'online';

  const ollamaUrl = String(config.OLLAMA_URL || '').trim() || 'http://localhost:11434';
  const ollamaModel = String(config.OLLAMA_MODEL || '').trim() || 'qwen3:4b';
  const ollamaHardModel = String(config.OLLAMA_COMPLEX_MODEL || '').trim() || 'qwen3:14b';
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

  const shouldFetchManagedCandidates = selectedStackMode === 'local'
    && activeStep === 3
    && !runtimeActionBusy;
  const shouldFetchManagedLogs = selectedStackMode === 'local' && activeStep === 3;

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

  const localStackActivity = useMemo(() => {
    if (selectedStackMode !== 'local') return false;
    const bundles = [managedMbBundle, managedOllamaBundle];
    return bundles.some((bundle) => {
      if (!bundle) return false;
      const state = String(bundle.state || '').trim().toLowerCase();
      return state !== 'ready' && state !== 'failed' && state !== 'absent';
    });
  }, [managedMbBundle, managedOllamaBundle, selectedStackMode]);
  const localStackHasStarted = useMemo(() => {
    if (selectedStackMode !== 'local') return false;
    return hasManagedBundleStarted(managedMbBundle) || hasManagedBundleStarted(managedOllamaBundle);
  }, [managedMbBundle, managedOllamaBundle, selectedStackMode]);

  useEffect(() => {
    void refreshStatus();
    const intervalMs = localStackActivity || runtimeActionBusy ? 3000 : 15000;
    const timer = window.setInterval(() => {
      void refreshStatus();
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [localStackActivity, refreshStatus, runtimeActionBusy]);

  useEffect(() => {
    if (!isModalPresentation) return;
    setActiveStep(0);
  }, [isModalPresentation]);

  const applyWorkflowPreset = useCallback((mode: WorkflowMode) => {
    updateConfig({
      LIBRARY_MODE: 'files',
      LIBRARY_WORKFLOW_MODE: mode,
    });
  }, [updateConfig]);

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
      });
      return;
    }

    updateConfig({
      MUSICBRAINZ_MIRROR_ENABLED: false,
      MUSICBRAINZ_RUNTIME_MODE: 'absent',
      PROVIDER_GATEWAY_ENABLED: true,
      PROVIDER_GATEWAY_CACHE_ENABLED: true,
      OLLAMA_RUNTIME_MODE: 'absent',
      SCAN_AI_POLICY: 'local_then_paid',
      WEB_SEARCH_PROVIDER: 'auto',
      AI_USAGE_LEVEL: 'auto',
    });
  }, [ollamaHardModel, ollamaModel, ollamaUrl, updateConfig]);

  const persistIfNeeded = useCallback(async () => {
    if (!isModalPresentation || !dirty || !onSave) return true;
    return onSave();
  }, [dirty, isModalPresentation, onSave]);

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
    if (localRuntimeReady || localStackHasStarted || runtimeActionBusy || statusLoading || localStackConfirmOpen) {
      return;
    }
    if (!managedPreflightReady) {
      localStackAutoPromptedRef.current = false;
      return;
    }
    if (localStackAutoPromptedRef.current) return;
    localStackAutoPromptedRef.current = true;
    setLocalStackConfirmOpen(true);
  }, [
    activeStep,
    localRuntimeReady,
    localStackConfirmOpen,
    localStackHasStarted,
    managedPreflightReady,
    runtimeActionBusy,
    selectedStackMode,
    statusLoading,
  ]);

  const localStackDetectedCount = (managedMbCandidate ? 1 : 0) + (managedOllamaCandidate ? 1 : 0);
  const localStackActionLabel = localStackDetectedCount > 0
    ? (localStackDetectedCount === 2 ? 'Use detected local services' : 'Use detected service and create the rest')
    : 'Create local stack';
  const localStackActionDescription = localStackDetectedCount === 2
    ? 'PMDA found an existing MusicBrainz mirror and an existing Ollama runtime on this server. It can adopt them directly after your confirmation.'
    : localStackDetectedCount === 1
      ? 'PMDA found one existing local service on this server. It can adopt it and create only the missing part after your confirmation.'
      : 'No local metadata service was detected yet. PMDA will create the MusicBrainz mirror and the Ollama runtime.';

  const localStackChecklist = useMemo(() => {
    if (selectedStackMode !== 'local') return [];
    const musicbrainzDetected = Boolean(managedMbCandidate) && !musicbrainzReady && !managedMbBundle?.effective_url;
    const ollamaDetected = Boolean(managedOllamaCandidate) && !ollamaReady && !managedOllamaBundle?.effective_url;
    const musicbrainzNotStarted = String(managedMbBundle?.mode || 'absent').trim().toLowerCase() === 'absent'
      || String(managedMbBundle?.state || 'idle').trim().toLowerCase() === 'idle';
    const ollamaNotStarted = String(managedOllamaBundle?.mode || 'absent').trim().toLowerCase() === 'absent'
      || String(managedOllamaBundle?.state || 'idle').trim().toLowerCase() === 'idle';
    return [
      {
        key: 'docker',
        label: 'Docker access',
        done: managedPreflightReady,
        detail: managedPreflightReady
          ? 'PMDA can manage local runtime containers on this server.'
          : statusLoading
            ? 'Checking Docker socket and CLI.'
            : (managedStatus?.preflight.message || 'Docker socket or runtime tooling is not available yet.'),
        progress: managedPreflightReady ? 100 : (statusLoading ? 6 : 0),
      },
      {
        key: 'musicbrainz',
        label: 'MusicBrainz mirror',
        done: musicbrainzReady,
        detail: musicbrainzReady
          ? 'Local MusicBrainz mirror is ready.'
          : (musicbrainzDetected
              ? `Existing MusicBrainz mirror detected${managedMbCandidate?.published_url ? ` at ${managedMbCandidate.published_url}` : ''}. It will be adopted when you confirm local stack setup.`
              : (managedMbBundle?.phase_message || (musicbrainzNotStarted ? 'Not created yet. It will start after you confirm local stack setup.' : 'MusicBrainz mirror is starting.'))),
        progress: musicbrainzNotStarted ? 0 : managedBundleProgress(managedMbBundle),
      },
      {
        key: 'ollama',
        label: 'Ollama + required models',
        done: ollamaReady,
        detail: ollamaReady
          ? `Ollama is ready with ${ollamaModel} and ${ollamaHardModel}.`
          : (ollamaDetected
              ? `Existing Ollama runtime detected${managedOllamaCandidate?.url ? ` at ${managedOllamaCandidate.url}` : ''}. It will be adopted when you confirm local stack setup and PMDA will ensure ${ollamaModel} and ${ollamaHardModel}.`
              : (managedOllamaBundle?.phase_message || (ollamaNotStarted
                  ? `Not created yet. It will start after you confirm local stack setup and install ${ollamaModel} and ${ollamaHardModel}.`
                  : `Ollama is starting and still needs ${ollamaModel} and ${ollamaHardModel}.`))),
        progress: ollamaNotStarted ? 0 : managedBundleProgress(managedOllamaBundle, { requiredModels: [ollamaModel, ollamaHardModel] }),
      },
    ];
  }, [localStackActionLabel, managedMbBundle, managedMbCandidate, managedOllamaBundle, managedOllamaCandidate, managedPreflightReady, managedStatus?.preflight.message, musicbrainzReady, ollamaHardModel, ollamaModel, ollamaReady, selectedStackMode, statusLoading]);

  const localStackProgress = useMemo(() => {
    if (selectedStackMode !== 'local' || localStackChecklist.length === 0) return 0;
    const serviceItems = localStackChecklist.filter((item) => item.key !== 'docker');
    if (serviceItems.length === 0) return 0;
    const total = serviceItems.reduce((sum, item) => sum + item.progress, 0);
    return Math.round(total / serviceItems.length);
  }, [localStackChecklist, selectedStackMode]);

  const localStackActivityRows = useMemo(() => {
    if (selectedStackMode !== 'local' || !localStackHasStarted) return [];
    return [
      {
        key: 'docker',
        label: 'Docker access',
        status: managedPreflightReady ? 'Ready' : 'Waiting',
        detail: managedStatus?.preflight.message || (managedPreflightReady ? 'Docker runtime is available.' : 'Checking Docker socket and CLI.'),
      },
      {
        key: 'musicbrainz',
        label: 'MusicBrainz mirror',
        status: musicbrainzReady ? 'Ready' : (managedMbBundle?.phase || managedMbBundle?.state || 'Pending'),
        detail: managedMbBundle?.phase_message || (managedMbCandidate ? 'Existing MusicBrainz mirror detected on this server.' : 'Preparing the local MusicBrainz mirror.'),
      },
      {
        key: 'ollama',
        label: 'Ollama + required models',
        status: ollamaReady ? 'Ready' : (managedOllamaBundle?.phase || managedOllamaBundle?.state || 'Pending'),
        detail: managedOllamaBundle?.phase_message || (managedOllamaCandidate ? 'Existing Ollama runtime detected on this server.' : `Preparing Ollama and models ${ollamaModel}, ${ollamaHardModel}.`),
      },
    ];
  }, [
    localStackHasStarted,
    managedMbBundle,
    managedMbCandidate,
    managedOllamaBundle,
    managedOllamaCandidate,
    managedPreflightReady,
    managedStatus?.preflight.message,
    musicbrainzReady,
    ollamaHardModel,
    ollamaModel,
    ollamaReady,
    selectedStackMode,
  ]);

  const localStackServices = useMemo(() => {
    if (selectedStackMode !== 'local') return [];
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
  }, [managedMbBundle?.services, managedOllamaBundle?.services, selectedStackMode]);

  const localStackRecentLogs = useMemo(() => {
    if (selectedStackMode !== 'local') return [];
    const showLogs = runtimeActionBusy || localStackActivity || localStackHasStarted;
    if (!showLogs) return [];
    return managedRuntimeLogs
      .filter((entry) => entry.bundle_type === 'musicbrainz_local' || entry.bundle_type === 'ollama_local')
      .slice(0, 8);
  }, [localStackActivity, localStackHasStarted, managedRuntimeLogs, runtimeActionBusy, selectedStackMode]);

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

  const blockers = useMemo(() => {
    const next: Blocker[] = [];

    if (!foldersReady) {
      next.push({
        key: 'folders',
        title: 'Required folders are missing',
        detail: missingFolderItems[0] || 'Choose the mandatory folders for this workflow.',
        sectionId: 'settings-files-export',
      });
    }

    if (selectedStackMode === 'local' && !localRuntimeReady) {
      const detail = !managedPreflightReady
        ? (managedStatus?.preflight.message || 'Docker socket or runtime tooling is not ready.')
        : !localStackHasStarted
          ? `Nothing has started yet. Click ${localStackActionLabel} to create or adopt the local MusicBrainz + Ollama stack.`
          : !musicbrainzReady
            ? (managedMbBundle?.phase_message || (managedMbCandidate ? 'An existing MusicBrainz mirror was detected. Use the local services button to adopt it.' : 'Managed MusicBrainz is not ready yet.'))
            : (managedOllamaBundle?.phase_message || (managedOllamaCandidate ? 'An existing Ollama runtime was detected. Use the local services button to adopt it and ensure the required models.' : 'Managed Ollama or the required models are not ready yet.'));
      next.push({
        key: 'local-runtime',
        title: localStackHasStarted ? 'Local metadata stack is not ready yet' : 'Local metadata stack has not been started yet',
        detail,
        sectionId: 'settings-scaling',
      });
    }

    return next;
  }, [foldersReady, localRuntimeReady, localStackActionLabel, localStackHasStarted, managedMbBundle?.phase_message, managedMbCandidate, managedOllamaBundle?.phase_message, managedOllamaCandidate, managedPreflightReady, managedStatus?.preflight.message, missingFolderItems, musicbrainzReady, selectedStackMode]);

  const completionCount = useMemo(() => {
    let count = 0;
    if (workflowMode) count += 1;
    if (foldersReady) count += 1;
    if (selectedStackMode === 'online' || localRuntimeReady) count += 1;
    if (blockers.length === 0) count += 1;
    return count;
  }, [blockers.length, foldersReady, localRuntimeReady, selectedStackMode, workflowMode]);

  const wizardPercent = Math.round(((activeStep + 1) / STEP_ORDER.length) * 100);

  const scanProgressPercent = Number.isFinite(Number(scanProgress?.stage_progress_percent))
    ? Number(scanProgress?.stage_progress_percent)
    : Number.isFinite(Number(scanProgress?.overall_progress_percent))
      ? Number(scanProgress?.overall_progress_percent)
      : Number.isFinite(Number(scanProgress?.progress)) && Number(scanProgress?.total)
        ? (Number(scanProgress?.progress) / Math.max(1, Number(scanProgress?.total))) * 100
        : 0;

  const metadataLabel = selectedStackMode === 'local' ? 'Local stack' : 'Online-assisted';
  const readinessLabel = blockers.length === 0 ? 'Ready to scan' : blockers[0]!.title;
  const remainingReadinessCount = Math.max(0, STEP_ORDER.length - completionCount);
  const firstScanLabel = scanProgress?.scanning
    ? 'Scan running'
    : scanProgress?.resume_available
      ? 'Resume available'
      : firstScanStarted || !Boolean(bootstrap?.bootstrap_required)
        ? 'Already started'
        : 'Not started';

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

  const confirmAndStartLocalStack = useCallback(async () => {
    const started = await bootstrapLocalStack();
    if (!started) return;
    setLocalStackConfirmOpen(false);
    await goToStep(3);
  }, [bootstrapLocalStack, goToStep]);

  const handleNextStep = useCallback(async () => {
    if (activeStep >= STEP_ORDER.length - 1) return;
    if (activeStep === 2 && selectedStackMode === 'local' && !localRuntimeReady && !localStackHasStarted && managedPreflightReady) {
      setLocalStackConfirmOpen(true);
      return;
    }
    await goToStep(activeStep + 1);
  }, [activeStep, goToStep, localRuntimeReady, localStackHasStarted, managedPreflightReady, selectedStackMode]);

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
                Keep onboarding minimal here. Open the guided flow when you want to adjust workflow, folders or metadata mode.
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
                {selectedStackMode === 'local' ? 'Uses the local MusicBrainz + Ollama stack.' : 'Uses public metadata with the provider gateway cache.'}
              </p>
            </div>
            <div className="rounded-2xl border border-border/70 bg-background/60 p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Readiness</div>
              <div className="mt-2 flex items-center gap-2 text-sm font-semibold text-foreground">
                {statusLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : blockers.length === 0 ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <AlertTriangle className="h-4 w-4 text-amber-500" />}
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
              Four steps only: pick the workflow, point PMDA at the required folders, choose the metadata mode, then start scanning when the blocking checks are clear.
            </p>
          </div>
            <div className="space-y-3">
              <div className="flex items-center justify-between text-xs text-slate-400">
              <span>{STEP_ORDER[activeStep]?.label}</span>
              <span>{remainingReadinessCount === 0 ? 'All checks ready' : `${completionCount}/${STEP_ORDER.length} checks ready`}</span>
              </div>
              <Progress value={wizardPercent} className="h-2 bg-white/10" />
              <div className="grid gap-2 sm:grid-cols-4">
                {STEP_ORDER.map((step, index) => {
                  const active = index === activeStep;
                  const complete =
                    (step.id === 'workflow' && Boolean(workflowMode))
                    || (step.id === 'folders' && foldersReady)
                    || (step.id === 'metadata' && (selectedStackMode === 'online' || selectedStackMode === 'local'))
                    || (step.id === 'ready' && blockers.length === 0);
                  const blocked = step.id === 'ready' && blockers.length > 0;
                  return (
                    <div
                      key={step.id}
                      className={`rounded-2xl border px-3 py-3 text-left ${
                      active
                        ? blocked
                          ? 'border-amber-500/35 bg-amber-500/10'
                          : 'border-primary/45 bg-primary/12'
                        : complete
                          ? 'border-emerald-500/25 bg-emerald-500/10'
                          : 'border-white/10 bg-white/[0.03]'
                    }`}
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
                <p className="text-sm text-slate-400">Pick the situation that matches your current setup. This only changes which folders PMDA asks for next.</p>
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
                    onChange={(paths) => updateConfig({ LIBRARY_INTAKE_ROOTS: stringifyPathList(paths) })}
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
                    onChange={(paths) => updateConfig({ LIBRARY_SOURCE_ROOTS: stringifyPathList(paths) })}
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
                    onChange={(value) => updateConfig({ LIBRARY_SERVING_ROOT: normalizeFolderPath(value) })}
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
                <p className="text-sm text-slate-400">This step only chooses where metadata work should happen. Advanced runtime tuning stays in Settings.</p>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <button
                  type="button"
                  onClick={() => applyStackPreset('online')}
                  className={`rounded-3xl border p-5 text-left transition ${selectedStackMode === 'online' ? 'border-primary/50 bg-primary/12 shadow-[0_0_0_1px_rgba(59,130,246,0.18)]' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-white/5 text-primary">
                      <Sparkles className="h-5 w-5" />
                    </div>
                    {selectedStackMode === 'online' ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                  </div>
                  <div className="mt-4 text-lg font-semibold text-white">Online-assisted</div>
                  <p className="mt-2 text-sm leading-6 text-slate-300">
                    Uses public metadata services. Fastest setup. Best if you want the first scan running quickly.
                  </p>
                </button>
                <button
                  type="button"
                  onClick={() => applyStackPreset('local')}
                  className={`rounded-3xl border p-5 text-left transition ${selectedStackMode === 'local' ? 'border-primary/50 bg-primary/12 shadow-[0_0_0_1px_rgba(59,130,246,0.18)]' : 'border-white/10 bg-white/[0.03] hover:border-white/20'}`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-white/5 text-primary">
                      <FolderOutput className="h-5 w-5" />
                    </div>
                    {selectedStackMode === 'local' ? <Badge className="bg-primary text-primary-foreground">Selected</Badge> : null}
                  </div>
                  <div className="mt-4 text-lg font-semibold text-white">Local stack</div>
                  <p className="mt-2 text-sm leading-6 text-slate-300">
                    Uses a local MusicBrainz mirror and a local Ollama runtime on this server. Best if you want maximum local control.
                  </p>
                </button>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 text-sm text-slate-300">
                {selectedStackMode === 'online'
                  ? 'Preset applied: PMDA uses online metadata and does not require local runtime creation in the wizard.'
                  : 'Preset applied: when you continue, PMDA will ask for confirmation before creating or adopting the local MusicBrainz mirror and local Ollama runtime.'}
              </div>
            </div>
          ) : null}

          {activeStep === 3 ? (
            <div className="space-y-5">
              <div className="space-y-1">
                <div className="text-sm font-semibold text-white">Ready for the first scan</div>
                <p className="text-sm text-slate-400">Only the real blockers are shown here. If you picked the local stack, PMDA can create it directly from this step.</p>
              </div>

              {selectedStackMode === 'local' ? (
                <div className="rounded-2xl border border-primary/20 bg-primary/10 p-4 text-sm text-slate-200">
                  <div className="font-medium text-white">Local metadata mode has two separate tracks.</div>
                  <p className="mt-2 text-xs leading-5 text-slate-300">
                    This step only chooses the <span className="font-medium text-white">local preset</span>. When you continue, PMDA asks for confirmation before creating or adopting MusicBrainz + Ollama.
                    Step 4 then becomes the live progress view for provisioning and model downloads.
                  </p>
                </div>
              ) : null}

              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Workflow</div>
                  <div className="mt-2 text-sm font-semibold text-white">{workflowMeta.label}</div>
                  <p className="mt-1 text-xs text-slate-400">{workflowMeta.whenToUse}</p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Metadata</div>
                  <div className="mt-2 text-sm font-semibold text-white">{metadataLabel}</div>
                  <p className="mt-1 text-xs text-slate-400">
                    {selectedStackMode === 'local' ? 'PMDA will use a local MusicBrainz mirror and local Ollama runtime.' : 'PMDA will use the online metadata preset.'}
                  </p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">First scan</div>
                  <div className="mt-2 text-sm font-semibold text-white">{firstScanLabel}</div>
                  <p className="mt-1 text-xs text-slate-400">
                    {scanProgress?.scanning ? `Current phase: ${scanProgress.phase || 'running'}` : Boolean(bootstrap?.bootstrap_required) ? 'PMDA still needs its first full scan.' : 'PMDA already has scan history.'}
                  </p>
                </div>
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
                        <Badge variant={localRuntimeReady ? 'default' : localStackHasStarted ? 'secondary' : 'outline'}>
                          {localRuntimeReady ? 'Local stack ready' : localStackHasStarted ? 'Provisioning in progress' : 'Not started yet'}
                        </Badge>
                        {managedPreflightReady ? <Badge variant="outline">Docker reachable</Badge> : <Badge variant="destructive">Docker not ready</Badge>}
                      </div>
                    </div>
                    {!localRuntimeReady ? (
                      <div className="flex flex-wrap items-center gap-2">
                        <Button type="button" variant="outline" size="sm" onClick={() => void refreshStatus()} disabled={statusLoading}>
                          {statusLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                          Refresh status
                        </Button>
                        <Button type="button" variant="outline" size="sm" onClick={() => openAdvancedSection('settings-scaling')}>
                          Open advanced settings
                        </Button>
                      </div>
                    ) : (
                      <Badge className="bg-success text-success-foreground">Local stack ready</Badge>
                    )}
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-xs text-slate-400">
                      <span>
                        {localRuntimeReady
                          ? 'All local services are ready'
                          : localStackHasStarted
                            ? 'Local stack creation progress'
                            : 'Nothing has started yet'}
                      </span>
                      <span>{localRuntimeReady ? '100%' : `${localStackProgress}%`}</span>
                    </div>
                    <Progress value={Math.max(0, Math.min(100, localStackProgress))} className="h-2 bg-white/10" />
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-black/10 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">Runtime status</div>
                        <p className="mt-1 text-xs text-slate-400">
                          {localStackHasStarted
                            ? 'PMDA is now checking or creating the local services below.'
                            : 'These are the only local services PMDA still needs before the local metadata stack is ready.'}
                        </p>
                      </div>
                      {!localRuntimeReady ? (
                        <Badge variant="outline" className="border-white/15 bg-white/5 text-slate-200">
                          {localStackHasStarted ? 'Provisioning' : 'Idle'}
                        </Badge>
                      ) : null}
                    </div>

                    {localStackHasStarted ? (
                      <div className="mt-3 space-y-3">
                        {localStackActivityRows.map((row) => (
                          <div key={row.key} className="flex items-start justify-between gap-3 rounded-2xl border border-white/10 bg-white/[0.02] px-3 py-3">
                            <div className="min-w-0">
                              <div className="text-sm font-medium text-white">{row.label}</div>
                              <p className="mt-1 text-xs leading-5 text-slate-400">{row.detail}</p>
                            </div>
                            <Badge variant="outline" className="shrink-0 border-white/15 bg-white/5 text-slate-200">
                              {row.status}
                            </Badge>
                          </div>
                        ))}
                      </div>
                    ) : null}

                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                    {localStackChecklist.map((item) => {
                      const itemBadge = item.done ? null : item.progress > 0 ? `${item.progress}%` : (item.detail.toLowerCase().includes('detected') ? 'Detected' : 'Not started');
                      return (
                      <div key={item.key} className="rounded-2xl border border-white/10 bg-white/[0.02] p-3">
                        <div className="flex items-center justify-between gap-2">
                          <div className="text-sm font-medium text-white">{item.label}</div>
                          {item.done ? (
                             <CheckCircle2 className="h-4 w-4 text-success" />
                          ) : (
                            <Badge variant="outline" className="border-white/15 bg-white/5 text-slate-200">
                              {itemBadge}
                            </Badge>
                          )}
                        </div>
                        <p className="mt-2 text-xs leading-5 text-slate-400">{item.detail}</p>
                        {item.progress > 0 || item.done ? (
                          <Progress value={Math.max(0, Math.min(100, item.progress))} className="mt-3 h-1.5 bg-white/10" />
                        ) : (
                          <div className="mt-3 text-[11px] uppercase tracking-[0.14em] text-slate-500">Starts after you confirm local setup</div>
                        )}
                      </div>
                    );})}
                    </div>
                  </div>

                  {localStackServices.length > 0 ? (
                    <div className="rounded-2xl border border-white/10 bg-black/10 p-4">
                      <div className="text-sm font-semibold text-white">Services currently available</div>
                      <p className="mt-1 text-xs text-slate-400">These are the local runtime services PMDA can already see on this server.</p>
                      <div className="mt-3 grid gap-2 md:grid-cols-2">
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
                    </div>
                  ) : null}

                  {localStackRecentLogs.length > 0 ? (
                    <div className="rounded-2xl border border-white/10 bg-black/10 p-4">
                      <div className="text-sm font-semibold text-white">Recent runtime events</div>
                      <p className="mt-1 text-xs text-slate-400">Latest backend events while PMDA is preparing the local stack.</p>
                      <div className="mt-3 space-y-2">
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
                    </div>
                  ) : null}
                </div>
              ) : null}

              {blockers.length > 0 ? (
                <div className="space-y-3 rounded-3xl border border-warning/25 bg-warning/10 p-4">
                  <div className="flex items-center gap-2 text-sm font-semibold text-warning">
                    <AlertTriangle className="h-4 w-4" />
                    Blocking issues
                  </div>
                  <div className="space-y-2">
                    {blockers.map((blocker) => (
                      <div key={blocker.key} className="rounded-2xl border border-warning/20 bg-background/10 p-3">
                        <div className="text-sm font-medium text-warning">{blocker.title}</div>
                        <p className="mt-1 text-xs text-warning/80">{blocker.detail}</p>
                        {!(blocker.key === 'local-runtime' && managedPreflightReady) ? (
                          <Button type="button" variant="outline" size="sm" className="mt-3" onClick={() => openAdvancedSection(blocker.sectionId)}>
                            Open advanced settings
                          </Button>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="rounded-3xl border border-success/25 bg-success/10 p-4">
                  <div className="flex items-center gap-2 text-sm font-semibold text-success">
                    <CheckCircle2 className="h-4 w-4" />
                    No blocking issue detected
                  </div>
                  <p className="mt-2 text-xs text-success/85">
                    PMDA has the minimum configuration required to launch or resume the first full scan.
                  </p>
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
                <Button type="button" className="gap-2" onClick={() => void handleNextStep()} disabled={isSaving || runtimeActionBusy}>
                  {isSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                  {activeStep === 2 && selectedStackMode === 'local' && managedPreflightReady && !localRuntimeReady && !localStackHasStarted ? 'Continue' : 'Next'}
                </Button>
              ) : (
                <Button type="button" className="gap-2" onClick={() => void startOrResumeScan()} disabled={scanActionBusy || blockers.length > 0}>
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
            <p>After confirmation, step 4 becomes the live progress view. You will see Docker checks, MusicBrainz provisioning, Ollama startup, and model downloads there.</p>
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
