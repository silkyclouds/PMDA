import type { ScanProgress } from '@/lib/api';

export type ScanPipelineStepKey =
  | 'pre_scan'
  | 'run_scope'
  | 'format_analysis'
  | 'identification_tags'
  | 'ia_analysis'
  | 'incomplete_move'
  | 'moving_dupes'
  | 'export'
  | 'post_processing'
  | 'finalizing';

export interface ScanPipelineStep {
  key: ScanPipelineStepKey;
  label: string;
  description: string;
  state: 'done' | 'active' | 'pending';
  index: number;
  total: number;
}

function phaseToPipelineKey(
  phase: ScanProgress['phase'],
  progress: Partial<ScanProgress> | null | undefined,
): ScanPipelineStepKey {
  if (phase === 'pre_scan') return 'pre_scan';
  if (phase === 'preparing_run_scope') return 'run_scope';
  if (phase === 'incomplete_move') return 'incomplete_move';
  if (phase === 'format_analysis') return 'format_analysis';
  if (phase === 'identification_tags') return 'identification_tags';
  if (phase === 'ia_analysis') return 'ia_analysis';
  if (phase === 'export') return 'export';
  if (phase === 'moving_dupes') return 'moving_dupes';
  if (phase === 'post_processing' || phase === 'profile_enrichment' || phase === 'background_enrichment') {
    return 'post_processing';
  }
  if (phase === 'finalizing') return 'finalizing';
  return 'pre_scan';
}

export function buildScanPipelineSteps(
  progress: Partial<ScanProgress> | null | undefined,
): {
  steps: ScanPipelineStep[];
  currentIndex: number;
  total: number;
  currentLabel: string;
} {
  const flags = (progress?.scan_pipeline_flags || {}) as Record<string, unknown>;
  const phase = progress?.phase ?? null;
  const runScopePreparing = Boolean(progress?.scan_run_scope_preparing) || phase === 'preparing_run_scope';
  const runScopeStage = String(progress?.scan_run_scope_stage || '').trim().toLowerCase();
  const runScopeTotal = Math.max(0, Number(progress?.scan_run_scope_total || 0));
  const runScopeDone = Math.max(0, Number(progress?.scan_run_scope_done || 0));
  const hasRunScope =
    runScopePreparing
    || (
      runScopeStage !== ''
      && runScopeStage !== 'idle'
      && runScopeStage !== 'done'
      && runScopeTotal > 0
      && runScopeDone < runScopeTotal
    );
  const hasAi = phase === 'ia_analysis' || Boolean(progress?.ai_enabled) || Number(progress?.scan_ai_batch_total || 0) > 0;
  const hasIncompleteMove = Boolean(flags.incomplete_move);
  const hasDedupe = Boolean(flags.dedupe) || Boolean(progress?.auto_move_enabled) || phase === 'moving_dupes';
  const hasExport = Boolean(flags.export);
  const postProcessingDone = Math.max(0, Number(progress?.post_processing_done || 0));
  const postProcessingTotal = Math.max(0, Number(progress?.post_processing_total || 0));
  const hasPostProcessing =
    phase === 'post_processing'
    || phase === 'profile_enrichment'
    || phase === 'background_enrichment';
  const postProcessingActive =
    phase === 'post_processing'
    || phase === 'profile_enrichment'
    || phase === 'background_enrichment'
    || Boolean(progress?.post_processing)
    || (postProcessingTotal > 0 && postProcessingDone < postProcessingTotal);

  const blueprint: Array<{ key: ScanPipelineStepKey; label: string; description: string; enabled: boolean }> = [
    { key: 'pre_scan', label: 'Pre-scan', description: 'Discover monitored folders, warm caches, and estimate the real scan scope.', enabled: true },
    { key: 'run_scope', label: 'Prepare scope', description: 'Compute the effective work set for resumed or incremental runs.', enabled: hasRunScope },
    { key: 'format_analysis', label: 'Format analysis', description: 'Read formats, durations, and technical audio properties.', enabled: true },
    { key: 'identification_tags', label: 'Identification & tags', description: 'Match albums against MusicBrainz and providers, then verify tags.', enabled: true },
    { key: 'ia_analysis', label: 'AI analysis', description: 'Use AI only for ambiguous candidates that deterministic checks could not settle.', enabled: hasAi },
    { key: 'incomplete_move', label: 'Quarantine incompletes', description: 'Move incomplete albums into a reviewable quarantine.', enabled: hasIncompleteMove },
    { key: 'moving_dupes', label: 'Move dupes', description: 'Move duplicate losers away from the clean serving library.', enabled: hasDedupe },
    { key: 'export', label: 'Build library', description: 'Materialize clean winners into the PMDA-built library.', enabled: hasExport },
    { key: 'post_processing', label: 'Post-processing', description: 'Finish metadata, profiles, and artwork after the main scan work.', enabled: hasPostProcessing || postProcessingActive },
    { key: 'finalizing', label: 'Finalizing', description: 'Write summaries, settle queues, and close the run cleanly.', enabled: true },
  ];

  const enabledSteps = blueprint.filter((step) => step.enabled);
  const currentKey = phaseToPipelineKey(phase, progress);
  const currentIndexRaw = enabledSteps.findIndex((step) => step.key === currentKey);
  const currentIndex = currentIndexRaw >= 0 ? currentIndexRaw : 0;
  const total = enabledSteps.length;
  const steps: ScanPipelineStep[] = enabledSteps.map((step, index) => ({
    key: step.key,
    label: step.label,
    description: step.description,
    state: index < currentIndex ? 'done' : index === currentIndex ? 'active' : 'pending',
    index: index + 1,
    total,
  }));

  return {
    steps,
    currentIndex: currentIndex + 1,
    total,
    currentLabel: enabledSteps[currentIndex]?.label || enabledSteps[0]?.label || 'Scan',
  };
}
