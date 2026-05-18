import type { ScanProgress, ScalingRuntimeResponse } from '@/lib/api';
import { buildScanPipelineSteps } from '@/lib/scanPipeline';

function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function safeNumber(value: unknown, fallback: number = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function deriveHoursRate(done: number, elapsedSeconds: number | null | undefined): number | null {
  const elapsed = safeNumber(elapsedSeconds, 0);
  if (elapsed <= 0 || done <= 0) return null;
  return (done * 3600) / elapsed;
}

export interface ScanPresentationModel {
  pipeline: ReturnType<typeof buildScanPipelineSteps>;
  pipelineOverallDoneSteps: number;
  pipelineOverallPercent: number;
  pipelineHeadline: string;
  pipelineProgressLabel: string;
  currentStageLabel: string;
  currentStageProgressLabel: string;
  currentStagePercent: number;
  currentStagePercentLabel: string;
  stageHeroLabel: string;
  progressMode: string;
  etaConfidence: string;
  humanExplanation: string;
  whySteps: string[];
  albumsProcessed: number;
  albumsProcessedLabel: string;
  artistsProcessed: number;
  artistsProcessedLabel: string;
  activeArtistsCount: number;
  matchesSoFar: number;
  exportsSoFar: number;
  incompleteAlbumsSoFar: number;
  duplicateLosersSoFar: number;
  providerMatches: Record<string, number>;
  albumsPerHour: number | null;
  artistsPerHour: number | null;
}

export function buildScanPresentationModel(
  progress: Partial<ScanProgress> | null | undefined,
  scalingRuntime?: ScalingRuntimeResponse | null,
): ScanPresentationModel {
  const data = progress || {};
  const pipeline = buildScanPipelineSteps(data);
  const currentStepLabel = String(
    data.pipeline_step_human_label
    || data.current_stage_human_label
    || pipeline.currentLabel
    || 'Scan step',
  ).trim();
  const currentStageLabel = String(
    data.current_stage_human_label
    || data.pipeline_step_human_label
    || pipeline.currentLabel
    || 'Current stage',
  ).trim();
  const stageProgressDone = Math.max(0, safeNumber(data.stage_progress_done, 0));
  const stageProgressTotal = Math.max(0, safeNumber(data.stage_progress_total, 0));
  const stageProgressUnit = String(data.stage_progress_unit || 'items').trim() || 'items';
  const finalizingItemDone = Math.max(0, safeNumber(data.scan_finalizing_item_done, 0));
  const finalizingItemTotal = Math.max(0, safeNumber(data.scan_finalizing_item_total, 0));
  const finalizingItemLabel = String(data.scan_finalizing_item_label || '').trim();
  const finalizingStepDone = Math.max(0, safeNumber(data.scan_finalizing_done, 0));
  const finalizingStepTotal = Math.max(0, safeNumber(data.scan_finalizing_total, 0));
  const isFinalizing = Boolean(data.finalizing) || data.phase === 'finalizing' || data.scan_progress_mode === 'finalizing';
  const stageProgressPercent = clampPercent(safeNumber(data.stage_progress_percent, 0));
  const pipelineStageFraction = stageProgressTotal > 0
    ? clampPercent(stageProgressPercent) / 100
    : 0;
  const pipelineOverallDoneSteps = pipeline.total > 0
    ? Math.max(0, Math.min(pipeline.total, (pipeline.currentIndex - 1) + pipelineStageFraction))
    : 0;
  const pipelineOverallPercent = pipeline.total > 0
    ? clampPercent((pipelineOverallDoneSteps / pipeline.total) * 100)
    : clampPercent(safeNumber(data.overall_progress_percent, 0));

  const currentStageProgressLabel = finalizingItemTotal > 0
    ? `${finalizingItemLabel ? `${finalizingItemLabel} · ` : ''}${finalizingItemDone.toLocaleString()} / ${finalizingItemTotal.toLocaleString()} items`
    : isFinalizing && finalizingStepTotal > 0
    ? `${Math.min(finalizingStepDone + 1, finalizingStepTotal).toLocaleString()} / ${finalizingStepTotal.toLocaleString()} tasks`
    : stageProgressTotal > 0
    ? `${stageProgressDone.toLocaleString()} / ${stageProgressTotal.toLocaleString()} ${stageProgressUnit}`
    : isFinalizing
    ? 'Finalizing current task…'
    : 'Waiting for measurable work…';
  const currentStagePercentLabel = stageProgressTotal > 0
    ? `${stageProgressPercent.toFixed(stageProgressPercent >= 10 ? 0 : 2)}% of ${currentStepLabel}`
    : `Current stage: ${currentStepLabel}`;
  const stageHeroLabel = stageProgressTotal > 0
    ? `Current stage: ${currentStageProgressLabel}`
    : currentStageLabel;
  const pipelineHeadline = `Step ${pipeline.currentIndex} of ${pipeline.total} — ${currentStepLabel}`;
  const pipelineProgressLabel = pipeline.total > 0
    ? `${pipelineOverallDoneSteps.toFixed(1)} / ${pipeline.total} steps · ${pipelineOverallPercent.toFixed(1)}% of configured scan pipeline`
    : 'Pipeline progress unavailable';

  const progressMode = String(data.scan_progress_mode || (data.phase === 'pre_scan' ? 'preparing' : 'stage_active'));
  const etaConfidence = String(data.scan_eta_confidence || 'medium');
  let humanExplanation = 'PMDA is working through the current scan pipeline.';
  if (progressMode === 'preparing') {
    humanExplanation = 'PMDA is still discovering or restoring the true scan scope. Early percentages and ETA are provisional.';
  } else if (data.phase === 'identification_tags') {
    humanExplanation = 'PMDA has entered the matching phase. ETA is now based on real album throughput through MusicBrainz and provider lookups.';
  } else if (data.phase === 'export') {
    humanExplanation = 'Matching is done for the current batch. PMDA is now materializing clean winners into the PMDA-built library.';
  } else if (progressMode === 'finalizing' || data.phase === 'finalizing') {
    humanExplanation = 'The album work is done. PMDA is writing summaries and finishing trailing background work.';
  } else if (data.phase === 'moving_dupes') {
    humanExplanation = 'PMDA is moving duplicate losers out of the clean library path while preserving the chosen winner.';
  } else if (data.phase === 'incomplete_move') {
    humanExplanation = 'PMDA is quarantining incomplete albums so they do not pollute the clean library.';
  }

  const whySteps: string[] = [];
  if (Boolean(data.ai_enabled) || safeNumber(data.scan_ai_batch_total, 0) > 0) {
    whySteps.push('AI analysis is enabled for ambiguous cases.');
  }
  if (Boolean(data.scan_pipeline_flags?.incomplete_move)) {
    whySteps.push('Incomplete quarantine is enabled.');
  }
  if (Boolean(data.scan_pipeline_flags?.dedupe) || Boolean(data.auto_move_enabled)) {
    whySteps.push('Duplicate moving is enabled.');
  }
  if (Boolean(data.scan_pipeline_flags?.export)) {
    whySteps.push('Library export is enabled.');
  }
  if (Boolean(data.post_processing) || safeNumber(data.post_processing_total, 0) > 0) {
    whySteps.push('Post-processing is active for metadata, artwork, or profile enrichment.');
  }
  if (Boolean(data.scan_run_scope_preparing) || safeNumber(data.scan_run_scope_total, 0) > 0) {
    whySteps.push('Run-scope preparation is active for resume or incremental filtering.');
  }

  const providerMatches: Record<string, number> = {};
  for (const [provider, rawValue] of Object.entries(data.provider_matches_so_far || {})) {
    const key = String(provider || '').trim().toLowerCase();
    if (!key) continue;
    providerMatches[key] = Math.max(0, safeNumber(rawValue, 0));
  }
  providerMatches.discogs = Math.max(0, safeNumber(providerMatches.discogs ?? data.scan_discogs_matched, 0));
  providerMatches.lastfm = Math.max(0, safeNumber(providerMatches.lastfm ?? data.scan_lastfm_matched, 0));
  providerMatches.bandcamp = Math.max(0, safeNumber(providerMatches.bandcamp ?? data.scan_bandcamp_matched, 0));
  const providerMatchTotal = Object.values(providerMatches).reduce((sum, value) => sum + Math.max(0, safeNumber(value, 0)), 0);
  const matchesSoFar = Math.max(0, safeNumber(data.matches_so_far, providerMatchTotal));
  const exportsSoFar = Math.max(0, safeNumber(data.exports_so_far, data.scan_published_albums_count));
  const incompleteAlbumsSoFar = Math.max(0, safeNumber(data.incomplete_albums_so_far, data.broken_albums_count));
  const duplicateLosersSoFar = Math.max(0, safeNumber(data.duplicate_losers_so_far, data.total_duplicates_count));
  const committedAlbumsProcessed = Math.max(0, safeNumber(data.scan_processed_albums_count, 0));
  const committedArtistsProcessed = Math.max(0, safeNumber(data.artists_processed, 0));
  const preScanAlbumsDiscovered = Math.max(
    0,
    safeNumber(data.scan_preplan_done, 0),
    safeNumber(data.scan_discovery_albums_done, 0),
    safeNumber(data.scan_discovery_folders_done, 0),
  );
  const preScanArtistsDiscovered = Math.max(
    0,
    safeNumber(data.detected_artists_total, 0),
    safeNumber(data.scan_discovery_artists_found, 0),
  );
  let albumsProcessed = Math.max(
    committedAlbumsProcessed,
    safeNumber(data.scan_processed_albums_effective, 0),
  );
  let artistsProcessed = Math.max(
    committedArtistsProcessed,
    safeNumber(data.artists_processed_effective, 0),
  );
  let albumsProcessedLabel = albumsProcessed > committedAlbumsProcessed ? 'Albums advanced' : 'Albums processed';
  let artistsProcessedLabel = artistsProcessed > committedArtistsProcessed ? 'Artists advanced' : 'Artists processed';
  if (String(data.phase || '') === 'pre_scan' || String(data.scan_progress_mode || '') === 'preparing') {
    if (preScanAlbumsDiscovered > albumsProcessed) {
      albumsProcessed = preScanAlbumsDiscovered;
      albumsProcessedLabel = 'Albums discovered';
    }
    if (preScanArtistsDiscovered > artistsProcessed) {
      artistsProcessed = preScanArtistsDiscovered;
      artistsProcessedLabel = 'Artists discovered';
    }
  }
  const activeArtistsCount = Math.max(0, safeNumber(data.active_artists_count, Array.isArray(data.active_artists) ? data.active_artists.length : 0));
  const elapsedSeconds = safeNumber(data.elapsed_seconds ?? data.scan_runtime_sec, 0) || null;
  const activeAlbumWork = Array.isArray(data.active_artists)
    ? data.active_artists.reduce((sum, artist) => sum + Math.max(0, safeNumber((artist as { albums_processed?: number })?.albums_processed, 0)), 0)
    : 0;
  const stageRatePerHour = safeNumber(data.phase_rate, 0) > 0 ? safeNumber(data.phase_rate, 0) * 3600 : null;
  const throughputAlbumProxy = Math.max(
    albumsProcessed,
    Math.max(0, safeNumber(data.stage_progress_done, 0)),
    committedAlbumsProcessed + activeAlbumWork,
  );
  const albumsPerHour = scalingRuntime?.stage_rates?.albums_processed_per_hour || stageRatePerHour || deriveHoursRate(throughputAlbumProxy, elapsedSeconds);
  const artistsPerHour = scalingRuntime?.stage_rates?.artists_processed_per_hour || deriveHoursRate(artistsProcessed, elapsedSeconds);

  return {
    pipeline,
    pipelineOverallDoneSteps,
    pipelineOverallPercent,
    pipelineHeadline,
    pipelineProgressLabel,
    currentStageLabel,
    currentStageProgressLabel,
    currentStagePercent: stageProgressPercent,
    currentStagePercentLabel,
    stageHeroLabel,
    progressMode,
    etaConfidence,
    humanExplanation,
    whySteps,
    albumsProcessed,
    albumsProcessedLabel,
    artistsProcessed,
    artistsProcessedLabel,
    activeArtistsCount,
    matchesSoFar,
    exportsSoFar,
    incompleteAlbumsSoFar,
    duplicateLosersSoFar,
    providerMatches,
    albumsPerHour,
    artistsPerHour,
  };
}
