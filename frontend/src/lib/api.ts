// PMDA API Client
// Empty = same origin (e.g. when served from the same Docker container). Set VITE_PMDA_API_URL for dev (must be full URL, e.g. http://192.168.3.2:5005).
const _raw = import.meta.env.VITE_PMDA_API_URL ?? '';
const API_BASE_URL = typeof _raw === 'string' && (_raw.startsWith('http://') || _raw.startsWith('https://')) ? _raw : '';
const AUTH_TOKEN_STORAGE_KEY = 'pmda_auth_token';
const AUTH_TOKEN_SESSION_STORAGE_KEY = 'pmda_auth_token_session';

export function getAuthToken(): string {
  try {
    const sessionToken = (window.sessionStorage.getItem(AUTH_TOKEN_SESSION_STORAGE_KEY) || '').trim();
    if (sessionToken) return sessionToken;
    return (window.localStorage.getItem(AUTH_TOKEN_STORAGE_KEY) || '').trim();
  } catch {
    return '';
  }
}

export function setAuthToken(token: string, remember = true): void {
  try {
    const clean = String(token || '').trim();
    window.localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
    window.sessionStorage.removeItem(AUTH_TOKEN_SESSION_STORAGE_KEY);
    if (!clean) {
      return;
    }
    if (remember) {
      window.localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, clean);
    } else {
      window.sessionStorage.setItem(AUTH_TOKEN_SESSION_STORAGE_KEY, clean);
    }
  } catch {
    // ignore
  }
}

export function clearAuthToken(): void {
  try {
    window.localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
    window.sessionStorage.removeItem(AUTH_TOKEN_SESSION_STORAGE_KEY);
  } catch {
    // ignore
  }
}

export function normalizePmdaAssetUrl(value?: string | null): string {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const apiIdx = raw.indexOf('/api/');
  if (apiIdx > 0) {
    return raw.slice(apiIdx);
  }
  return raw;
}

function normalizePmdaPayloadUrls<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((item) => normalizePmdaPayloadUrls(item)) as T;
  }
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, inner] of Object.entries(value as Record<string, unknown>)) {
      out[key] = normalizePmdaPayloadUrls(inner);
    }
    return out as T;
  }
  if (typeof value === 'string') {
    return normalizePmdaAssetUrl(value) as T;
  }
  return value;
}

function withAuthHeaders(input?: HeadersInit): HeadersInit {
  const token = getAuthToken();
  const headers = new Headers(input || undefined);
  if (token && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  return headers;
}

let authFetchInterceptorInstalled = false;

/** Attach Authorization bearer token to legacy direct fetch() calls hitting PMDA API routes. */
export function installGlobalAuthFetchInterceptor(): void {
  if (authFetchInterceptorInstalled || typeof window === 'undefined' || typeof window.fetch !== 'function') {
    return;
  }
  const nativeFetch = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const requestUrl = typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url;
    const isApiRequest = requestUrl.startsWith('/api/')
      || requestUrl.startsWith('/scan/')
      || requestUrl.startsWith('/dedupe/')
      || requestUrl.startsWith('/details/')
      || (API_BASE_URL ? requestUrl.startsWith(`${API_BASE_URL}/api/`) : false)
      || (API_BASE_URL ? requestUrl.startsWith(`${API_BASE_URL}/scan/`) : false)
      || (API_BASE_URL ? requestUrl.startsWith(`${API_BASE_URL}/dedupe/`) : false)
      || (API_BASE_URL ? requestUrl.startsWith(`${API_BASE_URL}/details/`) : false);
    if (!isApiRequest) {
      return nativeFetch(input, init);
    }
    const inheritedHeaders =
      init?.headers
      ?? (input instanceof Request ? input.headers : undefined);
    const headers = new Headers(withAuthHeaders(inheritedHeaders));
    return nativeFetch(input, { ...init, headers }).then((response) => {
      const originalJson = response.json.bind(response);
      (response as Response & { json: typeof response.json }).json = async () => {
        const payload = await originalJson();
        return normalizePmdaPayloadUrls(payload);
      };
      return response;
    });
  };
  authFetchInterceptorInstalled = true;
}

async function fetchWithAuth(endpoint: string, init?: RequestInit): Promise<Response> {
  return fetch(`${API_BASE_URL}${endpoint}`, {
    ...init,
    headers: withAuthHeaders(init?.headers),
  });
}

// Types
export interface DuplicateCard {
  artist_key: string;
  artist: string;
  album_id: string;
  best_thumb: string;
  best_title: string;
  best_fmt: string;
  formats: string[];
  n: number;
  used_ai: boolean;
  /** AI provider name when used_ai (e.g. OpenAI, Anthropic). */
  ai_provider?: string;
  /** AI model name when used_ai (e.g. gpt-4o-mini). */
  ai_model?: string;
  // New fields for detailed view
  size?: number;
  track_count?: number;
  path?: string;
  br?: number;
  sr?: number;
  bd?: number;
  /** True when group is from Library (same name) but scan has no best/loser — run scan to dedupe */
  no_move?: boolean;
  /** True when MusicBrainz match was chosen by AI verify (USE_AI_FOR_MB_VERIFY). */
  match_verified_by_ai?: boolean;
}

export interface ScanResumeSnapshot {
  available: boolean;
  run_id: string;
  status: string;
  scan_type: 'full' | 'changed_only';
  created_at?: number | null;
  updated_at?: number | null;
  scan_id?: number | null;
  total_artists: number;
  total_albums: number;
  done_artists: number;
  done_albums: number;
  remaining_artists: number;
  remaining_albums: number;
  pending_artists: number;
  pending_albums: number;
  running_artists: number;
  running_albums: number;
  failed_artists: number;
  failed_albums: number;
  detected_artists_total?: number;
  detected_albums_total?: number;
  detected_tracks_total?: number;
  plan_snapshot_ready?: boolean;
}

export interface ScanProgress {
  scan_id?: number | null;
  scanning: boolean;
  scan_starting?: boolean;
  library_ready?: boolean;
  background_enrichment_running?: boolean;
  background_jobs?: Array<{
    run_id: string;
    job_type: string;
    scope: string;
    source: string;
    origin_scan_id?: number | null;
    started_at?: number | null;
  }>;
  profile_backfill?: {
    running?: boolean;
    reason?: string;
    started_at?: number | null;
    finished_at?: number | null;
    current?: number;
    total?: number;
    current_artist?: string;
    errors?: number;
  } | null;
  progress: number;
  total: number;
  /** Progress including in-progress artist albums (so bar moves during scan) */
  effective_progress?: number;
  status: 'running' | 'paused' | 'stopped' | 'idle';
  resume_available?: boolean;
  resume_available_by_scan_type?: Partial<Record<'full' | 'changed_only', ScanResumeSnapshot>>;
  scan_type?: 'full' | 'changed_only' | 'incomplete_only';
  scan_resume_run_id?: string | null;
  /** Current scan phase: pre_scan | preparing_run_scope | format_analysis | identification_tags | ia_analysis | finalizing | moving_dupes | post_processing | profile_enrichment | background_enrichment */
  phase?: 'pre_scan' | 'preparing_run_scope' | 'incomplete_move' | 'format_analysis' | 'identification_tags' | 'ia_analysis' | 'moving_dupes' | 'export' | 'post_processing' | 'profile_enrichment' | 'background_enrichment' | 'finalizing' | null;
  /** Micro-step for live indicators: analyzing_format | fetching_mb_id | searching_mb | comparing_versions | detecting_best | done */
  current_step?: string | null;
  scan_progress_mode?: 'preparing' | 'stage_active' | 'finalizing' | string;
  scan_eta_confidence?: 'low' | 'medium' | 'high' | string;
  pipeline_step_human_label?: string | null;
  current_stage_human_label?: string | null;
  /** Total albums in this scan (for N/M display in findings) */
  total_albums?: number;
  /** AI provider name (e.g. OpenAI) when AI is used */
  ai_provider?: string;
  /** AI model name (e.g. gpt-4o-mini) when AI is used */
  ai_model?: string;
  /** Per-stage progress counters for the currently active phase. */
  stage_progress_done?: number;
  stage_progress_total?: number;
  stage_progress_percent?: number;
  stage_progress_unit?: 'steps' | 'roots' | 'albums' | 'artists' | 'groups' | 'jobs' | 'tasks' | string;
  /** Overall progress counters for the full run, separate from the current stage. */
  overall_progress_done?: number;
  overall_progress_total?: number;
  overall_progress_percent?: number;
  // Scan details
  artists_processed?: number;
  artists_total?: number;
  /** Artists discovered from source before resume/incremental filtering. */
  detected_artists_total?: number;
  /** Albums discovered from source before resume/incremental filtering. */
  detected_albums_total?: number;
  /** Artists skipped by resume (already done/unchanged). */
  resume_skipped_artists?: number;
  /** Albums skipped by resume (already done/unchanged). */
  resume_skipped_albums?: number;
  /** True while resume filtering is preparing the effective run scope. */
  scan_run_scope_preparing?: boolean;
  /** Current sub-stage of run-scope preparation. */
  scan_run_scope_stage?: 'idle' | 'signatures' | 'resume_compare' | 'resume_seed' | 'done' | string | null;
  /** Progress counter for run-scope preparation stage. */
  scan_run_scope_done?: number;
  /** Total artists for run-scope preparation stage. */
  scan_run_scope_total?: number;
  /** Percent completion for run-scope preparation stage. */
  scan_run_scope_percent?: number;
  /** Artists included in effective run scope after resume filtering. */
  scan_run_scope_artists_included?: number;
  /** Albums included in effective run scope after resume filtering. */
  scan_run_scope_albums_included?: number;
  ai_used_count?: number;
  mb_used_count?: number;
  ai_enabled?: boolean;
  mb_enabled?: boolean;
  // ETA
  eta_seconds?: number;
  threads_in_use?: number;
  active_artists?: Array<{
    artist_name: string;
    total_albums: number;
    albums_processed: number;
    current_album?: {
      album_id: number;
      album_title: string;
      status: string;
      status_details: string;
      /** Low-level step summary (action). */
      step_summary?: string;
      /** Tool response (FFprobe/MusicBrainz/AI result). */
      step_response?: string;
    };
  }>;
  /** Albums that completed format (FFprobe) step — for 33% progress during phase duplicates */
  format_done_count?: number;
  /** Albums that completed MusicBrainz lookup — for 33% progress during phase duplicates */
  mb_done_count?: number;
  // Cache statistics
  audio_cache_hits?: number;
  audio_cache_misses?: number;
  mb_cache_hits?: number;
  mb_cache_misses?: number;
  // Detailed statistics
  duplicate_groups_count?: number;
  total_duplicates_count?: number;
  broken_albums_count?: number;
  missing_albums_count?: number;
  albums_without_artist_image?: number;
  albums_without_album_image?: number;
  albums_without_complete_tags?: number;
  albums_without_mb_id?: number;
  albums_without_artist_mb_id?: number;
  /** Re-check MusicBrainz for albums previously not found (from Settings) */
  mb_retry_not_found?: boolean;
  /** End-of-scan summary (FFmpeg formats, MusicBrainz, AI) when scan just completed */
  last_scan_summary?: LastScanSummary | null;
  /** Saving results to DB (Unduper + stats); scan not "done" until this is false */
  finalizing?: boolean;
  /** Auto-move dupes is enabled (show step 4 when applicable) */
  auto_move_enabled?: boolean;
  /** Currently moving duplicate folders to /dupes */
  deduping?: boolean;
  dedupe_progress?: number;
  dedupe_total?: number;
  dedupe_current_group?: { artist: string; album: string; num_dupes?: number; winner?: { title_raw?: string }; losers?: Array<{ title_raw?: string }>; destination?: string; status?: string } | null;
  /** Paths RW status from backend (music + dupes folders) */
  paths_status?: { music_rw: boolean; dupes_rw: boolean };
  /** IA analysis step: total duplicate groups to process */
  scan_ai_batch_total?: number;
  /** IA analysis step: groups processed so far */
  scan_ai_batch_processed?: number;
  /** IA analysis step: artist – album of the group currently or last processed */
  scan_ai_current_label?: string | null;
  /** Rolling per-artist log of steps executed during the current scan (for activity log UI). */
  scan_steps_log?: string[];
  /** True when post-scan/post-artist metadata fixing is still running. */
  post_processing?: boolean;
  post_processing_done?: number;
  post_processing_total?: number;
  post_processing_current_artist?: string | null;
  post_processing_current_album?: string | null;
  /** Files-mode discovery counters before full artist plan is ready */
  scan_discovery_running?: boolean;
  scan_discovery_current_root?: string | null;
  scan_discovery_roots_done?: number;
  scan_discovery_roots_total?: number;
  scan_discovery_files_found?: number;
  scan_discovery_folders_found?: number;
  scan_discovery_albums_found?: number;
  scan_discovery_artists_found?: number;
  scan_discovery_stage?: 'idle' | 'filesystem' | 'album_candidates' | 'ready' | 'cancelled' | string | null;
  scan_discovery_entries_scanned?: number;
  scan_discovery_root_entries_scanned?: number;
  scan_discovery_folders_done?: number;
  scan_discovery_folders_total?: number;
  scan_discovery_albums_done?: number;
  scan_discovery_albums_total?: number;
  scan_discovery_started_at?: number | null;
  scan_discovery_updated_at?: number | null;
  scan_preplan_done?: number;
  scan_preplan_total?: number;
  scan_preplan_percent?: number;
  scan_preplan_label?: string | null;
  scan_prescan_cache_snapshot_running?: boolean;
  scan_prescan_cache_snapshot_done?: boolean;
  scan_prescan_cache_snapshot_rows?: number;
  scan_prescan_cache_snapshot_total?: number;
  scan_prescan_cache_snapshot_updated_at?: number | null;
  scan_published_catchup_running?: boolean;
  scan_published_catchup_reason?: string | null;
  scan_published_catchup_done?: number;
  scan_published_catchup_total?: number;
  scan_published_catchup_ok?: number;
  scan_published_catchup_failed?: number;
  scan_published_catchup_current_artist?: string | null;
  scan_published_catchup_started_at?: number | null;
  scan_published_catchup_updated_at?: number | null;
  scan_published_catchup_finished_at?: number | null;
  scan_discogs_matched?: number;
  scan_lastfm_matched?: number;
  scan_bandcamp_matched?: number;
  provider_matches_so_far?: Partial<Record<'discogs' | 'lastfm' | 'bandcamp', number>>;
  matches_so_far?: number;
  exports_so_far?: number;
  incomplete_albums_so_far?: number;
  duplicate_losers_so_far?: number;
  active_artists_count?: number;
  /** AI guardrail counters for the currently running scan. */
  scan_ai_guard_calls_used?: number;
  scan_ai_guard_calls_blocked?: number;
  scan_ai_guard_last_reason?: string;
  scan_ai_guard_last_block_at?: number | null;
  scan_start_time?: number | null;
  /** Pipeline toggles effectively applied for this running scan. */
  scan_pipeline_flags?: {
    match_fix?: boolean;
    dedupe?: boolean;
    incomplete_move?: boolean;
    export?: boolean;
    player_sync?: boolean;
    sync_target?: string;
  };
  /** When true, post-scan pipeline work is queued in background jobs. */
  scan_pipeline_async?: boolean;
  /** Sync target resolved for this scan. */
  scan_pipeline_sync_target?: string | null;
  /** Number of incomplete albums auto-moved in this scan. */
  scan_incomplete_moved_count?: number;
  /** Total size moved for incompletes (MB) in this scan. */
  scan_incomplete_moved_mb?: number;
  scan_incomplete_move_running?: boolean;
  scan_incomplete_move_done?: number;
  scan_incomplete_move_total?: number;
  scan_incomplete_move_current_album?: {
    artist?: string;
    album?: string;
    path?: string;
  } | null;
  export_progress?: {
    running?: boolean;
    tracks_done?: number;
    total_tracks?: number;
    albums_done?: number;
    total_albums?: number;
    error?: string | null;
  } | null;
  export_running?: boolean;
  export_albums_done?: number;
  export_albums_total?: number;
  export_tracks_done?: number;
  export_tracks_total?: number;
  /** Tracks detected during pre-scan (filesystem walk). */
  scan_tracks_detected_total?: number;
  /** Tracks currently kept in live library index (files_tracks). */
  scan_tracks_library_kept?: number;
  /** Tracks moved by dedupe during this run. */
  scan_tracks_moved_dupes?: number;
  /** Tracks moved by incomplete-album quarantine during this run. */
  scan_tracks_moved_incomplete?: number;
  /** Residual detected tracks not accounted in kept/moved buckets. */
  scan_tracks_unaccounted?: number;
  /** Albums whose per-artist scan worker completed successfully during this run. */
  scan_processed_albums_count?: number;
  /** Albums already published into the live Files library during this run. */
  scan_published_albums_count?: number;
  /** Raw rows written to the publication table before browse/index filtering settles. */
  scan_published_album_rows_count?: number;
  /** Albums already passed through streamed post-processing during this run. */
  scan_postprocessed_albums_count?: number;
  /** Player sync telemetry. */
  scan_player_sync_target?: string | null;
  scan_player_sync_ok?: boolean | null;
  scan_player_sync_message?: string | null;
  /** Bootstrap/autonomy state for scan defaults. */
  bootstrap_required?: boolean;
  autonomous_mode?: boolean;
  has_completed_full_scan?: boolean;
  default_scan_type?: 'full' | 'changed_only' | 'incomplete_only' | string;
  /** Live timing metrics. */
  elapsed_seconds?: number | null;
  scan_runtime_sec?: number | null;
  phase_rate?: number | null;
  phase_progress?: number | null;
  library_visible_albums_count?: number | null;
  library_visible_artists_count?: number | null;
}

export interface CacheControlMetrics {
  generated_at: number;
  cache_policies: {
    scan_disable_cache: boolean;
    mb_disable_cache: boolean;
  };
  runtime: {
    library_mode: string;
    process_rss_bytes: number;
    container_memory: {
      current_bytes: number;
      limit_bytes: number;
      used_pct?: number | null;
    };
  };
  redis: {
    available: boolean;
    mode?: string;
    reason?: string;
    last_error?: string;
    last_ok_ts?: number;
    local_cache_enabled?: boolean;
    local_cache_keys?: number;
    host: string;
    port: number;
    db: number;
    db_keys: number;
    pmda_prefix_keys: number;
    pmda_prefix_scan_truncated: boolean;
    used_memory_bytes: number;
    used_memory_peak_bytes: number;
    maxmemory_bytes: number;
    evicted_keys: number;
    keyspace_hits: number;
    keyspace_misses: number;
    keyspace_hit_rate_pct?: number | null;
  };
  postgres: {
    available: boolean;
    mode?: string;
    reason?: string;
    last_error?: string;
    last_ok_ts?: number;
    db_size_bytes: number;
    db_cache_hit_rate_pct?: number | null;
    numbackends: number;
    table_estimated_rows: Record<string, number>;
    table_total_bytes: Record<string, number>;
  };
  sqlite_cache_db: {
    db_path: string;
    db_bytes: number;
    wal_bytes: number;
    shm_bytes: number;
    audio_cache_rows: number;
    musicbrainz_cache_rows: number;
    musicbrainz_album_lookup_rows: number;
    musicbrainz_album_lookup_not_found_rows: number;
    provider_album_lookup_rows?: number;
    provider_album_lookup_not_found_rows?: number;
  };
  sqlite_state_db: {
    db_path: string;
    db_bytes: number;
    wal_bytes: number;
    shm_bytes: number;
    files_album_scan_cache_rows: number;
    files_album_scan_cache_healthy_rows: number;
    files_pending_changes_rows: number;
    files_library_published_rows: number;
    scan_resume_pending_artists_rows: number;
  };
  media_cache: {
    root: string;
    total: {
      exists: boolean;
      bytes_total: number;
      file_count: number;
      dir_count: number;
      walk_truncated: boolean;
      walk_errors: number;
    };
    album: {
      exists: boolean;
      bytes_total: number;
      file_count: number;
      dir_count: number;
      walk_truncated: boolean;
      walk_errors: number;
    };
    artist: {
      exists: boolean;
      bytes_total: number;
      file_count: number;
      dir_count: number;
      walk_truncated: boolean;
      walk_errors: number;
    };
  };
  scan_cache_counters_live: {
    audio_hits: number;
    audio_misses: number;
    mb_hits: number;
    mb_misses: number;
  };
  files_watcher: {
    running: boolean;
    enabled?: boolean;
    available?: boolean;
    reason?: string;
    restart_in_progress?: boolean;
    consecutive_failures?: number;
    last_restart_duration_ms?: number | null;
    last_error?: string;
    last_restart_started_at?: number | null;
    last_restart_ended_at?: number | null;
    roots: string[];
    dirty_count: number;
    last_event_at?: number | null;
    last_event_path?: string | null;
  };
}

export interface LogTailResponse {
  path: string;
  lines: string[];
  entries?: LogTailEntry[];
}

export interface LogTailEntry {
  raw: string;
  timestamp: string;
  level: string;
  thread: string;
  thread_key: string;
  thread_slot: number;
  message: string;
  kind: string;
  marker: string;
}

export interface LastScanSummary {
  ffmpeg_formats: Record<string, number>;
  mb_connection_ok: boolean;
  mb_albums_verified: number;
  mb_albums_identified: number;
  ai_connection_ok: boolean;
  ai_groups_count: number;
  /** Number of metadata (MB) matches verified by AI this scan. */
  mb_verified_by_ai?: number;
  /** AI errors during the scan (e.g. provider 400/429); shown in summary. */
  ai_errors?: { message: string; group?: string }[];
  /** Last-scan-only stats for "Last scan summary" UI */
  duration_seconds?: number;
  artists_total?: number;
  albums_scanned?: number;
  duplicate_groups_count?: number;
  total_duplicates_count?: number;
  broken_albums_count?: number;
  missing_albums_count?: number;
  albums_without_artist_image?: number;
  albums_without_album_image?: number;
  albums_without_complete_tags?: number;
  albums_without_mb_id?: number;
  albums_without_artist_mb_id?: number;
  audio_cache_hits?: number;
  audio_cache_misses?: number;
  mb_cache_hits?: number;
  mb_cache_misses?: number;
  lossy_count?: number;
  lossless_count?: number;
  strict_total_albums?: number;
  strict_matched_albums?: number;
  strict_unmatched_albums?: number;
  albums_with_mb_id?: number;
  /** When auto-move ran this scan: number of duplicate albums moved. */
  dupes_moved_this_scan?: number;
  /** When auto-move ran this scan: MB reclaimed. */
  space_saved_mb_this_scan?: number;
  /** Match stats per source (chart-ready): matched / total */
  mb_match?: { matched: number; total: number };
  discogs_match?: { matched: number; total: number };
  lastfm_match?: { matched: number; total: number };
  bandcamp_match?: { matched: number; total: number };
}

/** Current group being moved (for live dedupe UI). */
export interface DedupeCurrentGroup {
  artist: string;
  album: string;
  num_dupes: number;
  winner: { title_raw: string; album_id?: number; folder?: string };
  losers: Array<{ title_raw: string; album_id?: number; folder?: string }>;
  destination: string;
  status: string;
}

export interface DedupeProgress {
  deduping: boolean;
  progress: number;
  total: number;
  saved: number;
  /** Global count of albums/folders moved (from DB). */
  moved?: number;
  /** MB saved in this run (so far). */
  saved_this_run?: number;
  /** 0–100. */
  percent?: number;
  /** Estimated seconds until completion. */
  eta_seconds?: number | null;
  /** Group currently being moved (artist, album, winner, losers, destination). */
  current_group?: DedupeCurrentGroup | null;
  /** Last folder written to /dupes (path + timestamp) so UI can confirm writes. */
  last_write?: { path: string; at: number } | null;
}

export interface Track {
  name: string;
  duration?: number;
  format?: string;
  bitrate?: number;
  is_bonus?: boolean;
  path?: string;
}

export interface Edition {
  thumb_data: string;
  thumb_url?: string;
  title_raw: string;
  size: number;
  fmt: string;
  br: number;
  sr: number;
  bd: number;
  path?: string;
  folder?: string;
  album_id?: number;
  track_count?: number;
  tracks?: Track[];
  musicbrainz_id?: string;  // MusicBrainz release-group ID
  /** True when MusicBrainz match was chosen by AI verify (USE_AI_FOR_MB_VERIFY). */
  match_verified_by_ai?: boolean;
}

export interface DuplicateDetails {
  artist: string;
  album: string;
  /** Plex rating key of the artist (for "Open in Plex" → artist page) */
  artist_id?: number;
  editions: Edition[];
  rationale: string;
  merge_list: string[];
}

export interface MovedItem {
  thumb_data: string;
  artist: string;
  title_raw: string;
  size: number;
  fmt: string;
  br: number;
  sr: number;
  bd: number;
}

export interface DedupeResult {
  moved: MovedItem[];
  /** When backend runs dedupe in background: "started" and moved=[] */
  status?: string;
  message?: string;
}

export interface PMDAConfig {
  // Plex
  PLEX_HOST: string;
  PLEX_TOKEN: string;
  PLEX_BASE_PATH?: string;
  PLEX_DB_PATH: string;
  PLEX_DB_FILE: string;
  SECTION_IDS: string;
  
  // Paths
  PATH_MAP: Record<string, string>;
  DUPE_ROOT: string;
  /** Quarantine folder for incomplete albums (e.g. /dupes/incomplete_albums). Used when moving from "Incomplete scan results". */
  INCOMPLETE_ALBUMS_TARGET_DIR?: string;
  PMDA_CONFIG_DIR: string;
  MUSIC_PARENT_PATH: string;
  /** RW status for music folder(s) and dupes folder (from backend) */
  paths_status?: { music_rw: boolean; dupes_rw: boolean };
  /** Container mounts status for fresh-config welcome message */
  container_mounts?: { config_rw: boolean; plex_db_ro: boolean; music_rw: boolean; dupes_rw: boolean };
  
  // Scan
  SCAN_THREADS: number | 'auto';
  SKIP_FOLDERS: string;
  CROSS_LIBRARY_DEDUPE: boolean;
  CROSSCHECK_SAMPLES: number;
  /** When true, ignore all existing caches during scans (audio+metadata). */
  SCAN_DISABLE_CACHE?: boolean;
  /** Skip path binding verification at startup */
  DISABLE_PATH_CROSSCHECK?: boolean;
  /** When true, treat album titles like "Lemodie (Flac)" and "Lemodie" as the same for duplicate detection (strip format/version in parentheses). */
  NORMALIZE_PARENTHETICAL_FOR_DEDUPE?: boolean;
  FORMAT_PREFERENCE: string[];
  /** Library backend mode: plex (default) or files. */
  LIBRARY_MODE?: 'plex' | 'files';
  /** Include albums not formally matched by PMDA in library views. */
  LIBRARY_INCLUDE_UNMATCHED?: boolean;
  /** File-library roots (comma-separated string in UI). */
  FILES_ROOTS?: string;
  /** Guided library workflow mode. */
  LIBRARY_WORKFLOW_MODE?: 'managed' | 'mirror' | 'inplace' | 'custom';
  /** Clean serving library root for the guided workflow. */
  LIBRARY_SERVING_ROOT?: string;
  /** Intake folders used by managed / in-place workflows. */
  LIBRARY_INTAKE_ROOTS?: string;
  /** Source library folders used by mirror / in-place workflows. */
  LIBRARY_SOURCE_ROOTS?: string;
  /** Guided duplicate quarantine root. */
  LIBRARY_DUPES_ROOT?: string;
  /** Guided incomplete quarantine root. */
  LIBRARY_INCOMPLETE_ROOT?: string;
  /** Guided materialization policy for library promotion. */
  LIBRARY_MATERIALIZATION_MODE?: 'hardlink' | 'symlink' | 'copy' | 'move';
  /** Guided folder naming toggle for detected format. */
  LIBRARY_INCLUDE_FORMAT_IN_FOLDER?: boolean;
  /** Guided folder naming toggle for album type. */
  LIBRARY_INCLUDE_TYPE_IN_FOLDER?: boolean;
  /** Derived helper flags from backend. */
  LIBRARY_HAS_INTAKE?: boolean;
  LIBRARY_VISIBLE_SCOPES?: LibraryBrowseScope[] | string;
  /** Canonical winner source root id for dedupe winner placement. */
  WINNER_SOURCE_ROOT_ID?: string;
  /** Winner placement strategy for canonical root normalization. */
  LIBRARY_WINNER_PLACEMENT_STRATEGY?: 'move' | 'hardlink' | 'symlink' | 'copy';
  /** Root folder for the clean export library. */
  EXPORT_ROOT?: string;
  /** Naming template used when exporting via hardlinks/symlinks/copies. */
  EXPORT_NAMING_TEMPLATE?: string;
  /** Link strategy for export: hardlink | symlink | copy | move. */
  EXPORT_LINK_STRATEGY?: 'hardlink' | 'symlink' | 'copy' | 'move';
  /** Include the detected album format in the exported album folder name. */
  EXPORT_INCLUDE_ALBUM_FORMAT_IN_FOLDER?: boolean;
  /** Include the detected album type (Album/EP/Single/Compilation/Live) in the exported album folder name. */
  EXPORT_INCLUDE_ALBUM_TYPE_IN_FOLDER?: boolean;
  /** NVMe-friendly media cache root for pre-rendered artwork thumbnails (album/artist). */
  MEDIA_CACHE_ROOT?: string;
  /** In-process RAM cache budget for artwork bytes (MB). 0 disables RAM artwork cache. */
  ARTWORK_RAM_CACHE_MB?: number;
  /** TTL (seconds) for artwork entries in in-process RAM cache. */
  ARTWORK_RAM_CACHE_TTL_SEC?: number;
  /** Max single artwork payload admitted in RAM cache (MB). */
  ARTWORK_RAM_CACHE_MAX_ITEM_MB?: number;
  /** Auto-tune artwork RAM cache from currently available memory. */
  ARTWORK_RAM_CACHE_AUTO?: boolean;
  /** Optional hard cap for auto artwork RAM cache (MB). 0 = no explicit cap. */
  ARTWORK_RAM_CACHE_AUTO_MAX_MB?: number;
  /** Auto-tune interval (seconds). */
  ARTWORK_RAM_CACHE_AUTO_INTERVAL_SEC?: number;
  /** When true (Files mode), rebuild the export library automatically after a Magic scan. */
  AUTO_EXPORT_LIBRARY?: boolean;
  
  // AI Provider
  AI_PROVIDER: 'openai' | 'openai-api' | 'openai-codex' | 'anthropic' | 'google' | 'ollama';
  /** Global AI usage profile across matching/dedupe/vision flow. */
  AI_USAGE_LEVEL?: 'limited' | 'medium' | 'aggressive' | 'auto';
  /** Scan-time AI routing policy once providers/OCR are exhausted. */
  SCAN_AI_POLICY?: 'local_only' | 'local_then_paid' | 'paid_only';
  /** Ordered paid AI fallback chain for scan batch flows. */
  SCAN_PAID_PROVIDER_ORDER?: string;
  /** Ordered local/external web-search chain before any paid AI web search. */
  WEB_SEARCH_LOCAL_ORDER?: string;
  /** Effective batch AI provider after applying the scan policy. */
  SCAN_AI_EFFECTIVE_BATCH?: string;
  /** Effective web-search layer for the scan pipeline. */
  SCAN_AI_EFFECTIVE_WEB_SEARCH?: string;
  OPENAI_API_KEY: string;
  OPENAI_API_KEY_SET?: boolean;
  OPENAI_ENABLE_API_KEY_MODE?: boolean;
  OPENAI_ENABLE_CODEX_OAUTH_MODE?: boolean;
  OPENAI_OAUTH_REFRESH_TOKEN_SET?: boolean;
  OPENAI_AUTH_MODE?: 'none' | 'api_key' | 'oauth_api_key' | 'oauth_connected_no_api_key' | string;
  OPENAI_CODEX_OAUTH_CONNECTED?: boolean;
  OPENAI_PROVIDER_PREF_INTERACTIVE?: string;
  OPENAI_PROVIDER_PREF_BATCH?: string;
  OPENAI_PROVIDER_PREF_WEB_SEARCH?: string;
  OPENAI_PROVIDER_EFFECTIVE_INTERACTIVE?: string;
  OPENAI_PROVIDER_EFFECTIVE_BATCH?: string;
  OPENAI_MODEL: string;
  OPENAI_MODEL_FALLBACKS: string;
  ANTHROPIC_API_KEY: string;
  GOOGLE_API_KEY: string;
  OLLAMA_URL: string;
  OLLAMA_MODEL?: string;
  OLLAMA_COMPLEX_MODEL?: string;
  OLLAMA_RUNTIME_MODE?: 'managed' | 'adopted' | 'external' | 'absent' | string;
  SCAN_AI_LOCAL_BULK_MODEL?: string;
  SCAN_AI_LOCAL_HARD_MODEL?: string;
  SCAN_AI_LOCAL_HARD_AVAILABLE?: boolean;
  /** Custom AI prompt for duplicate selection (advanced). Empty = use default. */
  AI_PROMPT?: string;
  
  // MusicBrainz & Notifications
  USE_MUSICBRAINZ: boolean;
  MUSICBRAINZ_EMAIL: string;
  /** How to apply MusicBrainz artist credits to tags and grouping. */
  ARTIST_CREDIT_MODE?: 'album_artist_strict' | 'musicbrainz_full_credit' | 'picard_like_default';
  /** Which display name to prefer for classical people: original/default or english/transliterated. */
  CLASSICAL_NAME_PREFERENCE?: 'original' | 'english';
  /** Re-query MusicBrainz for albums previously cached as "not found" on each scan */
  MB_RETRY_NOT_FOUND?: boolean;
  /** Advanced: ignore MusicBrainz cache and stored MBIDs, forcing a full lookup every scan (slower; for testing). */
  MB_DISABLE_CACHE?: boolean;
  /** Use AI to choose among multiple MusicBrainz candidates (title-only prompt). */
  USE_AI_FOR_MB_MATCH?: boolean;
  /** Use AI to verify MusicBrainz match (artist, title, track count/titles). Can recover e.g. "Volume I" vs "volume i". */
  USE_AI_FOR_MB_VERIFY?: boolean;
  /** Use AI arbitration for ambiguous duplicate groups. */
  USE_AI_FOR_DEDUPE?: boolean;
  /** Route MusicBrainz lookups to a local/private mirror. */
  MUSICBRAINZ_MIRROR_ENABLED?: boolean;
  /** Base URL of the local/private MusicBrainz mirror. */
  MUSICBRAINZ_BASE_URL?: string;
  /** Friendly name displayed in the UI for the MusicBrainz mirror. */
  MUSICBRAINZ_MIRROR_NAME?: string;
  /** Runtime management mode for the local MusicBrainz mirror. */
  MUSICBRAINZ_RUNTIME_MODE?: 'managed' | 'adopted' | 'external' | 'absent' | string;
  /** Optional MetaBrainz replication token used by managed local mirrors. */
  MUSICBRAINZ_REPLICATION_TOKEN?: string;
  MUSICBRAINZ_REPLICATION_TOKEN_SET?: boolean;
  /** Public MusicBrainz queue rate limit in requests/sec. */
  MB_PUBLIC_QUEUE_RPS?: number;
  /** Local MusicBrainz mirror queue rate limit in requests/sec. */
  MB_MIRROR_QUEUE_RPS?: number;
  /** Effective MusicBrainz endpoint after applying mirror settings. */
  MUSICBRAINZ_EFFECTIVE_BASE_URL?: string;
  /** Shared roots for managed local runtime provisioning. */
  MANAGED_RUNTIME_CONFIG_ROOT?: string;
  MANAGED_RUNTIME_DATA_ROOT?: string;
  MANAGED_MUSICBRAINZ_INSTALL_ROOT?: string;
  MANAGED_MUSICBRAINZ_UPDATE_ENABLED?: boolean;
  MANAGED_MUSICBRAINZ_REINDEX_INTERVAL_HOURS?: number;
  /** Enable the shared provider gateway with cache and throttling. */
  PROVIDER_GATEWAY_ENABLED?: boolean;
  /** Enable shared provider gateway response caching. */
  PROVIDER_GATEWAY_CACHE_ENABLED?: boolean;
  /** Maximum simultaneous provider requests through the gateway. */
  PROVIDER_GATEWAY_MAX_INFLIGHT?: number;
  /** Shared Discogs request budget per minute. */
  PROVIDER_GATEWAY_DISCOGS_RPM?: number;
  /** Shared Last.fm request budget per minute. */
  PROVIDER_GATEWAY_LASTFM_RPM?: number;
  /** Shared Bandcamp request budget per minute. */
  PROVIDER_GATEWAY_BANDCAMP_RPM?: number;
  /** Enable runtime auto-tuning for local MB mirror and gateway concurrency. */
  AUTO_TUNE_ENABLED?: boolean;
  /** Seconds between auto-tune passes. */
  AUTO_TUNE_INTERVAL_SEC?: number;
  /** Lower bound for MB mirror requests/sec. */
  AUTO_TUNE_MB_MIRROR_MIN_RPS?: number;
  /** Upper bound for MB mirror requests/sec. */
  AUTO_TUNE_MB_MIRROR_MAX_RPS?: number;
  /** Lower bound for provider gateway in-flight requests. */
  AUTO_TUNE_PROVIDER_MAX_INFLIGHT_MIN?: number;
  /** Upper bound for provider gateway in-flight requests. */
  AUTO_TUNE_PROVIDER_MAX_INFLIGHT_CAP?: number;
  /** Enable persisted metadata jobs for hybrid/offloaded metadata processing. */
  METADATA_QUEUE_ENABLED?: boolean;
  /** Metadata worker topology. */
  METADATA_WORKER_MODE?: 'local' | 'hybrid';
  /** Number of metadata workers. */
  METADATA_WORKER_COUNT?: number;
  /** Number of metadata jobs to pull per batch. */
  METADATA_JOB_BATCH_SIZE?: number;
  /** When enabled, PMDA can auto-generate album reviews/descriptions for SOFT_MATCH albums. */
  USE_AI_FOR_SOFT_MATCH_PROFILES?: boolean;
  /** After AI text match, compare local cover to Cover Art Archive (vision). Reject match if "No". */
  USE_AI_VISION_FOR_COVER?: boolean;
  /** Minimum AI confidence (0–100). Below this, reject match and try other sources. 0 = accept all. */
  AI_CONFIDENCE_MIN?: number;
  /** Model for vision (e.g. gpt-4o-mini). Empty = use main model. */
  OPENAI_VISION_MODEL?: string;
  /** Before saving/embedding a fetched cover (improve), verify via vision that it matches the album. */
  USE_AI_VISION_BEFORE_COVER_INJECT?: boolean;
  /** Copy each album to /dupes/original_version before applying tags, cover, and artist image. */
  BACKUP_BEFORE_FIX?: boolean;
  /** After each scan: automatic dedupe then improve-all (tags, covers, artist images). */
  MAGIC_MODE?: boolean;
  /** When true (default), improve-all will re-run on albums that PMDA has processed but not fully completed (missing tags/cover/artist image). When false, albums already touched by PMDA but not 100% complete are skipped in subsequent improve-all runs. */
  REPROCESS_INCOMPLETE_ALBUMS?: boolean;
  /** Number of albums to improve in parallel during improve-all (1–8). 1 = sequential; higher speeds up fix-all when Discogs/Last.fm/Bandcamp are used. MusicBrainz calls remain rate-limited. */
  IMPROVE_ALL_WORKERS?: number;
  /** Number of parallel ffprobe workers for audio analysis during scan. Higher values speed up scan on multi-core systems. */
  FFPROBE_POOL_SIZE?: number;
  /** Identify albums by acoustic fingerprint (AcoustID) when tags are missing. */
  USE_ACOUSTID?: boolean;
  /** AcoustID API key (from acoustid.org). Required if USE_ACOUSTID is on. */
  ACOUSTID_API_KEY?: string;
  /** When false (default), skip AcousticID lookup for albums that already have MusicBrainz release-group ID in tags (saves API calls). */
  USE_ACOUSTID_WHEN_TAGGED?: boolean;
  /** When no MB candidate or AI says NONE, use web search (Serper) + AI to suggest MBID. */
  USE_WEB_SEARCH_FOR_MB?: boolean;
  /** Which web search backend PMDA should use before any paid fallback. */
  WEB_SEARCH_PROVIDER?: 'auto' | 'serper' | 'ollama' | 'ai_only' | 'disabled';
  /** When Serper is unavailable (missing key/no credits), fallback to OpenAI native web search tool. */
  USE_AI_WEB_SEARCH_FALLBACK?: boolean;
  /** Hard cap for total AI calls per scan (0 = disable AI calls during scan). */
  AI_MAX_CALLS_PER_SCAN?: number;
  /** Minimum delay between AI calls in scan flows. */
  AI_CALL_COOLDOWN_SEC?: number;
  /** Serper.dev API key for web search. */
  SERPER_API_KEY?: string;
  // Metadata Providers
  USE_DISCOGS: boolean;
  DISCOGS_USER_TOKEN: string;
  DISCOGS_CONSUMER_KEY: string;
  DISCOGS_CONSUMER_SECRET: string;
  USE_LASTFM: boolean;
  LASTFM_API_KEY: string;
  LASTFM_API_SECRET: string;
  LASTFM_SCROBBLE_ENABLED?: boolean;
  LASTFM_NOW_PLAYING_ENABLED?: boolean;
  LASTFM_SCROBBLE_CONNECTED?: boolean;
  LASTFM_SCROBBLE_USER?: string;
  LASTFM_SCROBBLE_PENDING?: boolean;
  /** fanart.tv API key (optional). Used for additional artist artwork (MBID-based). */
  FANART_API_KEY?: string;
  USE_BANDCAMP: boolean;
  /** Skip MusicBrainz lookup and tagging for albums detected as live (folder/title heuristics). */
  SKIP_MB_FOR_LIVE_ALBUMS?: boolean;
  /** Minimum tracklist match ratio (0–1) to accept a release; below this the match is rejected. */
  TRACKLIST_MATCH_MIN?: number | string;
  /** For live albums: only assign an MB release-group if it has secondary type "Live". */
  LIVE_ALBUMS_MB_STRICT?: boolean;
  /** Safety mode for live albums dedupe heuristics. */
  LIVE_DEDUPE_MODE?: 'safe' | 'aggressive';

  // Concert discovery (UI filtering)
  CONCERTS_FILTER_ENABLED?: boolean;
  /** Home latitude used to filter upcoming concerts (stringified float). */
  CONCERTS_HOME_LAT?: string;
  /** Home longitude used to filter upcoming concerts (stringified float). */
  CONCERTS_HOME_LON?: string;
  /** Radius (km) used to filter upcoming concerts (stringified int). */
  CONCERTS_RADIUS_KM?: string;
  DISCORD_WEBHOOK: string;
  DISCORD_WEBHOOK_SET?: boolean;
  LOG_LEVEL: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR';
  LOG_FILE: string;
  AUTO_MOVE_DUPES: boolean;
  PIPELINE_ENABLE_MATCH_FIX?: boolean;
  PIPELINE_ENABLE_DEDUPE?: boolean;
  PIPELINE_ENABLE_INCOMPLETE_MOVE?: boolean;
  PIPELINE_ENABLE_EXPORT?: boolean;
  PIPELINE_ENABLE_PLAYER_SYNC?: boolean;
  PIPELINE_PLAYER_TARGET?: 'none' | 'plex' | 'jellyfin' | 'navidrome';
  PIPELINE_POST_SCAN_ASYNC?: boolean;
  TASK_NOTIFICATIONS_ENABLED?: boolean;
  TASK_NOTIFICATIONS_SUCCESS?: boolean;
  TASK_NOTIFICATIONS_FAILURE?: boolean;
  TASK_NOTIFICATIONS_SILENT_INTERACTIVE_SCAN?: boolean;
  TASK_NOTIFICATIONS_COOLDOWN_SEC?: number;
  TASK_NOTIFY_SCAN_CHANGED?: boolean;
  TASK_NOTIFY_SCAN_FULL?: boolean;
  TASK_NOTIFY_ENRICH_BATCH?: boolean;
  TASK_NOTIFY_DEDUPE?: boolean;
  TASK_NOTIFY_INCOMPLETE_MOVE?: boolean;
  TASK_NOTIFY_EXPORT?: boolean;
  TASK_NOTIFY_PLAYER_SYNC?: boolean;
  SCHEDULER_PAUSED?: boolean;
  /** Allow scheduler to run non-scan jobs (enrich/dedupe/export/player-sync) outside scan post-chain. */
  SCHEDULER_ALLOW_NON_SCAN_JOBS?: boolean;
  // Integrations
  LIDARR_URL: string;
  LIDARR_API_KEY: string;
  AUTOBRR_URL: string;
  AUTOBRR_API_KEY: string;
  AUTO_FIX_BROKEN_ALBUMS: boolean;
  // Broken album detection thresholds
  BROKEN_ALBUM_CONSECUTIVE_THRESHOLD: number;
  BROKEN_ALBUM_PERCENTAGE_THRESHOLD: number;
  // Incomplete album definition
  REQUIRED_TAGS: string[];
  // Player integrations
  JELLYFIN_URL?: string;
  JELLYFIN_API_KEY?: string;
  NAVIDROME_URL?: string;
  NAVIDROME_USERNAME?: string;
  NAVIDROME_PASSWORD?: string;
  NAVIDROME_API_KEY?: string;
}

export type PlayerTarget = 'none' | 'plex' | 'jellyfin' | 'navidrome';

export interface PlayerActionResult {
  success: boolean;
  target: string;
  message: string;
}

export interface PlayerCheckPayload {
  target?: PlayerTarget;
  PLEX_HOST?: string;
  PLEX_TOKEN?: string;
  JELLYFIN_URL?: string;
  JELLYFIN_API_KEY?: string;
  NAVIDROME_URL?: string;
  NAVIDROME_USERNAME?: string;
  NAVIDROME_PASSWORD?: string;
  NAVIDROME_API_KEY?: string;
}

export type TaskJobType =
  | 'scan_changed'
  | 'scan_full'
  | 'enrich_batch'
  | 'dedupe'
  | 'incomplete_move'
  | 'export'
  | 'player_sync'
  | 'managed_musicbrainz_update';

export type TaskScope = 'new' | 'full' | 'both';
export type TaskEventStatus = 'started' | 'completed' | 'failed' | 'skipped';

export interface TaskEvent {
  event_id: number;
  run_id?: string | null;
  job_type: TaskJobType;
  scope: TaskScope;
  status: TaskEventStatus;
  message: string;
  metrics?: Record<string, unknown>;
  summary?: Record<string, unknown>;
  error?: string;
  source?: string;
  started_at: number;
  ended_at?: number | null;
  duration_ms?: number | null;
}

export interface TaskEventsResponse {
  events: TaskEvent[];
  last_id: number;
  server_time: number;
}

export type SchedulerTriggerType = 'interval' | 'weekly';

export interface SchedulerRule {
  rule_id?: number;
  job_type: TaskJobType;
  enabled: boolean;
  trigger_type: SchedulerTriggerType;
  interval_min?: number | null;
  days_of_week?: string;
  time_local?: string;
  scope: TaskScope;
  post_scan_chain: boolean;
  priority: number;
  max_concurrency: number;
  next_run_ts?: number | null;
  last_run_ts?: number | null;
  created_at?: number;
  updated_at?: number;
}

export interface SchedulerRulesResponse {
  rules: SchedulerRule[];
  paused: boolean;
}

export interface SchedulerRunJobPayload {
  job_type: TaskJobType;
  scope?: TaskScope;
  source?: string;
}

export interface SchedulerRunJobResponse {
  status: 'started' | 'blocked';
  message?: string;
  run_id?: string | null;
}

export interface SchedulerRunningJob {
  run_id: string;
  rule_id?: number | null;
  job_type: TaskJobType;
  scope: TaskScope;
  source: string;
  origin_scan_id?: number | null;
  started_at: number;
}

export interface SchedulerJobStatusEntry {
  job_run_id: string;
  rule_id?: number | null;
  job_type: TaskJobType;
  scope: TaskScope;
  source: string;
  origin_scan_id?: number | null;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'skipped';
  message: string;
  metrics?: Record<string, unknown>;
  error?: string;
  created_at: number;
  started_at?: number | null;
  ended_at?: number | null;
  duration_ms?: number | null;
}

export interface SchedulerJobsStatusResponse {
  paused: boolean;
  thread_alive: boolean;
  running: SchedulerRunningJob[];
  jobs: SchedulerJobStatusEntry[];
}

export interface ScanHistoryEntry {
  scan_id: number;
  start_time: number;
  end_time?: number;
  duration_seconds?: number;
  albums_scanned: number;
  duplicates_found: number;
  artists_processed: number;
  artists_total: number;
  ai_used_count: number;
  mb_used_count: number;
  ai_enabled: boolean;
  mb_enabled: boolean;
  auto_move_enabled: boolean;
  space_saved_mb: number;
  albums_moved: number;
  status: string;
  /** 'scan' | 'dedupe' – type of history entry for unified History view */
  entry_type?: 'scan' | 'dedupe' | 'incomplete';
  // Detailed statistics
  duplicate_groups_count?: number;
  total_duplicates_count?: number;
  broken_albums_count?: number;
  missing_albums_count?: number;
  albums_without_artist_image?: number;
  albums_without_album_image?: number;
  albums_without_complete_tags?: number;
  albums_without_mb_id?: number;
  albums_without_artist_mb_id?: number;
  ai_tokens_total?: number;
  ai_cost_usd_total?: number;
  ai_unpriced_calls?: number;
  ai_lifecycle_complete?: boolean;
  /** Parsed summary from scan (formats, cache, AI/MB stats, etc.) when available */
  summary_json?: ScanHistorySummaryJson | null;
  /** Human-readable list of steps executed during the scan (from summary_json.steps_executed) */
  steps_executed?: string[];
}

export interface ScanHistorySummaryJson {
  /** Human-readable list of metadata steps executed (MusicBrainz, AI, vision, fallbacks, etc.) */
  steps_executed?: string[];
  ffmpeg_formats?: Record<string, number>;
  mb_connection_ok?: boolean;
  mb_albums_verified?: number;
  mb_albums_identified?: number;
  ai_connection_ok?: boolean;
  ai_groups_count?: number;
  mb_verified_by_ai?: number;
  ai_calls_total?: number;
  ai_calls_provider_identity?: number;
  ai_calls_mb_verify?: number;
  ai_calls_web_mbid?: number;
  ai_calls_vision?: number;
  ai_tokens_total?: number;
  ai_cost_usd_total?: number;
  ai_unpriced_calls?: number;
  ai_lifecycle_complete?: boolean;
  // Duplicate decision telemetry
  duplicate_groups_total?: number;
  duplicate_groups_saved?: number;
  duplicate_groups_ai_decided?: number;
  duplicate_groups_skipped?: number;
  duplicate_groups_ai_failed_total?: number;
  duplicate_groups_ai_failed_then_recovered?: number;
  duplicate_groups_ai_failed_unresolved?: number;
  ai_errors?: { message: string; group?: string }[];
  duration_seconds?: number;
  artists_total?: number;
  albums_scanned?: number;
  duplicate_groups_count?: number;
  total_duplicates_count?: number;
  broken_albums_count?: number;
  missing_albums_count?: number;
  albums_without_artist_image?: number;
  albums_without_album_image?: number;
  albums_without_complete_tags?: number;
  albums_without_mb_id?: number;
  albums_without_artist_mb_id?: number;
  audio_cache_hits?: number;
  audio_cache_misses?: number;
  mb_cache_hits?: number;
  mb_cache_misses?: number;
  lossy_count?: number;
  lossless_count?: number;
  strict_total_albums?: number;
  strict_matched_albums?: number;
  strict_unmatched_albums?: number;
  albums_with_mb_id?: number;
  dupes_moved_this_scan?: number;
  space_saved_mb_this_scan?: number;
  scan_discogs_matched?: number;
  scan_lastfm_matched?: number;
  scan_bandcamp_matched?: number;
  incomplete_moved_this_scan?: number;
  incomplete_moved_mb_this_scan?: number;
  scan_tracks_detected_total?: number;
  scan_tracks_library_kept?: number;
  scan_tracks_moved_dupes?: number;
  scan_tracks_moved_incomplete?: number;
  scan_tracks_unaccounted?: number;
  player_sync_target?: string;
  player_sync_ok?: boolean | null;
  player_sync_message?: string;
  cover_from_mb?: number;
  cover_from_discogs?: number;
  cover_from_lastfm?: number;
  cover_from_bandcamp?: number;
  // PMDA album-level stats for this scan (albums touched by PMDA during this run)
  pmda_albums_processed?: number;
  pmda_albums_complete?: number;
  pmda_albums_with_cover?: number;
  pmda_albums_with_artist_image?: number;
  provider_no_tracklist?: {
    total?: number;
    by_provider?: Record<string, number>;
    by_cause?: Record<string, number>;
    details_by_provider?: Record<string, Record<string, number>>;
  };
  provider_no_tracklist_total?: number;
  provider_no_tracklist_by_provider?: Record<string, number>;
  provider_no_tracklist_by_cause?: Record<string, number>;
}

export type AICostGroupBy = 'analysis_type' | 'job_type' | 'model' | 'provider' | 'album' | 'auth_mode';

export interface AICostEntry {
  analysis_type?: string;
  job_type?: string;
  model?: string;
  provider?: string;
  auth_mode?: string;
  album_id?: number | null;
  album_artist?: string;
  album_title?: string;
  scope?: string;
  calls: number;
  input_tokens: number;
  cached_input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cost_usd: number;
  cost_microusd?: number;
  unpriced_calls?: number;
}

export interface ScanAICostTotals {
  calls: number;
  input_tokens: number;
  cached_input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cost_usd: number;
  cost_microusd?: number;
  unpriced_calls: number;
}

export interface ScanAICostSummary {
  scan_id: number;
  currency: 'USD' | string;
  include_lifecycle: boolean;
  group_by: AICostGroupBy;
  limit?: number;
  totals: ScanAICostTotals;
  breakdown: AICostEntry[];
  lifecycle_complete: boolean;
  last_updated_at?: number | null;
}

export interface ProviderSourceBucket {
  total: number;
  providers: Record<string, number>;
}

export interface EnrichmentSourcesResponse {
  available: boolean;
  reason?: string;
  overall: ProviderSourceBucket;
  album_profiles: ProviderSourceBucket;
  artist_profiles: ProviderSourceBucket;
  artist_images: ProviderSourceBucket;
  label_logos: ProviderSourceBucket;
}

export interface BenchmarkCheckResult {
  name: string;
  pass: boolean;
  weight: number;
  detail?: string;
}

export interface BenchmarkReportSummary {
  file: string;
  generated_at: number;
  scan_id: number;
  score: number;
  earned_weight: number;
  total_weight: number;
  check_count: number;
  pass_count: number;
  fail_count: number;
  failed_checks: string[];
  checks: BenchmarkCheckResult[];
}

export interface BenchmarkReportsResponse {
  available: boolean;
  path: string;
  reports: BenchmarkReportSummary[];
}

export interface AIDomainBenchmarkSummary {
  file: string;
  generated_at: number;
  available: boolean;
  note?: string;
  sample_size?: number;
  baseline_score?: number | null;
  assisted_score?: number | null;
  delta?: number | null;
  ai_completed?: number;
  ai_skipped?: number;
  ai_failed?: number;
  avg_latency_sec?: number | null;
}

export interface AIQueueStatus {
  domain: 'matching' | 'dedupe' | 'incomplete' | 'review' | string;
  running: boolean;
  waiting_for_idle_scan?: boolean;
  queued: number;
  worker_started?: boolean;
  current_label?: string;
  current_model?: string;
  last_status?: string;
  last_error?: string;
  last_started_at?: number;
  last_finished_at?: number;
  last_latency_ms?: number;
  avg_latency_ms?: number;
  completed_count?: number;
  failed_count?: number;
  skipped_count?: number;
  last_result?: Record<string, unknown>;
}

export interface AIDomainOverview {
  domain: 'matching' | 'dedupe' | 'incomplete' | 'review' | string;
  analysis_types: string[];
  calls_total: number;
  completed: number;
  failed: number;
  skipped: number;
  avg_latency_ms?: number | null;
  cache_hits: number;
  cache_hit_rate?: number | null;
  last_call_at?: number | null;
  override_count: number;
  override_rate?: number | null;
  last_override_at?: number | null;
  queue?: AIQueueStatus | null;
  benchmark?: AIDomainBenchmarkSummary | null;
}

export interface AIOverviewResponse {
  generated_at: number;
  analysis_dir: string;
  queues: Record<string, AIQueueStatus>;
  domains: AIDomainOverview[];
}

export interface ProviderGatewayStatsBucket {
  rpm_limit: number;
  request_count: number;
  network_request_count: number;
  cache_hits: number;
  negative_cache_hits?: number;
  error_cache_hits?: number;
  coalesced_waits?: number;
  lookup_request_count?: number;
  lookup_network_request_count?: number;
  lookup_saved_count?: number;
  lookup_cache_hits?: number;
  lookup_negative_hits?: number;
  lookup_error_hits?: number;
  lookup_coalesced_waits?: number;
  lookup_hit_rate?: number;
  avg_network_requests_per_lookup?: number;
  cache_hit_rate: number;
  rate_limited_count: number;
  failure_count: number;
  timeout_count: number;
  avg_latency_ms: number;
  last_status?: number | null;
  last_error?: string;
  last_context?: string;
  last_request_at?: number | null;
}

export type ManagedRuntimeBundleType = 'musicbrainz_local' | 'ollama_local';

export interface ManagedRuntimeHealth {
  available: boolean;
  overall_status: string;
  message?: string;
  url?: string;
  models?: string[];
  model_count?: number;
}

export interface ManagedRuntimeGpuDevice {
  path: string;
  name?: string;
  kind?: string;
  vendor_id?: string;
  vendor?: string;
}

export interface ManagedRuntimeGpuProbe {
  available: boolean;
  recommended_mode: string;
  available_modes: string[];
  nvidia_devices?: string[];
  dri_devices?: ManagedRuntimeGpuDevice[];
  render_devices?: ManagedRuntimeGpuDevice[];
  card_devices?: ManagedRuntimeGpuDevice[];
  kfd_present?: boolean;
  message?: string;
}

export interface ManagedRuntimeGpuState {
  requested_mode?: string;
  selected_mode?: string;
  mode_label?: string;
  active?: boolean;
  device_paths?: string[];
  env?: Record<string, string>;
  probe?: ManagedRuntimeGpuProbe;
  fallback_reason?: string;
  message?: string;
}

export interface ManagedRuntimeServiceRow {
  name: string;
  status: string;
  message?: string;
  image?: string;
}

export interface ManagedRuntimeCandidate {
  id: string;
  bundle_type: ManagedRuntimeBundleType;
  adoptable: boolean;
  project_name?: string;
  published_url?: string;
  url?: string;
  service_count?: number;
  expected_service_count?: number;
  services_present?: string[];
  health: ManagedRuntimeHealth;
  models?: string[];
  model_count?: number;
}

export interface ManagedRuntimeActionEntry {
  action_id: string;
  bundle_type: ManagedRuntimeBundleType;
  action: string;
  status: string;
  payload?: Record<string, unknown>;
  result?: Record<string, unknown>;
  error?: string;
  created_at: number;
  updated_at: number;
  completed_at?: number | null;
}

export interface ManagedRuntimeUpdateState {
  enabled?: boolean;
  interval_sec?: number;
  strategy?: string;
  next_planned_at?: number;
  last_success_at?: number;
  last_failure_at?: number;
  last_error?: string;
  last_deferred_at?: number;
  last_deferred_reason?: string;
}

export interface ManagedRuntimeBundleStatus {
  bundle_type: ManagedRuntimeBundleType;
  mode: 'managed' | 'adopted' | 'external' | 'absent' | string;
  state: string;
  phase: string;
  phase_message: string;
  config_root?: string;
  data_root?: string;
  install_root?: string;
  effective_url?: string;
  ownership?: string;
  health: ManagedRuntimeHealth;
  services: ManagedRuntimeServiceRow[];
  meta?: Record<string, unknown> & {
    gpu?: ManagedRuntimeGpuState;
    models?: string[];
    model_count?: number;
  };
  last_error?: string;
  update_state?: ManagedRuntimeUpdateState;
  latest_action?: ManagedRuntimeActionEntry | null;
  active?: boolean;
  candidates?: ManagedRuntimeCandidate[];
}

export interface ManagedRuntimePreflight {
  available: boolean;
  docker_socket: string;
  docker_socket_present: boolean;
  docker_cli: string;
  git_cli: string;
  docker_ok: boolean;
  compose_ok: boolean;
  git_ok: boolean;
  message: string;
  self_container: string;
  gpu_probe?: ManagedRuntimeGpuProbe;
}

export interface ManagedRuntimeStatusResponse {
  preflight: ManagedRuntimePreflight;
  config_root: string;
  data_root: string;
  ready: boolean;
  bundles: Record<ManagedRuntimeBundleType, ManagedRuntimeBundleStatus>;
}

export interface ScalingRuntimeResponse {
  musicbrainz: {
    enabled: boolean;
    queue_enabled: boolean;
    mirror_enabled: boolean;
    mirror_name?: string;
    base_url?: string;
    hostname?: string;
    queue_pending: number;
    queue_waiters: number;
    public_rate_limit_per_sec: number;
    current_rate_limit_per_sec?: number;
    avg_latency_ms?: number;
    last_latency_ms?: number;
    completed_count?: number;
    error_count?: number;
    timeout_count?: number;
  };
  provider_gateway: {
    enabled: boolean;
    cache_enabled: boolean;
    max_inflight: number;
    inflight: number;
    max_inflight_observed: number;
    providers: Record<string, ProviderGatewayStatsBucket>;
  };
  metadata_workers: {
    queue_enabled: boolean;
    mode: 'local' | 'hybrid' | string;
    worker_count: number;
    batch_size: number;
    queued?: number;
    running?: number;
    completed?: number;
    failed?: number;
    total?: number;
    oldest_queued_at?: number | null;
  };
  pipeline: {
    local_orchestrator: boolean;
    materialization_local: boolean;
    ocr_execution: string;
    ai_mode: string;
    scan_threads: number;
    ffprobe_pool_size: number;
    match_cover_ocr_mode: string;
    ai_usage_level: string;
    scan_ai_policy: string;
  };
  auto_tune?: {
    enabled: boolean;
    interval_sec: number;
    mb_mirror_min_rps: number;
    mb_mirror_max_rps: number;
    provider_inflight_min: number;
    provider_inflight_cap: number;
    last_run_at?: number;
    last_change_at?: number;
    last_reason?: string;
  };
  managed_runtime?: ManagedRuntimeStatusResponse;
  stage_rates: {
    runtime_sec: number;
    phase: string;
    filesystem_entries_per_hour: number;
    audio_files_per_hour: number;
    albums_processed_per_hour: number;
    albums_published_per_hour: number;
    artists_processed_per_hour: number;
  };
}

export interface ScanHistoryEntryOld {
  scan_id: number;
  start_time: number;
  end_time?: number;
  duration_seconds?: number;
  albums_scanned: number;
  duplicates_found: number;
  artists_processed: number;
  artists_total: number;
  ai_used_count: number;
  mb_used_count: number;
  ai_enabled: boolean;
  mb_enabled: boolean;
  auto_move_enabled: boolean;
  space_saved_mb: number;
  albums_moved: number;
  status: 'completed' | 'failed' | 'cancelled';
  // Detailed statistics
  duplicate_groups_count?: number;
  total_duplicates_count?: number;
  broken_albums_count?: number;
  missing_albums_count?: number;
  albums_without_artist_image?: number;
  albums_without_album_image?: number;
  albums_without_complete_tags?: number;
  albums_without_mb_id?: number;
  albums_without_artist_mb_id?: number;
}

export interface ScanMove {
  move_id: number;
  scan_id: number;
  artist: string;
  album_id: number;
  original_path: string;
  moved_to_path: string;
  size_mb: number;
  moved_at: number;
  restored: boolean;
  /** Album title (moved edition) when available */
  album_title?: string;
  /** Format description (e.g. FLAC 24/96) when available */
  fmt_text?: string;
  /** Move reason: dedupe | incomplete */
  move_reason?: 'dedupe' | 'incomplete' | string;
  /** Dedupe winner metadata (when move_reason=dedupe). */
  winner_album_id?: number | null;
  winner_title?: string;
  winner_path?: string;
  /** Pipeline decision metadata captured at move time. */
  decision_source?: string;
  decision_provider?: string;
  decision_reason?: string;
  decision_confidence?: number | null;
  /** Current filesystem state for the moved edition. */
  status?: 'moved' | 'restored' | 'missing' | string;
  /** Human-readable reason label for the move. */
  reason_label?: string;
  /** Thumbnail for the moved edition. */
  thumb_url?: string | null;
  /** Thumbnail for the dedupe winner, when available. */
  winner_thumb_url?: string | null;
  /** Full move review payload (winner/loser analysis or incomplete diagnostics). */
  details?: Record<string, unknown>;
}

export interface ScanMovesSummary {
  scan_id: number;
  total_moved: number;
  pending: number;
  restored: number;
  size_mb: number;
  first_moved_at?: number | null;
  last_moved_at?: number | null;
  by_reason: Record<string, { total_moved: number; pending: number; restored: number; size_mb: number }>;
}

export type ScanPipelineOutcome =
  | 'matched'
  | 'provider_only'
  | 'unmatched'
  | 'duplicate_winner'
  | 'duplicate_loser'
  | 'duplicate_candidate'
  | 'incomplete'
  | 'moved_duplicate'
  | 'moved_incomplete'
  | 'restored_duplicate'
  | 'restored_incomplete';

export interface ScanPipelineTraceRow {
  scan_id: number;
  artist: string;
  album_id: number;
  album_title: string;
  folder: string;
  folder_name: string;
  fmt_text?: string;
  metadata_source?: string;
  strict_match_verified: boolean;
  strict_match_provider?: string;
  strict_reject_reason?: string;
  strict_tracklist_score?: number;
  has_cover: boolean;
  is_broken: boolean;
  expected_track_count?: number;
  actual_track_count?: number;
  missing_indices: Array<number | string | unknown[]>;
  missing_required_tags: string[];
  providers: {
    musicbrainz: boolean;
    discogs: boolean;
    lastfm: boolean;
    bandcamp: boolean;
  };
  provider_refs: {
    musicbrainz_release_id?: string;
    discogs_release_id?: string;
    lastfm_album_mbid?: string;
    bandcamp_album_url?: string;
  };
  dupe_role: string;
  dupe_signal?: string;
  dupe_peer_count?: number;
  dupe_needs_ai?: boolean;
  no_move?: boolean;
  manual_review?: boolean;
  same_folder?: boolean;
  winner_album_id?: number | null;
  winner_title?: string;
  ai_used?: boolean;
  ai_provider?: string;
  ai_model?: string;
  pipeline_status: ScanPipelineOutcome | string;
  move_reason?: string;
  move_status?: string;
  moved_to_path?: string;
  decision_provider?: string;
  decision_reason?: string;
  decision_confidence?: number | null;
  timeline: Array<{ stage: string; label: string; tone?: string }>;
  meta_summary?: Record<string, unknown>;
  updated_at: number;
}

export interface ScanPipelineTraceResponse {
  scan_id: number;
  page: number;
  page_size: number;
  total: number;
  items: ScanPipelineTraceRow[];
  summary: {
    total: number;
    strict_matches: number;
    provider_matches: number;
    broken: number;
    duplicate_winners: number;
    duplicate_losers: number;
    moved_duplicates: number;
    moved_incompletes: number;
    ai_touched: number;
    status_counts: Record<string, number>;
    provider_counts: Record<string, number>;
  };
}

type ApiError = Error & { response?: Response; body?: unknown };

const isApiError = (error: unknown): error is ApiError => {
  return error instanceof Error && typeof (error as ApiError).response !== 'undefined';
};

const GET_REQUEST_TIMEOUT_MS = 12000;
const inFlightGetRequests = new Map<string, Promise<unknown>>();

type FetchApiRequestInit = RequestInit & {
  timeoutMs?: number;
};

function createFetchSignal(
  externalSignal?: AbortSignal,
  timeoutMs: number = 0,
): { signal?: AbortSignal; cleanup: () => void } {
  if (!externalSignal && timeoutMs <= 0) {
    return { signal: undefined, cleanup: () => {} };
  }

  const controller = new AbortController();
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  const onExternalAbort = () => {
    if (!controller.signal.aborted) controller.abort();
  };

  if (externalSignal) {
    if (externalSignal.aborted) {
      controller.abort();
    } else {
      externalSignal.addEventListener('abort', onExternalAbort, { once: true });
    }
  }

  if (timeoutMs > 0) {
    timeoutId = setTimeout(() => {
      if (!controller.signal.aborted) controller.abort();
    }, timeoutMs);
  }

  return {
    signal: controller.signal,
    cleanup: () => {
      if (timeoutId) clearTimeout(timeoutId);
      if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
    },
  };
}

async function executeFetchApi<T>(endpoint: string, options?: FetchApiRequestInit): Promise<T> {
  const method = String(options?.method ?? 'GET').toUpperCase();
  const customTimeout = Number(options?.timeoutMs || 0);
  const timeoutMs = method === 'GET' ? (customTimeout > 0 ? customTimeout : GET_REQUEST_TIMEOUT_MS) : Math.max(0, customTimeout);
  const { timeoutMs: _ignoredTimeoutMs, ...requestOptions } = options || {};
  const { signal, cleanup } = createFetchSignal(requestOptions?.signal, timeoutMs);
  let timedOut = false;
  try {
    const headers = new Headers(withAuthHeaders(requestOptions?.headers));
    if (!headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
    }
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      ...requestOptions,
      signal: signal ?? requestOptions?.signal,
      headers,
    });
    
    if (!response.ok) {
      const error = new Error(`API Error: ${response.status} ${response.statusText}`) as ApiError;
      error.response = response;
      try {
        const data = await response.json();
        error.body = data;
      } catch {
        error.body = {};
      }
      throw error;
    }

    if (response.status === 204) {
      return undefined as T;
    }
    const ct = response.headers.get('content-type');
    if (ct?.includes('application/json')) {
      const data = await response.json();
      return normalizePmdaPayloadUrls(data);
    }
    return undefined as T;
  } catch (error) {
    timedOut = Boolean((error as { name?: string } | null)?.name === 'AbortError');
    if (timedOut) {
      throw new Error(`API timeout after ${Math.round(timeoutMs / 1000)}s for ${endpoint}`);
    }
    throw error;
  } finally {
    cleanup();
  }
}

// API Functions
async function fetchApi<T>(endpoint: string, options?: FetchApiRequestInit): Promise<T> {
  const method = String(options?.method ?? 'GET').toUpperCase();
  if (method !== 'GET') {
    return executeFetchApi<T>(endpoint, options);
  }
  const timeoutKey = Number(options?.timeoutMs || 0);
  const key = `${method}:${API_BASE_URL}${endpoint}:timeout=${timeoutKey}`;
  const inFlight = inFlightGetRequests.get(key) as Promise<T> | undefined;
  if (inFlight) return inFlight;
  const request = executeFetchApi<T>(endpoint, options).finally(() => {
    inFlightGetRequests.delete(key);
  });
  inFlightGetRequests.set(key, request as Promise<unknown>);
  return request;
}

// Library stats (for Unduper when no scan results)
export interface LibraryStats {
  artists: number;
  albums: number;
  tracks?: number;
  scope?: LibraryBrowseScope;
  fallback_source?: string | null;
  index_running?: boolean;
  index_phase?: string;
}

export type LibraryBrowseScope = 'library' | 'inbox' | 'dupes' | 'all';

interface LibraryBrowseOptions {
  includeUnmatched?: boolean;
  scope?: LibraryBrowseScope;
}

function appendLibraryBrowseOptions(q: URLSearchParams, options?: LibraryBrowseOptions) {
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
  if (options?.scope) q.set('scope', options.scope);
}

export async function getLibraryStats(options?: LibraryBrowseOptions): Promise<LibraryStats> {
  const q = new URLSearchParams();
  appendLibraryBrowseOptions(q, options);
  const qs = q.toString();
  return fetchApi<LibraryStats>(`/api/library/stats${qs ? `?${qs}` : ''}`);
}

export interface LibraryStatsYearPoint {
  year: number;
  count: number;
}

export interface LibraryStatsGrowthPoint {
  month: string; // YYYY-MM
  count: number;
}

export interface LibraryStatsGenreItem {
  genre: string;
  count: number;
}

export interface LibraryStatsLabelItem {
  label: string;
  count: number;
}

export interface LibraryStatsFormatItem {
  format: string;
  count: number;
}

export interface LibraryStatsSourcePathItem {
  path: string;
  albums: number;
  artists: number;
  labels: number;
  tracks: number;
  albums_pct: number;
  artists_pct: number;
  labels_pct: number;
}

export interface LibraryStatsLibraryResponse {
  artists: number;
  albums: number;
  tracks: number;
  years: LibraryStatsYearPoint[];
  growth: LibraryStatsGrowthPoint[];
  genres: LibraryStatsGenreItem[];
  labels: LibraryStatsLabelItem[];
  formats: LibraryStatsFormatItem[];
  source_paths?: LibraryStatsSourcePathItem[];
  quality: {
    with_cover: number;
    without_cover: number;
    lossless: number;
    lossy: number;
  };
}

export async function getLibraryStatsLibrary(): Promise<LibraryStatsLibraryResponse> {
  return fetchApi<LibraryStatsLibraryResponse>('/api/library/stats/library');
}

export interface ScanMovesAuditResponse {
  scan_id: number;
  db_path: string;
  strict_columns_present: boolean;
  summary: Record<string, number>;
  strict_provider_counts: Record<string, number>;
  examples: {
    edition_missing_sample: string[];
    strict_no_sample: Array<{
      original_path: string;
      provider?: string;
      reject_reason?: string;
    }>;
  };
}

export async function getScanMovesAudit(scanId?: number): Promise<ScanMovesAuditResponse> {
  const q = new URLSearchParams();
  if (scanId != null && Number.isFinite(scanId) && scanId > 0) q.set('scan_id', String(Math.floor(scanId)));
  const qs = q.toString();
  return fetchApi<ScanMovesAuditResponse>(`/api/statistics/scan-moves-audit${qs ? `?${qs}` : ''}`);
}

export type LibraryDiscoverSectionKey = 'genre' | 'artists' | 'similar' | 'labels' | 'year' | 'rated' | 'heat' | 'random';

export interface LibraryDiscoverSection {
  key: LibraryDiscoverSectionKey;
  title: string;
  reason: string;
  seed?: Record<string, unknown>;
  albums: LibraryAlbumItem[];
}

export interface LibraryDiscoverResponse {
  days: number;
  limit: number;
  generated_at: number;
  sections: LibraryDiscoverSection[];
}

export async function getLibraryDiscover(days = 90, limit = 18, refresh = false): Promise<LibraryDiscoverResponse> {
  return getLibraryDiscoverWithOptions(days, limit, refresh, undefined);
}

export async function getLibraryDiscoverWithOptions(
  days = 90,
  limit = 18,
  refresh = false,
  options?: LibraryBrowseOptions
): Promise<LibraryDiscoverResponse> {
  const qs = new URLSearchParams();
  qs.set('days', String(Math.max(7, Math.min(365, days))));
  qs.set('limit', String(Math.max(6, Math.min(36, limit))));
  if (refresh) qs.set('refresh', '1');
  appendLibraryBrowseOptions(qs, options);
  return fetchApi<LibraryDiscoverResponse>(`/api/library/discover?${qs.toString()}`);
}

export type LibrarySearchItemType =
  | 'artist'
  | 'album'
  | 'track'
  | 'genre'
  | 'composer'
  | 'conductor'
  | 'orchestra';

export interface LibrarySearchSuggestionItem {
  type: LibrarySearchItemType;
  title: string;
  subtitle?: string;
  thumb?: string | null;
  search_query?: string;
  artist_id?: number;
  album_id?: number;
  track_id?: number;
  duration_sec?: number;
  track_num?: number;
}

export interface LibrarySearchSuggestResponse {
  query: string;
  items: LibrarySearchSuggestionItem[];
}

export async function getLibrarySearchSuggest(query: string, limit = 12): Promise<LibrarySearchSuggestResponse> {
  return getLibrarySearchSuggestWithOptions(query, limit, undefined);
}

export async function getLibrarySearchSuggestWithOptions(
  query: string,
  limit = 12,
  options?: LibraryBrowseOptions
): Promise<LibrarySearchSuggestResponse> {
  if (!query.trim()) return { query: '', items: [] };
  const qs = new URLSearchParams();
  qs.set('q', query.trim());
  qs.set('limit', String(Math.max(1, Math.min(40, limit))));
  appendLibraryBrowseOptions(qs, options);
  return fetchApi<LibrarySearchSuggestResponse>(
    `/api/library/search/suggest?${qs.toString()}`
  );
}

export interface LibraryAlbumItem {
  album_id: number;
  title: string;
  year?: number | null;
  genre?: string | null;
  genres?: string[] | null;
  label?: string | null;
  track_count: number;
  format?: string | null;
  is_lossless: boolean;
  sample_rate?: number | null;
  bit_depth?: number | null;
  mb_identified?: boolean;
  musicbrainz_release_group_id?: string | null;
  discogs_release_id?: string | null;
  lastfm_album_mbid?: string | null;
  bandcamp_album_url?: string | null;
  metadata_source?: string | null;
  strict_match_provider?: string | null;
  thumb?: string | null;
  artist_id: number;
  artist_name: string;
  short_description?: string | null;
  profile_source?: string | null;
  user_rating?: number | null;
  public_rating?: number | null;
  public_rating_votes?: number | null;
  public_rating_source?: string | null;
  heat_score?: number | null;
  classical?: ClassicalIdentityPayload | null;
}

export interface LibraryAlbumsResponse {
  albums: LibraryAlbumItem[];
  total: number;
  limit: number;
  offset: number;
  error?: string;
}

export interface LibraryDigestResponse {
  limit: number;
  generated_at: number;
  albums: LibraryAlbumItem[];
  enrichment?: {
    triggered?: boolean;
    missing_total?: number;
    available_total?: number;
    active_jobs?: number;
    profile_backfill?: {
      running?: boolean;
      reason?: string;
      started_at?: number;
      finished_at?: number;
      current?: number;
      total?: number;
      current_artist?: string;
      errors?: number;
    };
    error?: string;
  };
}

export async function getLibraryDigest(limit = 12, trigger = true): Promise<LibraryDigestResponse> {
  return getLibraryDigestWithOptions(limit, trigger, undefined);
}

export async function getLibraryDigestWithOptions(
  limit = 12,
  trigger = true,
  options?: LibraryBrowseOptions
): Promise<LibraryDigestResponse> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(36, limit))));
  if (!trigger) q.set('trigger', '0');
  appendLibraryBrowseOptions(q, options);
  return fetchApi<LibraryDigestResponse>(`/api/library/digest?${q.toString()}`);
}

export async function getLibraryAlbums(options?: {
  search?: string;
  sort?: 'recent' | 'year_desc' | 'alpha' | 'artist' | 'user_rating' | 'public_rating' | 'heat';
  genre?: string;
  label?: string;
  year?: number;
  limit?: number;
  offset?: number;
  includeUnmatched?: boolean;
  scope?: LibraryBrowseScope;
}): Promise<LibraryAlbumsResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.sort) q.set('sort', options.sort);
  if (options?.genre) q.set('genre', options.genre);
  if (options?.label) q.set('label', options.label);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(240, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  appendLibraryBrowseOptions(q, options);
  const qs = q.toString();
  return fetchApi<LibraryAlbumsResponse>(`/api/library/albums${qs ? `?${qs}` : ''}`);
}

export interface LibraryFacetItem {
  value: string;
  count: number;
  thumb?: string | null;
}

export interface LibraryFacetYearItem {
  value: number;
  count: number;
}

export interface LibraryFacetsResponse {
  genres: LibraryFacetItem[];
  labels: LibraryFacetItem[];
  years: LibraryFacetYearItem[];
  error?: string;
}

export async function getLibraryFacets(options?: LibraryBrowseOptions): Promise<LibraryFacetsResponse> {
  const q = new URLSearchParams();
  appendLibraryBrowseOptions(q, options);
  const qs = q.toString();
  return fetchApi<LibraryFacetsResponse>(`/api/library/facets${qs ? `?${qs}` : ''}`);
}

export interface LibraryGenresSuggestResponse {
  query: string;
  genres: LibraryFacetItem[];
  error?: string;
}

export async function suggestLibraryGenres(
  query = '',
  limit = 16,
  refresh = false,
  filters?: { label?: string; year?: number | null; includeUnmatched?: boolean; scope?: LibraryBrowseScope }
): Promise<LibraryGenresSuggestResponse> {
  const q = new URLSearchParams();
  if (query.trim()) q.set('q', query.trim());
  q.set('limit', String(Math.max(1, Math.min(80, limit))));
  if (refresh) q.set('refresh', '1');
  if (filters?.label) q.set('label', String(filters.label));
  if (filters?.year != null) q.set('year', String(filters.year));
  appendLibraryBrowseOptions(q, filters);
  return fetchApi<LibraryGenresSuggestResponse>(`/api/library/genres/suggest?${q.toString()}`);
}

export interface LibraryLabelsSuggestResponse {
  query: string;
  labels: LibraryFacetItem[];
  error?: string;
}

export async function suggestLibraryLabels(
  query = '',
  limit = 16,
  refresh = false,
  filters?: { genre?: string; year?: number | null; includeUnmatched?: boolean; scope?: LibraryBrowseScope }
): Promise<LibraryLabelsSuggestResponse> {
  const q = new URLSearchParams();
  if (query.trim()) q.set('q', query.trim());
  q.set('limit', String(Math.max(1, Math.min(80, limit))));
  if (refresh) q.set('refresh', '1');
  if (filters?.genre) q.set('genre', String(filters.genre));
  if (filters?.year != null) q.set('year', String(filters.year));
  appendLibraryBrowseOptions(q, filters);
  return fetchApi<LibraryLabelsSuggestResponse>(`/api/library/labels/suggest?${q.toString()}`);
}

export interface GenreLabelItem {
  label: string;
  count: number;
}

export interface LibraryGenreLabelsResponse {
  genre: string;
  album_count: number;
  labels: GenreLabelItem[];
  error?: string;
}

export async function getLibraryGenreLabels(
  genre: string,
  limit = 80,
  refresh = false,
  options?: LibraryBrowseOptions
): Promise<LibraryGenreLabelsResponse> {
  const g = genre.trim();
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (refresh) q.set('refresh', '1');
  appendLibraryBrowseOptions(q, options);
  return fetchApi<LibraryGenreLabelsResponse>(`/api/library/genre/${encodeURIComponent(g)}/labels?${q.toString()}`);
}

export interface RecentlyPlayedAlbumItem extends LibraryAlbumItem {
  last_played_at?: number;
}

export interface LibraryRecentlyPlayedAlbumsResponse {
  days: number;
  total?: number;
  limit: number;
  offset?: number;
  generated_at: number;
  source?: 'playback' | 'reco';
  albums: RecentlyPlayedAlbumItem[];
  error?: string;
}

export async function getLibraryRecentlyPlayedAlbums(days = 90, limit = 18, refresh = false): Promise<LibraryRecentlyPlayedAlbumsResponse> {
  return getLibraryRecentlyPlayedAlbumsWithOptions(days, limit, refresh, undefined);
}

export async function getLibraryRecentlyPlayedAlbumsWithOptions(
  days = 90,
  limit = 18,
  refresh = false,
  options?: { includeUnmatched?: boolean; offset?: number; scope?: LibraryBrowseScope }
): Promise<LibraryRecentlyPlayedAlbumsResponse> {
  const q = new URLSearchParams();
  q.set('days', String(Math.max(7, Math.min(365, days))));
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (refresh) q.set('refresh', '1');
  appendLibraryBrowseOptions(q, options);
  return fetchApi<LibraryRecentlyPlayedAlbumsResponse>(`/api/library/recently-played/albums?${q.toString()}`);
}

export interface LibraryArtistItem {
  artist_id: number;
  artist_name: string;
  album_count: number;
  broken_albums_count?: number;
  artist_thumb?: string | null;
  artist_has_image?: boolean;
}

export interface LibraryArtistsResponse {
  artists: LibraryArtistItem[];
  total: number;
  limit: number;
  offset: number;
  error?: string;
}

export async function getLibraryArtists(options?: {
  search?: string;
  sort?: 'recent' | 'alpha' | 'albums' | 'relevance';
  genre?: string;
  label?: string;
  year?: number;
  limit?: number;
  offset?: number;
  includeUnmatched?: boolean;
  scope?: LibraryBrowseScope;
  refresh?: boolean;
}): Promise<LibraryArtistsResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.sort) q.set('sort', options.sort);
  if (options?.genre) q.set('genre', options.genre);
  if (options?.label) q.set('label', options.label);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(500, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  appendLibraryBrowseOptions(q, options);
  if (options?.refresh) q.set('refresh', '1');
  const qs = q.toString();
  return fetchApi<LibraryArtistsResponse>(`/api/library/artists${qs ? `?${qs}` : ''}`);
}

export interface LibraryGenresResponse {
  genres: LibraryFacetItem[];
  total: number;
  limit: number;
  offset: number;
  error?: string;
}

export async function getLibraryGenres(options?: {
  search?: string;
  label?: string;
  year?: number;
  limit?: number;
  offset?: number;
  includeUnmatched?: boolean;
  scope?: LibraryBrowseScope;
}): Promise<LibraryGenresResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.label) q.set('label', options.label);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(200, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  appendLibraryBrowseOptions(q, options);
  const qs = q.toString();
  return fetchApi<LibraryGenresResponse>(`/api/library/genres${qs ? `?${qs}` : ''}`);
}

export interface LibraryLabelsResponse {
  labels: LibraryFacetItem[];
  total: number;
  limit: number;
  offset: number;
  error?: string;
}

export async function getLibraryLabels(options?: {
  search?: string;
  genre?: string;
  year?: number;
  limit?: number;
  offset?: number;
  includeUnmatched?: boolean;
  scope?: LibraryBrowseScope;
  refresh?: boolean;
}): Promise<LibraryLabelsResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.genre) q.set('genre', options.genre);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(200, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  appendLibraryBrowseOptions(q, options);
  if (options?.refresh) q.set('refresh', '1');
  const qs = q.toString();
  return fetchApi<LibraryLabelsResponse>(`/api/library/labels${qs ? `?${qs}` : ''}`);
}

export interface TopArtistItem {
  artist_id: number;
  artist_name: string;
  album_count: number;
  completion_count: number;
  play_count: number;
  thumb?: string | null;
}

export interface TopArtistsResponse {
  artists: TopArtistItem[];
  total?: number;
  offset?: number;
  limit?: number;
  days?: number;
  error?: string;
}

export async function getTopArtists(
  limit = 18,
  days = 0,
  options?: { includeUnmatched?: boolean; offset?: number; scope?: LibraryBrowseScope }
): Promise<TopArtistsResponse> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (days && days > 0) q.set('days', String(Math.max(1, Math.min(3650, days))));
  appendLibraryBrowseOptions(q, options);
  return fetchApi<TopArtistsResponse>(`/api/library/artists/top?${q.toString()}`);
}

export interface RecentlyAddedArtistItem {
  artist_id: number;
  artist_name: string;
  album_count: number;
  thumb?: string | null;
  last_added_at?: number;
}

export interface RecentlyAddedArtistsResponse {
  artists: RecentlyAddedArtistItem[];
  limit?: number;
  offset?: number;
  error?: string;
}

export async function getRecentlyAddedArtists(limit = 18, offset = 0, options?: { includeUnmatched?: boolean; scope?: LibraryBrowseScope }): Promise<RecentlyAddedArtistsResponse> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(60, limit))));
  q.set('offset', String(Math.max(0, offset)));
  appendLibraryBrowseOptions(q, options);
  return fetchApi<RecentlyAddedArtistsResponse>(`/api/library/artists/recent?${q.toString()}`);
}

export type LikeEntityType = 'artist' | 'album' | 'track' | 'label' | 'genre' | 'playlist';

export interface LikeItem {
  entity_id: number;
  entity_key?: string | null;
  liked: boolean;
  updated_at: number;
}

export interface LikesResponse {
  entity_type: LikeEntityType;
  items: LikeItem[];
  error?: string;
}

export async function getLikes(entityType: LikeEntityType, ids?: number[], keys?: string[]): Promise<LikesResponse> {
  const q = new URLSearchParams();
  q.set('entity_type', entityType);
  if (ids && ids.length > 0) q.set('ids', ids.join(','));
  if (keys && keys.length > 0) q.set('keys', keys.join(','));
  return fetchApi<LikesResponse>(`/api/library/likes?${q.toString()}`);
}

export async function setLike(payload: { entity_type: LikeEntityType; entity_id?: number; entity_key?: string; liked: boolean; source?: string }): Promise<{ entity_type: LikeEntityType; entity_id: number; entity_key?: string | null; liked: boolean; updated_at: number }> {
  return fetchApi(`/api/library/likes`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export interface SocialUser {
  id: number;
  username: string;
  is_admin: boolean;
  can_download: boolean;
  can_view_statistics: boolean;
  allow_ai_calls?: boolean;
  is_active: boolean;
  accept_shares?: boolean;
  share_liked_public?: boolean;
  share_recommendations_public?: boolean;
  avatar_data_url?: string | null;
  created_at: number;
  updated_at: number;
  last_login_at?: number | null;
}

export interface LikedSummaryResponse {
  owner?: SocialUser | null;
  tracks: Array<{
    track_id: number;
    title: string;
    artist_id: number;
    artist_name: string;
    album_id: number;
    album_title: string;
    duration_sec: number;
    track_num?: number;
    disc_num?: number;
    thumb?: string | null;
    updated_at: number;
  }>;
  albums: LibraryAlbumItem[];
  artists: RecentlyAddedArtistItem[];
  labels: Array<{ label: string; updated_at: number }>;
  recommended_albums: LibraryAlbumItem[];
}

export interface ShareEntityPayload {
  entity_type: LikeEntityType;
  entity_id?: number;
  entity_key?: string;
  recipient_user_ids: number[];
  message?: string;
  parent_recommendation_id?: number;
}

export interface RecommendationItem {
  recommendation_id: number;
  sender_user_id: number;
  sender_username: string;
  recipient_user_id: number;
  recipient_username: string;
  entity_type: LikeEntityType;
  entity_id?: number | null;
  entity_key?: string | null;
  entity_label: string;
  entity_subtitle?: string | null;
  entity_href?: string | null;
  entity_thumb?: string | null;
  entity_meta?: Record<string, unknown>;
  message?: string | null;
  parent_recommendation_id?: number | null;
  liked_by_recipient: boolean;
  created_at: number;
  read_at?: number | null;
  status: string;
}

export interface RecommendationListResponse {
  owner?: SocialUser | null;
  received: RecommendationItem[];
  sent: RecommendationItem[];
  unread_count: number;
}

export interface SocialContextResponse {
  liked_by: SocialUser[];
  recommended_by: SocialUser[];
}

export interface UserNotificationItem {
  notification_id: number;
  actor_user_id?: number | null;
  actor_username?: string | null;
  kind: string;
  title: string;
  body: string;
  entity_type?: LikeEntityType | null;
  entity_id?: number | null;
  entity_key?: string | null;
  recommendation_id?: number | null;
  payload?: Record<string, unknown>;
  is_read: boolean;
  created_at: number;
  read_at?: number | null;
}

export async function getLikedSummary(userId?: number): Promise<LikedSummaryResponse> {
  const q = new URLSearchParams();
  if (userId && userId > 0) q.set('user_id', String(userId));
  return fetchApi<LikedSummaryResponse>(`/api/library/liked${q.toString() ? `?${q.toString()}` : ''}`);
}

export async function getSocialUsers(mode: 'shares' | 'liked' | 'recommendations' = 'shares'): Promise<{ users: SocialUser[] }> {
  const q = new URLSearchParams();
  q.set('mode', mode);
  return fetchApi<{ users: SocialUser[] }>(`/api/library/social/users?${q.toString()}`);
}

export async function getSocialContext(input: {
  entity_type: 'artist' | 'album' | 'track' | 'label' | 'genre' | 'playlist';
  entity_id?: number;
  entity_key?: string;
}): Promise<SocialContextResponse> {
  const q = new URLSearchParams();
  q.set('entity_type', input.entity_type);
  if (input.entity_id && input.entity_id > 0) q.set('entity_id', String(input.entity_id));
  if (input.entity_key) q.set('entity_key', input.entity_key);
  return fetchApi<SocialContextResponse>(`/api/library/social/context?${q.toString()}`);
}

export async function shareLibraryEntity(payload: ShareEntityPayload): Promise<{ ok: boolean; count: number; recommendation_ids: number[] }> {
  return fetchApi('/api/library/share', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function getRecommendations(userId?: number): Promise<RecommendationListResponse> {
  const q = new URLSearchParams();
  if (userId && userId > 0) q.set('user_id', String(userId));
  return fetchApi<RecommendationListResponse>(`/api/library/recommendations${q.toString() ? `?${q.toString()}` : ''}`);
}

export async function likeRecommendation(recommendationId: number): Promise<{ ok: boolean; recommendation_id: number; liked_by_recipient: boolean }> {
  return fetchApi(`/api/library/recommendations/${encodeURIComponent(String(recommendationId))}/like`, {
    method: 'POST',
  });
}

export async function getNotifications(limit = 50, unreadOnly = false): Promise<{ notifications: UserNotificationItem[] }> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (unreadOnly) q.set('unread_only', '1');
  return fetchApi<{ notifications: UserNotificationItem[] }>(`/api/library/notifications?${q.toString()}`);
}

export async function markNotificationRead(notificationId: number): Promise<{ ok: boolean; notification_id: number }> {
  return fetchApi(`/api/library/notifications/${encodeURIComponent(String(notificationId))}/read`, {
    method: 'POST',
  });
}

export type RecoEventType = 'play_start' | 'play_partial' | 'play_complete' | 'skip' | 'stop' | 'like' | 'dislike';

export interface RecoTrack {
  track_id: number;
  title: string;
  artist_id: number;
  artist_name: string;
  album_id: number;
  album_title: string;
  duration_sec: number;
  track_num: number;
  score: number;
  reasons: string[];
  thumb?: string | null;
  file_url: string;
}

export interface RecoForYouResponse {
  session_id: string;
  total?: number;
  limit?: number;
  offset?: number;
  tracks: RecoTrack[];
  session_event_count?: number;
  algorithm?: string;
}

export async function getRecommendationsForYou(
  sessionId: string,
  limit = 12,
  excludeTrackId?: number,
  offset = 0
): Promise<RecoForYouResponse> {
  const q = new URLSearchParams();
  if (sessionId) q.set('session_id', sessionId);
  q.set('limit', String(Math.max(1, Math.min(120, limit))));
  q.set('offset', String(Math.max(0, offset)));
  if (excludeTrackId && excludeTrackId > 0) q.set('exclude_track_id', String(excludeTrackId));
  return fetchApi<RecoForYouResponse>(`/api/library/reco/for-you?${q.toString()}`);
}

export interface GenreProfileArtistItem {
  artist_id: number;
  artist_name: string;
  album_count: number;
  thumb?: string | null;
}

export interface GenreProfileResponse {
  genre: string;
  album_count: number;
  description?: string;
  wiki_url?: string;
  wiki_description?: string;
  source?: string;
  top_artists: GenreProfileArtistItem[];
  error?: string;
}

export async function getGenreProfile(
  genre: string,
  options?: { limit_artists?: number; refresh?: boolean; includeUnmatched?: boolean; scope?: LibraryBrowseScope }
): Promise<GenreProfileResponse> {
  const q = new URLSearchParams();
  if (options?.limit_artists != null) q.set('limit_artists', String(Math.max(1, Math.min(120, options.limit_artists))));
  if (options?.refresh) q.set('refresh', '1');
  appendLibraryBrowseOptions(q, options);
  return fetchApi<GenreProfileResponse>(`/api/library/genre/${encodeURIComponent(genre)}/profile?${q.toString()}`);
}

export interface LabelProfileArtistItem {
  artist_id: number;
  artist_name: string;
  album_count: number;
  thumb?: string | null;
}

export interface LabelProfileGenreItem {
  genre: string;
  count: number;
}

export interface LabelProfileResponse {
  label: string;
  album_count: number;
  description?: string;
  wiki_url?: string;
  wiki_description?: string;
  owner?: string;
  sub_labels?: string[];
  influential_artists: LabelProfileArtistItem[];
  genres: LabelProfileGenreItem[];
  discogs_profile?: string;
  discogs_url?: string;
  logo_url?: string;
  logo_provider?: string;
  error?: string;
}

export async function getLabelProfile(
  label: string,
  options?: { limit_artists?: number; limit_genres?: number; refresh?: boolean; includeUnmatched?: boolean; scope?: LibraryBrowseScope }
): Promise<LabelProfileResponse> {
  const q = new URLSearchParams();
  if (options?.limit_artists != null) q.set('limit_artists', String(Math.max(1, Math.min(120, options.limit_artists))));
  if (options?.limit_genres != null) q.set('limit_genres', String(Math.max(1, Math.min(120, options.limit_genres))));
  if (options?.refresh) q.set('refresh', '1');
  appendLibraryBrowseOptions(q, options);
  return fetchApi<LabelProfileResponse>(`/api/library/label/${encodeURIComponent(label)}/profile?${q.toString()}`);
}

export async function postRecommendationEvent(payload: {
  session_id: string;
  track_id: number;
  event_type: RecoEventType;
  played_seconds?: number;
}): Promise<{ ok: boolean; session_id: string; track_id: number; event_type: string }> {
  return fetchApi('/api/library/reco/event', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function postPlaybackEvent(payload: {
  track_id: number;
  event_type: RecoEventType;
  played_seconds?: number;
}): Promise<{ ok: boolean; track_id: number; event_type: string; played_seconds: number }> {
  return fetchApi('/api/library/playback/event', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export interface PlaybackTopArtist {
  artist_id: number;
  artist_name: string;
  seconds: number;
  plays: number;
}

export interface PlaybackTopTrack {
  track_id: number;
  track_title: string;
  artist_id: number;
  artist_name: string;
  album_id: number;
  album_title: string;
  seconds: number;
  plays: number;
}

export interface PlaybackTopGenre {
  genre: string;
  seconds: number;
}

export interface PlaybackDailyPoint {
  day: string; // YYYY-MM-DD
  seconds: number;
  plays: number;
}

export interface PlaybackEventTypeCount {
  event_type: string;
  count: number;
}

export interface PlaybackHourPoint {
  hour: number; // 0-23
  seconds: number;
}

export interface PlaybackStatsResponse {
  days: number;
  total_seconds: number;
  events: number;
  distinct_tracks: number;
  top_artists: PlaybackTopArtist[];
  top_tracks: PlaybackTopTrack[];
  top_genres: PlaybackTopGenre[];
  daily: PlaybackDailyPoint[];
  event_types: PlaybackEventTypeCount[];
  hours: PlaybackHourPoint[];
}

export async function getPlaybackStats(days = 30): Promise<PlaybackStatsResponse> {
  const q = new URLSearchParams();
  q.set('days', String(Math.max(1, Math.min(365, days))));
  return fetchApi<PlaybackStatsResponse>(`/api/library/playback/stats?${q.toString()}`);
}

export interface ArtistSummaryPayload {
  artist_id: number;
  artist_name: string;
  original: { text: string; source: string; updated_at: number };
  ai: { text: string; source: string; provider: string; model: string; lang: string; updated_at: number };
}

export async function getArtistSummary(artistId: number): Promise<ArtistSummaryPayload> {
  return fetchApi<ArtistSummaryPayload>(`/api/library/artist/${encodeURIComponent(String(artistId))}/summary`);
}

export async function generateArtistAiSummary(artistId: number, lang?: string): Promise<{ artist_id: number; artist_name: string; ai: { text: string; source: string; provider: string; model: string; lang: string; updated_at: number } }> {
  return fetchApi(`/api/library/artist/${encodeURIComponent(String(artistId))}/summary/ai`, {
    method: 'POST',
    body: JSON.stringify(lang ? { lang } : {}),
  });
}

export interface ArtistConcertVenue {
  name: string;
  city: string;
  region?: string;
  country?: string;
  latitude?: string;
  longitude?: string;
}

export interface ArtistConcertEvent {
  provider: 'bandsintown' | string;
  id?: string;
  datetime?: string;
  title?: string;
  url?: string;
  lineup?: string[];
  venue?: ArtistConcertVenue;
}

export interface ArtistConcertsResponse {
  artist_id: number;
  artist_name: string;
  provider: string;
  events: ArtistConcertEvent[];
  source_url?: string | null;
  updated_at: number;
  cached?: boolean;
}

export async function getArtistConcerts(artistId: number, options?: { refresh?: boolean }): Promise<ArtistConcertsResponse> {
  const q = new URLSearchParams();
  if (options?.refresh) q.set('refresh', '1');
  const qs = q.toString();
  return fetchApi<ArtistConcertsResponse>(`/api/library/artist/${encodeURIComponent(String(artistId))}/concerts${qs ? `?${qs}` : ''}`);
}

export interface ArtistFactsResponse {
  artist_id: number;
  artist_name: string;
  facts: Record<string, unknown>;
  evidence: Array<{ fact_path?: string; excerpt?: string; source?: string }>;
  source: string;
  provider: string;
  model: string;
  updated_at: number;
}

export async function getArtistFacts(artistId: number): Promise<ArtistFactsResponse> {
  return fetchApi<ArtistFactsResponse>(`/api/library/artist/${encodeURIComponent(String(artistId))}/facts`);
}

export async function extractArtistFacts(artistId: number): Promise<ArtistFactsResponse> {
  return fetchApi<ArtistFactsResponse>(`/api/library/artist/${encodeURIComponent(String(artistId))}/facts/extract`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
}

export interface AlbumDetailTrack {
  track_id: number;
  title: string;
  disc_num: number;
  track_num: number;
  disc_label?: string | null;
  duration_sec: number;
  format?: string;
  bitrate?: number;
  sample_rate?: number;
  bit_depth?: number;
  file_size_bytes?: number;
  file_path?: string;
  featured?: string;
  file_url: string;
}

export interface AlbumDetailReview {
  description?: string;
  short_description?: string;
  source?: string;
  updated_at?: number;
}

export interface AlbumBandcampSupporterComment {
  author?: string | null;
  text?: string | null;
  url?: string | null;
  avatar_url?: string | null;
}

export interface ClassicalIdentityPayload {
  is_classical?: boolean;
  composer?: string[];
  work?: string[];
  conductor?: string[];
  orchestra?: string[];
  ensemble?: string[];
  soloists?: string[];
  performers?: string[];
  catalog_numbers?: string[];
}

export interface AlbumArtworkGalleryItem {
  id: string;
  slot: string;
  label: string;
  origin: string;
  provider?: string | null;
  selected?: boolean;
  source_name?: string | null;
  source_url?: string | null;
  image_url: string;
  thumb_url: string;
}

export interface AlbumArtworkGalleryResponse {
  album_id: number;
  title?: string | null;
  updated_at?: number | null;
  items: AlbumArtworkGalleryItem[];
}

export interface AlbumDetailResponse {
  album_id: number;
  title: string;
  created_at?: number | null;
  updated_at?: number | null;
  year?: number | null;
  date_text?: string;
  genre?: string;
  label?: string;
  format?: string;
  is_lossless?: boolean;
  sample_rate?: number | null;
  bit_depth?: number | null;
  track_count: number;
  total_duration_sec: number;
  has_cover?: boolean;
  cover_url?: string | null;
  bandcamp_album_url?: string | null;
  metadata_source?: string | null;
  metadata_source_url?: string | null;
  artist_id: number;
  artist_name: string;
  classical?: ClassicalIdentityPayload | null;
  review?: AlbumDetailReview;
  ratings?: {
    user_rating?: number | null;
    public_rating?: number | null;
    public_rating_votes?: number | null;
    public_rating_source?: string | null;
    heat_score?: number | null;
    signals?: {
      discogs_have_count?: number | null;
      discogs_want_count?: number | null;
      bandcamp_supporter_count?: number | null;
      bandcamp_supporter_comments?: AlbumBandcampSupporterComment[] | null;
      lastfm_scrobbles?: number | null;
      lastfm_listeners?: number | null;
    };
  };
  tracks: AlbumDetailTrack[];
}

export async function getAlbumDetail(albumId: number, options?: { refresh?: boolean }): Promise<AlbumDetailResponse> {
  const q = new URLSearchParams();
  if (options?.refresh) q.set('refresh', '1');
  const qs = q.toString();
  return fetchApi<AlbumDetailResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}${qs ? `?${qs}` : ''}`, {
    timeoutMs: 30000,
  });
}

export async function getAlbumArtworkGallery(albumId: number): Promise<AlbumArtworkGalleryResponse> {
  return fetchApi<AlbumArtworkGalleryResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/artwork-gallery`, {
    timeoutMs: 30000,
  });
}

export interface AlbumUserRatingResponse {
  album_id: number;
  user_id: number;
  rating?: number | null;
  updated_at?: number | null;
}

export async function getAlbumRating(albumId: number): Promise<AlbumUserRatingResponse> {
  return fetchApi<AlbumUserRatingResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/rating`);
}

export async function setAlbumRating(albumId: number, rating: number | null, source = 'ui'): Promise<AlbumUserRatingResponse> {
  const normalized = rating == null ? 0 : Math.max(0, Math.min(5, Math.round(rating)));
  return fetchApi<AlbumUserRatingResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/rating`, {
    method: 'PUT',
    body: JSON.stringify({ rating: normalized, source }),
  });
}

export interface AlbumTrackDetailItem extends AlbumDetailTrack {
  tags?: Record<string, string>;
  tags_error?: string | null;
}

export interface AlbumTrackDetailsResponse {
  album_id: number;
  artist_name: string;
  album_title: string;
  tracks: AlbumTrackDetailItem[];
}

export async function getAlbumTrackDetails(albumId: number, includeTags = true): Promise<AlbumTrackDetailsResponse> {
  const q = new URLSearchParams();
  q.set('include_tags', includeTags ? '1' : '0');
  return fetchApi<AlbumTrackDetailsResponse>(
    `/api/library/album/${encodeURIComponent(String(albumId))}/tracks/detail?${q.toString()}`,
    { timeoutMs: 60000 }
  );
}

export interface MatchProviderAttempt {
  provider: string;
  label: string;
  attempted: boolean;
  selected: boolean;
  id?: string | null;
  url?: string | null;
  notes?: string[];
}

export interface MatchAuditRecord {
  id: number;
  run_kind: string;
  status: string;
  match_type?: 'MATCH' | 'SOFT_MATCH' | 'NO_MATCH' | null;
  confidence?: number | null;
  ai_used: boolean;
  ai_confidence?: number | null;
  provider_used?: string | null;
  summary?: string | null;
  created_at: number;
  details?: Record<string, unknown>;
  album_id?: number;
}

export interface ProviderCrosscheckItem {
  provider: string;
  label: string;
  attempted: boolean;
  selected: boolean;
  provider_id?: string | null;
  source_url?: string | null;
  title?: string | null;
  artist?: string | null;
  year?: number | null;
  cover_url?: string | null;
  track_count: number;
  local_track_count: number;
  title_score: number;
  artist_score: number;
  track_score: number;
  confidence: number;
  strict_match_verified: boolean;
  strict_reject_reason?: string | null;
  soft_match_ok: boolean;
  soft_match_reason?: string | null;
  has_review: boolean;
  ai_used: boolean;
  ai_confidence?: number | null;
  versions_count?: number | null;
  versions?: Array<{
    id: string;
    title?: string | null;
    date?: string | null;
    country?: string | null;
    status?: string | null;
    url?: string | null;
  }>;
}

export interface AlbumAlternativeCover {
  provider: string;
  label: string;
  cover_url: string;
  source_url?: string | null;
  selected: boolean;
}

export interface AlbumMatchDetailResponse {
  album_id: number;
  album_title: string;
  artist_id: number;
  artist_name: string;
  year?: number | null;
  track_count: number;
  match_type: 'MATCH' | 'SOFT_MATCH' | 'NO_MATCH';
  confidence?: number | null;
  decision: {
    strict_match_verified: boolean;
    strict_match_provider?: string | null;
    strict_reject_reason?: string | null;
    strict_tracklist_score: number;
    selected_provider?: string | null;
    metadata_source?: string | null;
  };
  pmda: {
    id?: string | null;
    matched: boolean;
    cover: boolean;
    artist_image: boolean;
    complete: boolean;
    match_provider?: string | null;
    cover_provider?: string | null;
    artist_provider?: string | null;
  };
  ai?: {
    used: boolean;
    confidence?: number | null;
    source?: string | null;
  };
  identity: {
    musicbrainz_release_group_id?: string | null;
    discogs_release_id?: string | null;
    lastfm_album_mbid?: string | null;
    bandcamp_album_url?: string | null;
  };
  cover: {
    has_cover: boolean;
    provider?: string | null;
    origin: string;
    path?: string | null;
    url?: string | null;
  };
  artist_image: {
    has_image: boolean;
    provider?: string | null;
    mode: string;
    source_url?: string | null;
    url?: string | null;
  };
  description: {
    album_profile_source?: string | null;
    album_profile_updated_at?: number | null;
    artist_profile_source?: string | null;
    artist_profile_updated_at?: number | null;
    soft_match_ai_auto_enabled?: boolean;
  };
  providers_order: string[];
  provider_attempts: MatchProviderAttempt[];
  provider_crosscheck?: ProviderCrosscheckItem[];
  links: Array<{
    provider: string;
    label: string;
    url: string;
    release_title?: string | null;
    release_artist?: string | null;
    release_year?: number | null;
    provider_ref?: string | null;
  }>;
  alternative_covers?: AlbumAlternativeCover[];
  versions?: {
    has_alternatives: boolean;
    provider?: string | null;
    count: number;
    source_url?: string | null;
    items: Array<{
      id: string;
      title?: string | null;
      date?: string | null;
      country?: string | null;
      status?: string | null;
      url?: string | null;
    }>;
  };
  latest_manual_run?: MatchAuditRecord | null;
  history: MatchAuditRecord[];
  updated_at?: number | null;
}

export interface ArtistMatchDetailAlbum {
  album_id: number;
  album_title: string;
  year?: number | null;
  match_type: 'MATCH' | 'SOFT_MATCH' | 'NO_MATCH';
  confidence?: number | null;
  selected_provider?: string | null;
  strict_match_verified: boolean;
  strict_match_provider?: string | null;
  strict_reject_reason?: string | null;
  strict_tracklist_score: number;
  metadata_source?: string | null;
  identity: {
    musicbrainz_release_group_id?: string | null;
    discogs_release_id?: string | null;
    lastfm_album_mbid?: string | null;
    bandcamp_album_url?: string | null;
  };
  cover: {
    has_cover: boolean;
    origin: string;
    url?: string | null;
  };
  links: Array<{
    provider: string;
    label: string;
    url: string;
    release_title?: string | null;
    release_artist?: string | null;
    release_year?: number | null;
    provider_ref?: string | null;
  }>;
  updated_at?: number;
}

export interface ArtistMatchDetailResponse {
  artist_id: number;
  artist_name: string;
  album_count: number;
  artist_profile_source?: string | null;
  artist_profile_updated_at?: number | null;
  artist_image: {
    has_image: boolean;
    mode: string;
    provider?: string | null;
    source_url?: string | null;
    url?: string | null;
  };
  summary: {
    matched: number;
    soft_matched: number;
    not_matched: number;
    total: number;
  };
  albums: ArtistMatchDetailAlbum[];
  history: MatchAuditRecord[];
  updated_at?: number;
}

export interface RematchAlbumResponse {
  summary?: string;
  steps?: string[];
  tags_updated?: boolean;
  cover_saved?: boolean;
  provider_used?: string | null;
  files_updated?: number;
  mutation_blocked?: boolean;
  mutation_blocked_reason?: string;
  strict_match_verified?: boolean;
  strict_match_provider?: string;
  strict_reject_reason?: string;
  strict_tracklist_score?: number;
  audit_recorded?: boolean;
}

export interface GenerateAlbumReviewResponse {
  ok: boolean;
  album_id: number;
  artist_id?: number;
  source?: string;
  used_ai?: boolean;
  updated_at?: number;
}

export interface EnrichArtistAiResponse {
  started: boolean;
  artist_id: number;
  artist_name?: string;
  albums_total?: number;
  profiles_targeted?: number;
  mode?: string;
}

export async function getAlbumMatchDetail(albumId: number): Promise<AlbumMatchDetailResponse> {
  return fetchApi<AlbumMatchDetailResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/match-detail`);
}

export async function getArtistMatchDetail(artistId: number): Promise<ArtistMatchDetailResponse> {
  return fetchApi<ArtistMatchDetailResponse>(`/api/library/artist/${encodeURIComponent(String(artistId))}/match-detail`);
}

export async function rematchAlbum(albumId: number): Promise<RematchAlbumResponse> {
  return fetchApi<RematchAlbumResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/rematch`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
}

export async function generateAlbumReview(albumId: number): Promise<GenerateAlbumReviewResponse> {
  return fetchApi<GenerateAlbumReviewResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/review/generate`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
}

export async function enrichArtistWithAI(artistId: number): Promise<EnrichArtistAiResponse> {
  return fetchApi<EnrichArtistAiResponse>(`/api/library/artist/${encodeURIComponent(String(artistId))}/ai/enrich`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
}

export async function rematchArtist(artistId: number): Promise<{ started: boolean; total: number }> {
  return fetchApi<{ started: boolean; total: number }>(`/api/library/artist/${encodeURIComponent(String(artistId))}/rematch`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
}

export interface SelectAlbumCoverResponse {
  ok: boolean;
  album_id: number;
  provider: string;
  cover_url: string;
  cover_path: string;
  source_url?: string | null;
}

export async function selectAlbumCover(
  albumId: number,
  payload: { cover_url: string; provider?: string | null; source_url?: string | null }
): Promise<SelectAlbumCoverResponse> {
  return fetchApi<SelectAlbumCoverResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}/cover/select`, {
    method: 'POST',
    body: JSON.stringify({
      cover_url: payload.cover_url,
      provider: payload.provider || null,
      source_url: payload.source_url || null,
    }),
  });
}

function sanitizeDownloadPart(value: string): string {
  return String(value || '')
    .replace(/[\\/:*?"<>|]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseContentDispositionFilename(headerValue: string | null): string | null {
  const raw = String(headerValue || '').trim();
  if (!raw) return null;
  const utf8Match = raw.match(/filename\\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch {
      return utf8Match[1];
    }
  }
  const plainMatch = raw.match(/filename="?([^";]+)"?/i);
  if (plainMatch?.[1]) return plainMatch[1];
  return null;
}

export async function downloadAlbumZip(albumId: number, artistName?: string, albumTitle?: string): Promise<void> {
  const endpoint = `/api/library/album/${encodeURIComponent(String(albumId))}/download`;
  const response = await fetchWithAuth(endpoint, { method: 'GET' });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    const msg = (data as { error?: unknown }).error;
    throw new Error(typeof msg === 'string' && msg.trim() ? msg : `Download failed (${response.status})`);
  }
  const blob = await response.blob();
  const fallbackNameBase = [sanitizeDownloadPart(artistName || ''), sanitizeDownloadPart(albumTitle || '')]
    .filter(Boolean)
    .join(' - ') || `album-${albumId}`;
  const filename = parseContentDispositionFilename(response.headers.get('content-disposition')) || `${fallbackNameBase}.zip`;
  const objectUrl = window.URL.createObjectURL(blob);
  try {
    const anchor = document.createElement('a');
    anchor.href = objectUrl;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
  } finally {
    window.URL.revokeObjectURL(objectUrl);
  }
}

export interface PlaylistSummary {
  playlist_id: number;
  name: string;
  description?: string;
  item_count: number;
  updated_at: number;
}

export interface PlaylistsResponse {
  playlists: PlaylistSummary[];
}

export async function getPlaylists(): Promise<PlaylistsResponse> {
  return fetchApi<PlaylistsResponse>('/api/library/playlists');
}

export async function createPlaylist(payload: { name: string; description?: string }): Promise<PlaylistSummary> {
  return fetchApi<PlaylistSummary>('/api/library/playlists', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export interface PlaylistTrack {
  track_id: number;
  title: string;
  artist_id: number;
  artist_name: string;
  album_id: number;
  album_title: string;
  duration_sec: number;
  track_num: number;
  disc_num: number;
  thumb?: string | null;
  file_url: string;
}

export interface PlaylistItem {
  item_id: number;
  position: number;
  added_at: number;
  track: PlaylistTrack;
}

export interface PlaylistDetailResponse {
  playlist_id: number;
  name: string;
  description?: string;
  updated_at: number;
  items: PlaylistItem[];
}

export async function getPlaylist(playlistId: number): Promise<PlaylistDetailResponse> {
  return fetchApi<PlaylistDetailResponse>(`/api/library/playlists/${encodeURIComponent(String(playlistId))}`);
}

export async function deletePlaylist(playlistId: number): Promise<{ ok: boolean; playlist_id: number }> {
  return fetchApi<{ ok: boolean; playlist_id: number }>(`/api/library/playlists/${encodeURIComponent(String(playlistId))}`, { method: 'DELETE' });
}

export async function addPlaylistItems(
  playlistId: number,
  payload: { track_ids?: number[]; track_id?: number; album_id?: number }
): Promise<{ ok: boolean; playlist_id: number; inserted: number }> {
  return fetchApi<{ ok: boolean; playlist_id: number; inserted: number }>(`/api/library/playlists/${encodeURIComponent(String(playlistId))}/items`, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function deletePlaylistItem(
  playlistId: number,
  itemId: number
): Promise<{ ok: boolean; playlist_id: number; item_id: number }> {
  return fetchApi<{ ok: boolean; playlist_id: number; item_id: number }>(
    `/api/library/playlists/${encodeURIComponent(String(playlistId))}/items/${encodeURIComponent(String(itemId))}`,
    { method: 'DELETE' }
  );
}

export async function reorderPlaylist(
  playlistId: number,
  itemIds: number[]
): Promise<{ ok: boolean; playlist_id: number; count: number }> {
  return fetchApi<{ ok: boolean; playlist_id: number; count: number }>(
    `/api/library/playlists/${encodeURIComponent(String(playlistId))}/reorder`,
    { method: 'POST', body: JSON.stringify({ item_ids: itemIds }) }
  );
}

export interface AlbumWithParentheticalName {
  album_id: number;
  artist: string;
  title: string;
  current_path: string;
  proposed_path: string;
  current_name: string;
  proposed_name: string;
}

export async function getAlbumsWithParentheticalNames(): Promise<{ albums: AlbumWithParentheticalName[] }> {
  return fetchApi<{ albums: AlbumWithParentheticalName[] }>('/api/library/albums-with-parenthetical-names');
}

export async function normalizeAlbumNames(albumIds?: number[]): Promise<{ renamed: { album_id: number; from: string; to: string }[]; errors: { album_id?: number; message: string; path?: string }[] }> {
  return fetchApi<{ renamed: { album_id: number; from: string; to: string }[]; errors: { album_id?: number; message: string; path?: string }[] }>(
    '/api/library/normalize-album-names',
    { method: 'POST', body: JSON.stringify(albumIds != null ? { album_ids: albumIds } : {}) }
  );
}

// Duplicates (scan-only by default; use source=all for scan + library-only groups on large libraries)
export async function getDuplicates(options?: { source?: 'scan' | 'all' }): Promise<DuplicateCard[]> {
  const source = options?.source ?? 'scan';
  return fetchApi<DuplicateCard[]>(`/api/duplicates?source=${source}`);
}

export async function getDuplicateDetails(artist: string, albumId: string): Promise<DuplicateDetails> {
  const safeArtist = encodeURIComponent(artist.replace(/\s+/g, '_'));
  return fetchApi<DuplicateDetails>(`/details/${safeArtist}/${albumId}`);
}

// Scan
export async function getScanProgress(): Promise<ScanProgress> {
  return fetchApi<ScanProgress>('/api/progress');
}

export async function getCacheControlMetrics(force: boolean = false): Promise<CacheControlMetrics> {
  return fetchApi<CacheControlMetrics>(`/api/statistics/cache-control${force ? '?force=true' : ''}`);
}

export async function getEnrichmentSources(): Promise<EnrichmentSourcesResponse> {
  return fetchApi<EnrichmentSourcesResponse>('/api/statistics/enrichment-sources');
}

export async function getBenchmarkReports(limit: number = 40): Promise<BenchmarkReportsResponse> {
  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(200, Math.trunc(limit))) : 40;
  return fetchApi<BenchmarkReportsResponse>(`/api/statistics/benchmark-reports?limit=${safeLimit}`);
}

export async function getAIOverview(): Promise<AIOverviewResponse> {
  return fetchApi<AIOverviewResponse>('/api/ai/overview');
}

export async function getScalingRuntime(): Promise<ScalingRuntimeResponse> {
  return fetchApi<ScalingRuntimeResponse>('/api/statistics/scaling-runtime');
}

export interface ManagedRuntimeBootstrapBundleRequest {
  action?: 'auto' | 'create' | 'adopt';
  candidate_id?: string;
  mirror_name?: string;
  fast_model?: string;
  hard_model?: string;
}

export interface ManagedRuntimeBootstrapRequest {
  config_root?: string;
  data_root?: string;
  bundle_type?: ManagedRuntimeBundleType;
  payload?: ManagedRuntimeBootstrapBundleRequest;
  bundles?: Partial<Record<ManagedRuntimeBundleType, ManagedRuntimeBootstrapBundleRequest>>;
}

export interface ManagedRuntimeBootstrapResponse {
  results: Array<{
    bundle_type: ManagedRuntimeBundleType;
    started: boolean;
    message: string;
    status: ManagedRuntimeBundleStatus;
  }>;
  snapshot: ManagedRuntimeStatusResponse;
}

export interface ManagedRuntimeAdoptRequest {
  bundle_type: ManagedRuntimeBundleType;
  candidate_id?: string;
  project_name?: string;
  url?: string;
  config_root?: string;
  data_root?: string;
  fast_model?: string;
  hard_model?: string;
}

export interface ManagedRuntimeActionRequest {
  bundle_type: ManagedRuntimeBundleType;
  action:
    | 'refresh-health'
    | 'retry-bootstrap'
    | 'rebuild'
    | 'start'
    | 'stop'
    | 'restart'
    | 'reset'
    | 'retry-update'
    | 'pull-model';
  config_root?: string;
  data_root?: string;
  mirror_name?: string;
  fast_model?: string;
  hard_model?: string;
  model?: string;
}

export interface ManagedRuntimeLogsResponse {
  logs: Array<{
    log_id: number;
    bundle_type: string;
    service_name: string;
    level: string;
    message: string;
    created_at: number;
  }>;
  bundle_type?: string | null;
  service_name?: string | null;
}

export async function getManagedRuntimeStatus(options?: { skipCandidates?: boolean }): Promise<ManagedRuntimeStatusResponse> {
  const query = options?.skipCandidates ? '?skip_candidates=true' : '';
  return fetchApi<ManagedRuntimeStatusResponse>(`/api/runtime/managed/status${query}`, {
    timeoutMs: options?.skipCandidates ? 20000 : 45000,
  });
}

export async function bootstrapManagedRuntime(payload: ManagedRuntimeBootstrapRequest): Promise<ManagedRuntimeBootstrapResponse> {
  return fetchApi<ManagedRuntimeBootstrapResponse>('/api/runtime/managed/bootstrap', {
    method: 'POST',
    body: JSON.stringify(payload),
    timeoutMs: 60000,
  });
}

export async function adoptManagedRuntime(payload: ManagedRuntimeAdoptRequest): Promise<{ status: string; bundle_type: ManagedRuntimeBundleType; result: ManagedRuntimeBundleStatus; snapshot: ManagedRuntimeStatusResponse }> {
  return fetchApi<{ status: string; bundle_type: ManagedRuntimeBundleType; result: ManagedRuntimeBundleStatus; snapshot: ManagedRuntimeStatusResponse }>('/api/runtime/managed/adopt', {
    method: 'POST',
    body: JSON.stringify(payload),
    timeoutMs: 30000,
  });
}

export async function managedRuntimeAction(payload: ManagedRuntimeActionRequest): Promise<{ status: string; message?: string; bundle_type?: ManagedRuntimeBundleType; action?: string; result?: ManagedRuntimeBundleStatus; snapshot?: ManagedRuntimeStatusResponse; run_id?: string | null; active?: OllamaPullStatus }> {
  return fetchApi<{ status: string; message?: string; bundle_type?: ManagedRuntimeBundleType; action?: string; result?: ManagedRuntimeBundleStatus; snapshot?: ManagedRuntimeStatusResponse; run_id?: string | null; active?: OllamaPullStatus }>('/api/runtime/managed/action', {
    method: 'POST',
    body: JSON.stringify(payload),
    timeoutMs: 30000,
  });
}

export async function getManagedRuntimeLogs(options?: { bundleType?: ManagedRuntimeBundleType; serviceName?: string; limit?: number }): Promise<ManagedRuntimeLogsResponse> {
  const params = new URLSearchParams();
  if (options?.bundleType) params.set('bundle_type', options.bundleType);
  if (options?.serviceName) params.set('service_name', options.serviceName);
  if (typeof options?.limit === 'number') params.set('limit', String(options.limit));
  const query = params.toString() ? `?${params.toString()}` : '';
  return fetchApi<ManagedRuntimeLogsResponse>(`/api/runtime/managed/logs${query}`);
}

export async function getScanLogsTail(lines: number = 180): Promise<LogTailResponse> {
  const n = Number.isFinite(lines) ? Math.max(20, Math.min(1200, Math.trunc(lines))) : 180;
  return fetchApi<LogTailResponse>(`/api/logs/tail?lines=${n}`);
}

export interface ScanPreflightResult {
  musicbrainz: { ok: boolean; message: string };
  ai: { ok: boolean; message: string; provider: string };
  discogs?: { ok: boolean; message: string };
  lastfm?: { ok: boolean; message: string };
  fanart?: { ok: boolean; message: string };
  bandcamp?: { ok: boolean; message: string };
  serper?: { ok: boolean; message: string };
  acoustid?: { ok: boolean; message: string };
  paths?: { music_rw: boolean; dupes_rw: boolean };
}

export async function getScanPreflight(): Promise<ScanPreflightResult> {
  return fetchApi<ScanPreflightResult>('/api/scan/preflight', { timeoutMs: 45000 });
}

export async function getProvidersPreflight(): Promise<ScanPreflightResult> {
  return fetchApi<ScanPreflightResult>('/api/providers/preflight', { timeoutMs: 45000 });
}

export async function playerCheck(payload: PlayerCheckPayload = {}): Promise<PlayerActionResult> {
  return fetchApi<PlayerActionResult>('/api/player/check', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function playerRefresh(target?: PlayerTarget): Promise<PlayerActionResult> {
  return fetchApi<PlayerActionResult>('/api/player/refresh', {
    method: 'POST',
    body: JSON.stringify(target ? { target } : {}),
  });
}

export interface StartScanOptions {
  scan_type?: 'full' | 'changed_only' | 'incomplete_only';
  run_improve_after?: boolean;
}

export interface StartScanResponse {
  status: 'started' | 'ok' | string;
  scan_type?: string;
  default_scan_type?: string;
  reason?: string;
  message?: string;
  active_scan_type?: string;
  started_at?: number | null;
  run_improve_after?: boolean;
  ai_enabled?: boolean;
  ai_warning?: string | null;
}

export interface FilesWatcherStatus {
  running: boolean;
  enabled: boolean;
  available: boolean;
  degraded_mode?: boolean;
  reason: string;
  dirty_count: number;
  dirty_count_by_root?: Record<string, number>;
  last_event_at?: number | null;
  last_event_path?: string | null;
  restart_in_progress: boolean;
  consecutive_failures: number;
  last_restart_duration_ms?: number | null;
  last_error?: string;
  last_restart_started_at?: number | null;
  last_restart_ended_at?: number | null;
  roots?: string[];
}

export interface FilesWatcherRestartResponse {
  status: 'accepted' | string;
  requested_at: number;
}

export async function startScan(options?: StartScanOptions): Promise<StartScanResponse> {
  return fetchApi('/scan/start', {
    method: 'POST',
    body: options ? JSON.stringify(options) : undefined,
    headers: options ? { 'Content-Type': 'application/json' } : undefined,
  });
}

export async function getFilesWatcherStatus(): Promise<FilesWatcherStatus> {
  return fetchApi<FilesWatcherStatus>('/api/files/watcher/status');
}

export async function restartFilesWatcher(): Promise<FilesWatcherRestartResponse> {
  return fetchApi<FilesWatcherRestartResponse>('/api/files/watcher/restart', { method: 'POST' });
}

export interface ScanDefaultsResponse {
  default_scan_type: 'full' | 'changed_only' | 'incomplete_only' | string;
  bootstrap_required: boolean;
  autonomous_mode: boolean;
  has_completed_full_scan: boolean;
  first_full_scan_id?: number | null;
  first_full_completed_at?: number | null;
}

export interface BootstrapStatus {
  bootstrap_required: boolean;
  autonomous_mode: boolean;
  first_full_scan_id?: number | null;
  first_full_completed_at?: number | null;
  default_scan_type?: 'full' | 'changed_only' | 'incomplete_only' | string;
}

export interface FileSourceRoot {
  source_id?: number;
  path: string;
  role: 'library' | 'incoming';
  enabled: boolean;
  priority: number;
  is_winner_root?: boolean;
  created_at?: number;
  updated_at?: number;
}

export interface FileSourcesResponse {
  roots: FileSourceRoot[];
  winner_source_root_id?: number | null;
  winner_placement_strategy?: 'move' | 'hardlink' | 'symlink' | 'copy';
  status?: string;
}

export interface IncomingDeltaStatus {
  pending_folders: number;
  oldest_event_at?: number | null;
  last_processed_at?: number | null;
  errors?: string[];
  pending_by_source?: Record<string, number>;
  incoming_source_ids?: number[];
}

export async function getScanDefaults(): Promise<ScanDefaultsResponse> {
  return fetchApi<ScanDefaultsResponse>('/api/scan/defaults');
}

export async function getPipelineBootstrapStatus(): Promise<BootstrapStatus> {
  return fetchApi<BootstrapStatus>('/api/pipeline/bootstrap/status');
}

export async function getFileSources(): Promise<FileSourcesResponse> {
  return fetchApi<FileSourcesResponse>('/api/files/sources');
}

export async function updateFileSources(payload: {
  roots: FileSourceRoot[];
  winner_source_root_id?: number | null;
  winner_placement_strategy?: 'move' | 'hardlink' | 'symlink' | 'copy';
}): Promise<FileSourcesResponse> {
  return fetchApi<FileSourcesResponse>('/api/files/sources', {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function getIncomingStatus(): Promise<IncomingDeltaStatus> {
  return fetchApi<IncomingDeltaStatus>('/api/incoming/status');
}

export async function triggerIncomingRescan(): Promise<{ status: string; reason?: string; message?: string }> {
  return fetchApi<{ status: string; reason?: string; message?: string }>('/api/incoming/rescan', { method: 'POST' });
}

export interface IncompleteScanProgress {
  running: boolean;
  run_id: number | null;
  progress: number;
  total: number;
  current_artist: string;
  current_album: string;
  count: number;
  error: string | null;
}

export async function getIncompleteScanProgress(): Promise<IncompleteScanProgress> {
  return fetchApi<IncompleteScanProgress>('/api/incomplete-albums/scan/progress');
}

export interface IncompleteAlbumItem {
  artist: string;
  album_id: number;
  title_raw: string;
  folder: string;
  classification: string;
  missing_in_plex: number[];
  missing_on_disk: string[];
  expected_track_count: number;
  actual_track_count: number;
  detected_at: number;
}

export async function getIncompleteAlbumsResults(runId?: number): Promise<{ run_id: number | null; items: IncompleteAlbumItem[] }> {
  const url = runId != null ? `/api/incomplete-albums/results?run_id=${runId}` : '/api/incomplete-albums/results';
  return fetchApi(url);
}

export async function moveIncompleteAlbums(runId: number, items: Array<{ artist: string; album_id: number; title_raw?: string }>): Promise<{ moved: Array<{ artist: string; album_id: number; moved_to: string }> }> {
  return fetchApi('/api/incomplete-albums/move', {
    method: 'POST',
    body: JSON.stringify({ run_id: runId, items }),
    headers: { 'Content-Type': 'application/json' },
  });
}

/** Returns the URL to download export (JSON or CSV). Call with fetch or window.open. */
export function getIncompleteAlbumsExportUrl(runId: number, format: 'json' | 'csv' = 'json'): string {
  const base = API_BASE_URL || '';
  return `${base}/api/incomplete-albums/export/${runId}?format=${format}`;
}

export async function pauseScan(): Promise<void> {
  await fetchApi('/scan/pause', { method: 'POST' });
}

export async function resumeScan(): Promise<void> {
  await fetchApi('/scan/resume', { method: 'POST' });
}

export async function stopScan(): Promise<void> {
  await fetchApi('/scan/stop', { method: 'POST' });
}

export interface ClearScanOptions {
  clear_audio_cache?: boolean;
  clear_mb_cache?: boolean;
}

export interface ClearScanResult {
  status: string;
  message: string;
  cleared: {
    duplicates_best: number;
    duplicates_loser: number;
    audio_cache?: number;
    musicbrainz_cache?: number;
  };
}

export async function clearScan(options: ClearScanOptions = {}): Promise<ClearScanResult> {
  return fetchApi<ClearScanResult>('/api/scan/clear', {
    method: 'POST',
    body: JSON.stringify(options),
  });
}

export type MaintenanceResetAction = 'media_cache' | 'state_db' | 'cache_db' | 'settings_db' | 'files_index';

export interface MaintenanceResetRequest {
  actions: MaintenanceResetAction[];
  restart?: boolean;
  confirm_settings_db?: boolean;
}

export interface MaintenanceResetResponse {
  status: 'ok' | 'partial' | 'error' | 'blocked';
  message: string;
  actions: MaintenanceResetAction[];
  results: Record<string, unknown>;
  warnings?: string[];
  errors?: string[];
  restart_initiated?: boolean;
}

export async function runMaintenanceReset(payload: MaintenanceResetRequest): Promise<MaintenanceResetResponse> {
  return fetchApi<MaintenanceResetResponse>('/api/admin/maintenance/reset', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

// Dedupe
export async function getDedupeProgress(): Promise<DedupeProgress> {
  return fetchApi<DedupeProgress>('/api/dedupe');
}

export async function dedupeAll(): Promise<{ started: boolean; total?: number }> {
  return fetchApi<{ started: boolean; total?: number }>('/api/dedupe/all', { method: 'POST' });
}

export interface ImproveAllProgress {
  running: boolean;
  global?: boolean;
  current: number;
  total: number;
  current_album?: string;
  current_artist?: string;
  result?: { albums_processed?: number; albums_improved?: number; by_provider?: Record<string, { identified: number; covers: number; tags: number }> };
  error?: string;
  finished?: boolean;
}

export async function improveAll(): Promise<{ started: boolean; total: number }> {
  return fetchApi<{ started: boolean; total: number }>('/api/library/improve-all', { method: 'POST' });
}

export async function getImproveAllProgress(): Promise<ImproveAllProgress> {
  return fetchApi<ImproveAllProgress>('/api/library/improve-all/progress');
}

/** Result of drop/improve (improve album by path from uploaded files). */
export interface ImproveDropResult {
  steps: string[];
  summary: string;
  tags_updated: boolean;
  cover_saved: boolean;
  provider_used: string | null;
  dupes_in_folder: Array<{ track: number; paths: string[] }>;
  files_updated: number;
  error?: string;
}

/** Upload audio files and run improve-by-path (identify, tag, cover). Uses multipart form. */
export async function improveDroppedAlbum(files: File[], folderName?: string): Promise<ImproveDropResult> {
  const formData = new FormData();
  files.forEach((f) => formData.append('files', f));
  if (folderName != null && folderName !== '') formData.append('folder_name', folderName);
  const res = await fetchWithAuth('/api/drop/improve', { method: 'POST', body: formData });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.error || data.detail || `HTTP ${res.status}`);
  }
  return {
    steps: Array.isArray(data.steps) ? data.steps : [],
    summary: data.summary ?? '',
    tags_updated: Boolean(data.tags_updated),
    cover_saved: Boolean(data.cover_saved),
    provider_used: data.provider_used ?? null,
    dupes_in_folder: Array.isArray(data.dupes_in_folder) ? data.dupes_in_folder : [],
    files_updated: Number(data.files_updated) || 0,
  };
}

export interface LidarrAddIncompleteProgress {
  running: boolean;
  current: number;
  total: number;
  current_album?: string;
  current_artist?: string;
  added: number;
  failed: number;
  result?: { added?: number; failed?: number; total?: number; error?: string };
  finished?: boolean;
}

export async function addIncompleteAlbumsToLidarr(): Promise<{ started: boolean; total: number }> {
  return fetchApi<{ started: boolean; total: number }>('/api/lidarr/add-incomplete-albums', { method: 'POST' });
}

export async function getLidarrAddIncompleteProgress(): Promise<LidarrAddIncompleteProgress> {
  return fetchApi<LidarrAddIncompleteProgress>('/api/lidarr/add-incomplete-albums/progress');
}

export async function dedupeArtist(
  artist: string,
  albumId?: string,
  options?: { keep_edition_album_id?: number }
): Promise<DedupeResult> {
  const safeArtist = encodeURIComponent(artist.replace(/\s+/g, '_'));
  const body: { album_id?: string; keep_edition_album_id?: number } = {};
  if (albumId != null) body.album_id = albumId;
  if (options?.keep_edition_album_id != null) body.keep_edition_album_id = options.keep_edition_album_id;
  return fetchApi<DedupeResult>(`/dedupe/artist/${safeArtist}`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function dedupeSelected(selected: string[]): Promise<DedupeResult> {
  return fetchApi<DedupeResult>('/dedupe/selected', {
    method: 'POST',
    body: JSON.stringify({ selected }),
  });
  }

/** Merge bonus tracks into kept editions for all groups, then run full dedupe. */
export async function dedupeMergeAndDedupe(): Promise<void> {
  await fetchApi<undefined>('/dedupe/merge-and-dedupe', { method: 'POST' });
}

// Manual dedupe with custom selection
export async function dedupeManual(artist: string, albumId: string, keepEditionIndex: number): Promise<DedupeResult> {
  const safeArtist = encodeURIComponent(artist.replace(/\s+/g, '_'));
  return fetchApi<DedupeResult>(`/dedupe/manual/${safeArtist}`, {
    method: 'POST',
    body: JSON.stringify({ album_id: albumId, keep_index: keepEditionIndex }),
  });
}

// Move bonus track to kept edition
export async function moveBonusTrack(
  artist: string,
  albumId: string,
  sourceEditionIndex: number,
  trackPath: string,
  targetEditionIndex: number
): Promise<{ success: boolean; message: string }> {
  const safeArtist = encodeURIComponent(artist.replace(/\s+/g, '_'));
  try {
    return await fetchApi(`/dedupe/move-track/${safeArtist}`, {
      method: 'POST',
      body: JSON.stringify({
        album_id: albumId,
        source_index: sourceEditionIndex,
        track_path: trackPath,
        target_index: targetEditionIndex,
      }),
    });
  } catch {
    return { success: false, message: 'Backend does not support bonus track moving yet' };
  }
}

// Config
/** Config response includes backend-only field configured (wizard-first). */
export type ConfigResponse = Partial<PMDAConfig> & { configured?: boolean };

export interface AuthUser {
  id: number;
  username: string;
  is_admin: boolean;
  can_download: boolean;
  can_view_statistics: boolean;
  allow_ai_calls?: boolean;
  is_active: boolean;
  accept_shares?: boolean;
  share_liked_public?: boolean;
  share_recommendations_public?: boolean;
  avatar_data_url?: string | null;
  created_at: number;
  updated_at: number;
  last_login_at?: number | null;
}

export interface AuthBootstrapStatusResponse {
  bootstrap_required: boolean;
}

export interface AuthBootstrapRequest {
  username: string;
  password: string;
  password_confirm: string;
}

export interface AuthLoginRequest {
  username: string;
  password: string;
  remember_me?: boolean;
}

export interface AuthLoginResponse {
  ok: boolean;
  token: string;
  expires_at: number;
  remember_me?: boolean;
  user: AuthUser;
}

export interface AuthMeResponse {
  ok: boolean;
  user: AuthUser;
}

export interface AuthProfileUpdateRequest {
  accept_shares?: boolean;
  share_liked_public?: boolean;
  share_recommendations_public?: boolean;
  avatar_data_url?: string | null;
}

export interface AuthProfileUpdateResponse {
  ok: boolean;
  user: AuthUser;
}

export interface AuthPasswordChangeRequest {
  current_password: string;
  new_password: string;
  new_password_confirm: string;
}

export interface AuthPasswordChangeResponse {
  ok: boolean;
  reauth_required?: boolean;
}

export interface AdminUsersResponse {
  users: AuthUser[];
}

export interface AdminUserCreateRequest {
  username: string;
  password: string;
  password_confirm: string;
  is_admin?: boolean;
  can_download?: boolean;
  can_view_statistics?: boolean;
  allow_ai_calls?: boolean;
  is_active?: boolean;
}

export interface AdminUserUpdateRequest {
  username?: string;
  password?: string;
  is_admin?: boolean;
  can_download?: boolean;
  can_view_statistics?: boolean;
  allow_ai_calls?: boolean;
  is_active?: boolean;
}

export interface AdminUserMutationResponse {
  ok: boolean;
  user: AuthUser;
}

export interface AdminUserDeleteResponse {
  ok: boolean;
  deleted_user_id: number;
  username?: string;
}

export async function getAuthBootstrapStatus(): Promise<AuthBootstrapStatusResponse> {
  return fetchApi<AuthBootstrapStatusResponse>('/api/auth/bootstrap/status');
}

export async function bootstrapAdmin(payload: AuthBootstrapRequest): Promise<{ ok: boolean; message?: string; user?: AuthUser }> {
  return fetchApi<{ ok: boolean; message?: string; user?: AuthUser }>('/api/auth/bootstrap', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function login(payload: AuthLoginRequest): Promise<AuthLoginResponse> {
  return fetchApi<AuthLoginResponse>('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function me(): Promise<AuthMeResponse> {
  return fetchApi<AuthMeResponse>('/api/auth/me');
}

export async function updateAuthProfile(payload: AuthProfileUpdateRequest): Promise<AuthProfileUpdateResponse> {
  return fetchApi<AuthProfileUpdateResponse>('/api/auth/profile', {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function changeAuthPassword(payload: AuthPasswordChangeRequest): Promise<AuthPasswordChangeResponse> {
  return fetchApi<AuthPasswordChangeResponse>('/api/auth/password', {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function logout(): Promise<{ ok: boolean }> {
  return fetchApi<{ ok: boolean }>('/api/auth/logout', { method: 'POST' });
}

export async function getAdminUsers(): Promise<AdminUsersResponse> {
  return fetchApi<AdminUsersResponse>('/api/admin/users');
}

export async function createAdminUser(payload: AdminUserCreateRequest): Promise<AdminUserMutationResponse> {
  return fetchApi<AdminUserMutationResponse>('/api/admin/users', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function updateAdminUser(userId: number, payload: AdminUserUpdateRequest): Promise<AdminUserMutationResponse> {
  return fetchApi<AdminUserMutationResponse>(`/api/admin/users/${encodeURIComponent(String(userId))}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function deleteAdminUser(userId: number): Promise<AdminUserDeleteResponse> {
  return fetchApi<AdminUserDeleteResponse>(`/api/admin/users/${encodeURIComponent(String(userId))}`, {
    method: 'DELETE',
  });
}

export async function getConfig(): Promise<ConfigResponse> {
  try {
    return await fetchApi<ConfigResponse>('/api/config');
  } catch {
    return {};
  }
}

export async function saveConfig(config: Partial<PMDAConfig>): Promise<{ status: string; restart_initiated?: boolean; message?: string }> {
  return fetchApi<{ status: string; restart_initiated?: boolean; message?: string }>('/api/config', {
    method: 'PUT',
    body: JSON.stringify(config),
  });
}

export async function getTaskEvents(afterId = 0, limit = 100): Promise<TaskEventsResponse> {
  const safeAfter = Math.max(0, Number(afterId) || 0);
  const safeLimit = Math.max(1, Math.min(500, Number(limit) || 100));
  return fetchApi<TaskEventsResponse>(`/api/events/tasks?after_id=${safeAfter}&limit=${safeLimit}`);
}

export async function getSchedulerRules(): Promise<SchedulerRulesResponse> {
  return fetchApi<SchedulerRulesResponse>('/api/scheduler/rules');
}

export async function saveSchedulerRules(rules: SchedulerRule[]): Promise<SchedulerRulesResponse & { status: string }> {
  return fetchApi<SchedulerRulesResponse & { status: string }>('/api/scheduler/rules', {
    method: 'PUT',
    body: JSON.stringify({ rules }),
  });
}

export async function runSchedulerJob(payload: SchedulerRunJobPayload): Promise<SchedulerRunJobResponse> {
  return fetchApi<SchedulerRunJobResponse>('/api/scheduler/jobs/run', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function pauseSchedulerJobs(): Promise<{ status: string; paused: boolean }> {
  return fetchApi<{ status: string; paused: boolean }>('/api/scheduler/jobs/pause', { method: 'POST' });
}

export async function resumeSchedulerJobs(): Promise<{ status: string; paused: boolean }> {
  return fetchApi<{ status: string; paused: boolean }>('/api/scheduler/jobs/resume', { method: 'POST' });
}

export async function getSchedulerJobsStatus(): Promise<SchedulerJobsStatusResponse> {
  return fetchApi<SchedulerJobsStatusResponse>('/api/scheduler/jobs/status');
}

/** Files backend: start rebuild of export library (hardlinks/symlinks/copies/moves). */
export async function postFilesExportRebuild(): Promise<{ status: string; message?: string }> {
  return fetchApi<{ status: string; message?: string }>('/api/files/export/rebuild', { method: 'POST' });
}

/** Files backend: drop and rebuild the indexed library from FILES_ROOTS. */
export async function postLibraryFilesIndexRebuild(): Promise<{ status: string; message?: string; progress?: unknown }> {
  return fetchApi<{ status: string; message?: string; progress?: unknown }>('/api/library/files-index/rebuild', { method: 'POST' });
}

/** Files backend: export progress (running, tracks_done, total_tracks, albums_done, total_albums, error). */
export interface FilesExportStatus {
  running: boolean;
  tracks_done: number;
  total_tracks: number;
  albums_done: number;
  total_albums: number;
  error: string | null;
}
export async function getFilesExportStatus(): Promise<FilesExportStatus> {
  return fetchApi<FilesExportStatus>('/api/files/export/status');
}

/** Files backend: structure overview (templates, metrics, samples). */
export interface FilesStructureOverview {
  templates: Array<{ name: string; example: string }>;
  metrics: { sample_count?: number; total_files_estimate?: number; average_path_depth?: number; paths_with_artist_tag?: number; paths_with_album_tag?: number };
  samples: Array<{ path: string; artist?: string; album?: string; year?: string; ext?: string }>;
  sample_count: number;
}
export async function getFilesStructureOverview(): Promise<FilesStructureOverview> {
  return fetchApi<FilesStructureOverview>('/api/files/structure/overview');
}

export interface FilesystemDirectoryEntry {
  name: string;
  path: string;
  writable: boolean;
}

export interface FilesystemDirectoryList {
  path: string;
  parent: string | null;
  writable: boolean;
  directories: FilesystemDirectoryEntry[];
  truncated: boolean;
  roots: string[];
}

/** Settings folder picker: list directories under a given absolute path. */
export async function getFilesystemDirectories(
  path?: string,
  options?: { timeoutMs?: number; limit?: number },
): Promise<FilesystemDirectoryList> {
  const params = new URLSearchParams();
  if (path) params.set('path', path);
  if (typeof options?.limit === 'number') params.set('limit', String(options.limit));
  const query = params.toString() ? `?${params.toString()}` : '';
  return fetchApi<FilesystemDirectoryList>(`/api/fs/list${query}`, {
    timeoutMs: options?.timeoutMs ?? 45000,
  });
}

export async function testMusicBrainz(
  useMusicBrainz?: boolean,
): Promise<{ success: boolean; message: string; base_url?: string; mirror_enabled?: boolean; mirror_name?: string }> {
  try {
    const body = useMusicBrainz !== undefined ? { USE_MUSICBRAINZ: useMusicBrainz } : undefined;
    const response = await fetchWithAuth('/api/musicbrainz/test', {
      method: body ? 'POST' : 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const data = await response.json().catch(() => ({ success: false, message: 'Invalid response from server' }));
    if (!response.ok) {
      return { success: false, message: data.message || data.error || `MusicBrainz test failed: ${response.statusText}` };
    }
    return data;
  } catch (error: unknown) {
    return { success: false, message: error instanceof Error ? error.message : 'Failed to test MusicBrainz connection' };
  }
}


export async function checkOpenAI(apiKey?: string): Promise<{ success: boolean; message: string }> {
  try {
    const body = apiKey ? { OPENAI_API_KEY: apiKey } : undefined;
    return await fetchApi('/api/openai/check', { 
      method: 'POST',
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch (error) {
    return { success: false, message: 'Failed to verify OpenAI connection' };
  }
}

export interface OpenAIDeviceOAuthStartResponse {
  ok: boolean;
  session_id?: string;
  verification_url?: string;
  user_code?: string;
  interval?: number;
  message?: string;
  warning?: string;
}

export interface OpenAIDeviceOAuthPollResponse {
  status: 'pending' | 'completed' | 'error';
  message?: string;
  retry_after?: number;
  api_key_saved?: boolean;
}

export interface OpenAICodexOAuthStatusResponse {
  connected: boolean;
  profile_connected?: boolean;
  ready?: boolean;
  provider_id?: string;
  auth_mode?: string;
  account_id?: string;
  expires_at?: number | null;
  expires_in_sec?: number | null;
  has_refresh_token?: boolean;
  metadata?: Record<string, unknown>;
  error?: string;
}

export interface LastfmAuthStatusResponse {
  configured: boolean;
  connected: boolean;
  pending: boolean;
  session_name?: string;
  auth_url?: string;
  error?: string;
  reconnect_required?: boolean;
  corrupt_session?: boolean;
  message?: string;
}

export interface AIProviderPreferencesResponse {
  interactive_provider_id: string;
  batch_provider_id: string;
  web_search_provider_id: string;
  codex_connected?: boolean;
  effective?: {
    interactive_provider_id?: string;
    batch_provider_id?: string;
    web_search_provider_id?: string;
  };
}

export async function startOpenAIDeviceOAuth(): Promise<OpenAIDeviceOAuthStartResponse> {
  return fetchApi<OpenAIDeviceOAuthStartResponse>('/api/ai/providers/openai-codex/oauth/device/start', {
    method: 'POST',
  });
}

export async function pollOpenAIDeviceOAuth(sessionId: string): Promise<OpenAIDeviceOAuthPollResponse> {
  return fetchApi<OpenAIDeviceOAuthPollResponse>('/api/ai/providers/openai-codex/oauth/device/poll', {
    method: 'POST',
    body: JSON.stringify({ session_id: sessionId }),
  });
}

export async function getOpenAICodexOAuthStatus(options?: { checkRuntime?: boolean }): Promise<OpenAICodexOAuthStatusResponse> {
  const runtimeFlag = Boolean(options?.checkRuntime);
  const query = runtimeFlag ? '?check_runtime=true' : '';
  return fetchApi<OpenAICodexOAuthStatusResponse>(`/api/ai/providers/openai-codex/oauth/status${query}`);
}

export async function disconnectOpenAICodexOAuth(): Promise<{ ok: boolean; message?: string }> {
  return fetchApi<{ ok: boolean; message?: string }>('/api/ai/providers/openai-codex/oauth/disconnect', {
    method: 'POST',
  });
}

export async function getLastfmAuthStatus(): Promise<LastfmAuthStatusResponse> {
  return fetchApi<LastfmAuthStatusResponse>('/api/lastfm/auth/status');
}

export async function startLastfmAuth(): Promise<{ ok: boolean; auth_url: string; pending: boolean; token?: string }> {
  return fetchApi<{ ok: boolean; auth_url: string; pending: boolean; token?: string }>('/api/lastfm/auth/start', {
    method: 'POST',
  });
}

export async function completeLastfmAuth(): Promise<{ ok: boolean; connected: boolean; session_name?: string; message?: string }> {
  return fetchApi<{ ok: boolean; connected: boolean; session_name?: string; message?: string }>('/api/lastfm/auth/complete', {
    method: 'POST',
  });
}

export async function disconnectLastfmAuth(): Promise<{ ok: boolean; message?: string }> {
  return fetchApi<{ ok: boolean; message?: string }>('/api/lastfm/auth/disconnect', {
    method: 'POST',
  });
}

export async function getAIProviderPreferences(): Promise<AIProviderPreferencesResponse> {
  return fetchApi<AIProviderPreferencesResponse>('/api/ai/providers/preferences');
}

export async function saveAIProviderPreferences(
  payload: Pick<AIProviderPreferencesResponse, 'interactive_provider_id' | 'batch_provider_id' | 'web_search_provider_id'>
): Promise<AIProviderPreferencesResponse & { status?: string }> {
  return fetchApi<AIProviderPreferencesResponse & { status?: string }>('/api/ai/providers/preferences', {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function getOpenAIModels(apiKey: string): Promise<string[]> {
  if (!apiKey?.trim()) {
    throw new Error('API key is required to fetch models');
  }
  const body = { OPENAI_API_KEY: apiKey.trim() };
  return await fetchApi<string[]>('/api/openai/models', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function getAIModels(
  provider: 'openai' | 'openai-api' | 'openai-codex' | 'anthropic' | 'google' | 'ollama',
  credentials: { apiKey?: string; url?: string }
): Promise<string[]> {
  try {
    const body: Record<string, string> & { AI_PROVIDER: typeof provider } = { AI_PROVIDER: provider };
    if (provider === 'ollama') {
      if (!credentials.url?.trim()) {
        throw new Error('Ollama URL is required');
      }
      body.OLLAMA_URL = credentials.url.trim();
    } else {
      if (!credentials.apiKey?.trim()) {
        throw new Error('API key is required');
      }
      if (provider === 'openai' || provider === 'openai-api' || provider === 'openai-codex') {
        body.OPENAI_API_KEY = credentials.apiKey.trim();
      } else if (provider === 'anthropic') {
        body.ANTHROPIC_API_KEY = credentials.apiKey.trim();
      } else if (provider === 'google') {
        body.GOOGLE_API_KEY = credentials.apiKey.trim();
      }
    }
    return await fetchApi<string[]>('/api/ai/models', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  } catch (error: unknown) {
    // Re-throw with better error message
    if (isApiError(error)) {
      const body = error.body;
      if (body && typeof body === 'object') {
        const msg = (body as { error?: unknown }).error;
        if (typeof msg === 'string' && msg) {
          throw new Error(msg);
        }
      }
      throw new Error(error.message || 'Failed to fetch models');
    }
    throw error instanceof Error ? error : new Error('Failed to fetch models');
  }
}

export interface OllamaPullStatus {
  active: boolean;
  status: string;
  message: string;
  model: string;
  url: string;
  completed: number;
  total: number;
  progress: number;
  started_at?: number | null;
  updated_at?: number | null;
  finished_at?: number | null;
  error?: string;
  digest?: string;
}

export interface OllamaDiscoveryRow {
  url: string;
  ok: boolean;
  message: string;
  models: string[];
  model_count: number;
}

export interface OllamaDiscoveryResponse {
  results: OllamaDiscoveryRow[];
}

export async function getOllamaPullStatus(): Promise<OllamaPullStatus> {
  return fetchApi<OllamaPullStatus>('/api/ollama/pull/status');
}

export async function startOllamaPull(payload: { OLLAMA_URL: string; model: string }): Promise<OllamaPullStatus> {
  return fetchApi<OllamaPullStatus>('/api/ollama/pull', {
    method: 'POST',
    body: JSON.stringify(payload),
    timeoutMs: 30000,
  });
}

export async function discoverOllama(url?: string): Promise<OllamaDiscoveryResponse> {
  const qs = url?.trim() ? `?url=${encodeURIComponent(url.trim())}` : '';
  return fetchApi<OllamaDiscoveryResponse>(`/api/ollama/discover${qs}`, {
    timeoutMs: 45000,
  });
}


// Stats calculation helper
export function calculateStats(duplicates: DuplicateCard[], dedupeProgress: DedupeProgress) {
  const artists = new Set(duplicates.map(d => d.artist)).size;
  const albums = duplicates.length;
  const remainingDupes = duplicates.reduce((sum, d) => sum + (d.no_move ? 0 : d.n - 1), 0);
  
  const baseSaved = dedupeProgress.saved || 0;
  const savedThisRun = dedupeProgress.deduping ? (dedupeProgress.saved_this_run ?? 0) : 0;
  return {
    artists,
    albums,
    remainingDupes,
    removedDupes: dedupeProgress.moved ?? dedupeProgress.progress ?? 0,
    spaceSaved: baseSaved + savedThisRun,
  };
}

// Scan History API
export async function getScanHistory(): Promise<ScanHistoryEntry[]> {
  return fetchApi<ScanHistoryEntry[]>(`/api/scan-history`);
}

export async function clearScanHistory(): Promise<{ status: string; message: string }> {
  return fetchApi<{ status: string; message: string }>(`/api/scan-history`, { method: 'DELETE' });
}

export async function getScanDetails(scanId: number): Promise<ScanHistoryEntry> {
  return fetchApi<ScanHistoryEntry>(`/api/scan-history/${scanId}`);
}

export async function getScanAICostSummary(
  scanId: number,
  options?: { includeLifecycle?: boolean; groupBy?: AICostGroupBy; limit?: number },
): Promise<ScanAICostSummary> {
  const params = new URLSearchParams();
  params.set('include_lifecycle', options?.includeLifecycle === false ? 'false' : 'true');
  params.set('group_by', options?.groupBy || 'analysis_type');
  if (typeof options?.limit === 'number' && Number.isFinite(options.limit) && options.limit > 0) {
    params.set('limit', String(Math.trunc(options.limit)));
  }
  return fetchApi<ScanAICostSummary>(`/api/scans/${scanId}/ai-costs?${params.toString()}`);
}

export async function getScanMoves(
  scanId: number,
  options?: { reason?: 'dedupe' | 'incomplete'; status?: 'all' | 'active' | 'restored' }
): Promise<ScanMove[]> {
  const params = new URLSearchParams();
  if (options?.reason) params.set('reason', options.reason);
  if (options?.status) params.set('status', options.status);
  const qs = params.toString();
  return fetchApi<ScanMove[]>(`/api/scan-history/${scanId}/moves${qs ? `?${qs}` : ''}`);
}

export async function getScanMovesSummary(scanId: number): Promise<ScanMovesSummary> {
  return fetchApi<ScanMovesSummary>(`/api/scan-history/${scanId}/moves/summary`);
}

export async function getScanPipelineTrace(
  scanId: number,
  options?: {
    page?: number;
    pageSize?: number;
    q?: string;
    provider?: 'all' | 'musicbrainz' | 'discogs' | 'lastfm' | 'bandcamp' | 'none';
    outcome?: 'all' | ScanPipelineOutcome;
  },
): Promise<ScanPipelineTraceResponse> {
  const params = new URLSearchParams();
  if (typeof options?.page === 'number' && Number.isFinite(options.page) && options.page > 0) {
    params.set('page', String(Math.trunc(options.page)));
  }
  if (typeof options?.pageSize === 'number' && Number.isFinite(options.pageSize) && options.pageSize > 0) {
    params.set('page_size', String(Math.trunc(options.pageSize)));
  }
  if (options?.q) params.set('q', options.q);
  if (options?.provider && options.provider !== 'all') params.set('provider', options.provider);
  if (options?.outcome && options.outcome !== 'all') params.set('outcome', options.outcome);
  const qs = params.toString();
  return fetchApi<ScanPipelineTraceResponse>(`/api/scan-history/${scanId}/pipeline-trace${qs ? `?${qs}` : ''}`, {
    timeoutMs: 30000,
  });
}

export async function downloadScanPipelineTrace(
  scanId: number,
  format: 'csv' | 'json',
  options?: {
    q?: string;
    provider?: 'all' | 'musicbrainz' | 'discogs' | 'lastfm' | 'bandcamp' | 'none';
    outcome?: 'all' | ScanPipelineOutcome;
  },
): Promise<void> {
  const params = new URLSearchParams();
  params.set('format', format);
  if (options?.q) params.set('q', options.q);
  if (options?.provider && options.provider !== 'all') params.set('provider', options.provider);
  if (options?.outcome && options.outcome !== 'all') params.set('outcome', options.outcome);
  const endpoint = `/api/scan-history/${scanId}/pipeline-trace/export?${params.toString()}`;
  const response = await fetchWithAuth(endpoint, { method: 'GET' });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    const msg = (data as { error?: unknown }).error;
    throw new Error(typeof msg === 'string' && msg.trim() ? msg : `Export failed (${response.status})`);
  }
  const blob = await response.blob();
  const filename = parseContentDispositionFilename(response.headers.get('content-disposition')) || `scan-${scanId}-pipeline-trace.${format}`;
  const objectUrl = window.URL.createObjectURL(blob);
  try {
    const anchor = document.createElement('a');
    anchor.href = objectUrl;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
  } finally {
    window.URL.revokeObjectURL(objectUrl);
  }
}

export interface ScanMoveDetailTrack {
  track_id?: number;
  title: string;
  disc_num: number;
  disc_label?: string | null;
  track_num: number;
  duration_sec?: number;
  format?: string;
  bitrate?: number;
  sample_rate?: number;
  bit_depth?: number;
  file_size_bytes?: number;
  file_path?: string;
}

export interface ScanMoveDetailEdition {
  album_id?: number | null;
  album_title?: string;
  path: string;
  thumb_url?: string | null;
  tracks: ScanMoveDetailTrack[];
  track_count: number;
  fmt_text?: string;
  analysis?: Record<string, unknown>;
}

export interface ScanMoveDetailResponse {
  move_id: number;
  scan_id: number;
  artist: string;
  album_id: number;
  album_title: string;
  move_reason: 'dedupe' | 'incomplete' | string;
  reason_label: string;
  status: 'moved' | 'restored' | 'missing' | string;
  moved_at: number;
  decision_source?: string;
  decision_provider?: string;
  decision_reason?: string;
  decision_confidence?: number | null;
  details?: Record<string, unknown>;
  moved: ScanMoveDetailEdition;
  winner?: ScanMoveDetailEdition;
  incomplete?: {
    expected_track_count: number;
    actual_track_count: number;
    missing_indices: number[];
    missing_required_tags: string[];
    strict_match_provider?: string;
    strict_reject_reason?: string;
    strict_tracklist_score?: number;
    expected_tracks: Array<{ index: number; title: string }>;
  };
}

export async function getScanMoveDetail(moveId: number): Promise<ScanMoveDetailResponse> {
  return fetchApi<ScanMoveDetailResponse>(`/api/scan-move/${encodeURIComponent(String(moveId))}/detail`, {
    timeoutMs: 30000,
  });
}

export interface RestoreMovesResult {
  restored: number;
  artists_refreshed: number;
  /** Paths restored: from (dupe folder) -> to (original location) */
  restored_paths?: { from: string; to: string }[];
}

export async function restoreMoves(scanId: number, moveIds?: number[], all?: boolean): Promise<RestoreMovesResult> {
  return fetchApi<RestoreMovesResult>(`/api/scan-history/${scanId}/restore`, {
    method: 'POST',
    body: JSON.stringify({ move_ids: moveIds, all }),
  });
}

export async function dedupeScan(scanId: number): Promise<{ status: string; message: string }> {
  return fetchApi<{ status: string; message: string }>(`/api/scan-history/${scanId}/dedupe`, {
    method: 'POST',
  });
}

// Broken Albums API
export interface BrokenAlbum {
  artist: string;
  album_id: number;
  album_title: string;
  expected_track_count?: number;
  actual_track_count: number;
  missing_indices: Array<number | [number, number]>;
  musicbrainz_release_group_id?: string;
  detected_at: number;
  sent_to_lidarr: boolean;
  folder_path?: string | null;
  thumb_url?: string | null;
  metadata_source?: string | null;
  strict_match_provider?: string | null;
  strict_reject_reason?: string | null;
  reason_summary?: string | null;
  classification?: string | null;
  classification_confidence?: number;
  classification_source?: string | null;
  quarantine_eligible?: boolean;
  evidence?: Record<string, unknown>;
  ai_verdict?: BrokenAlbumAiVerdict;
  pipeline_status?: string | null;
  recoverable?: boolean;
  review_status?: 'pending' | 'ignored' | null;
}

export interface BrokenAlbumDetailTrack {
  title: string;
  track_num?: number;
  disc_num?: number;
  disc_label?: string;
  duration_sec?: number;
  file_path?: string;
}

export interface BrokenAlbumExpectedTrack {
  index: number;
  title: string;
}

export interface BrokenAlbumAiVerdict {
  status?: 'completed' | 'failed' | 'skipped';
  provider?: string;
  model?: string;
  shadow_mode?: boolean;
  prompt_version?: string;
  verdict?: string;
  confidence?: number;
  reasoning_flags?: string[];
  recommended_action?: 'keep_review' | 'do_not_quarantine' | 'quarantine_candidate' | string;
  needs_manual_review?: boolean;
  evidence_summary?: string;
  deterministic_verdict?: string;
  deterministic_confidence?: number;
  ai_overrides_deterministic?: boolean;
  reason?: string;
  error?: string;
  created_at?: number;
}

export interface BrokenAlbumDetail {
  artist: string;
  album_id: number;
  album_title: string;
  detected_at: number;
  folder_path?: string | null;
  thumb_url?: string | null;
  metadata_source?: string | null;
  strict_match_provider?: string | null;
  strict_reject_reason?: string | null;
  expected_track_count?: number;
  actual_track_count: number;
  missing_indices: Array<number | [number, number]>;
  missing_required_tags?: string[];
  musicbrainz_release_group_id?: string | null;
  provider_refs?: Record<string, string>;
  pipeline_status?: string | null;
  timeline?: unknown[];
  meta_summary?: Record<string, unknown>;
  reason_summary?: string | null;
  classification?: string | null;
  classification_confidence?: number;
  classification_source?: string | null;
  quarantine_eligible?: boolean;
  evidence?: Record<string, unknown>;
  ai_verdict?: BrokenAlbumAiVerdict;
  local_tracks: BrokenAlbumDetailTrack[];
  expected_tracks: BrokenAlbumExpectedTrack[];
  recoverable: boolean;
  sent_to_lidarr: boolean;
  review_status?: 'pending' | 'ignored' | null;
}

export interface LidarrConfig {
  LIDARR_URL?: string;
  LIDARR_API_KEY?: string;
}

export async function testLidarr(url: string, apiKey: string): Promise<{ success: boolean; message: string }> {
  const response = await fetchWithAuth('/api/lidarr/test', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, api_key: apiKey }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: 'Failed to test Lidarr connection' }));
    throw new Error(error.message || 'Failed to test Lidarr connection');
  }
  return response.json();
}

export async function testAutobrr(url: string, apiKey: string): Promise<{ success: boolean; message: string }> {
  const response = await fetchWithAuth('/api/autobrr/test', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, api_key: apiKey }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: 'Failed to test Autobrr connection' }));
    throw new Error(error.message || 'Failed to test Autobrr connection');
  }
  return response.json();
}

export async function getBrokenAlbums(): Promise<BrokenAlbum[]> {
  const response = await fetchWithAuth('/api/broken-albums');
  if (!response.ok) throw new Error('Failed to fetch broken albums');
  return response.json();
}

export async function getBrokenAlbumDetail(
  artist: string,
  albumId: number,
  options?: { refreshAi?: boolean },
): Promise<BrokenAlbumDetail> {
  const q = new URLSearchParams({
    artist,
    album_id: String(Math.max(1, Math.floor(albumId))),
  });
  if (options?.refreshAi) q.set('refresh_ai', '1');
  return fetchApi<BrokenAlbumDetail>(`/api/broken-albums/detail?${q.toString()}`, { timeoutMs: 25000 });
}

export async function setBrokenAlbumReviewStatus(
  artist: string,
  albumId: number,
  reviewStatus: 'pending' | 'ignored',
): Promise<{ success: boolean; artist: string; album_id: number; review_status: string }> {
  const response = await fetchWithAuth('/api/broken-albums/review', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      artist,
      album_id: albumId,
      review_status: reviewStatus,
    }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to update incomplete album review status' }));
    throw new Error(error.error || error.message || 'Failed to update incomplete album review status');
  }
  return response.json();
}

export async function addAlbumToLidarr(album: BrokenAlbum): Promise<{ success: boolean; message: string }> {
  const response = await fetchWithAuth('/api/lidarr/add-album', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      artist_name: album.artist,
      album_id: album.album_id,
      musicbrainz_release_group_id: album.musicbrainz_release_group_id,
      album_title: album.album_title,
    }),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.message || 'Failed to add album to Lidarr');
  }
  return response.json();
}

// ─────────────────────────────── Assistant (Chat) ───────────────────────────────
export interface AssistantStatus {
  library_mode: string;
  ai_provider: string;
  ai_model: string;
  ai_auth_mode?: string;
  ai_ready: boolean;
  ai_error?: string | null;
  postgres_ready: boolean;
  db_tools_ready?: boolean;
  pg_host?: string;
  pg_port?: number;
  pg_db?: string;
  pg_user?: string;
  config_dir?: string;
  config_sources?: Record<string, string>;
}

export interface AssistantCitation {
  entity_type: string;
  entity_id: number;
  doc_type: string;
  source: string;
  title: string;
  chunk_id: number;
  score: number;
  snippet: string;
}

export interface AssistantMessage {
  id: number;
  role: 'user' | 'assistant' | 'system';
  content: string;
  created_at: number;
  context?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

export interface AssistantSessionResponse {
  session_id: string;
  messages: AssistantMessage[];
}

export interface AssistantChatResponse {
  session_id: string;
  user_message: AssistantMessage;
  assistant_message: AssistantMessage;
  citations: AssistantCitation[];
}

export interface EntityDiscoverLink {
  kind: 'internal' | 'external';
  label: string;
  href: string;
  entity_type?: string;
  entity_id?: number;
  thumb?: string | null;
  subtitle?: string | null;
  provider?: string | null;
}

export interface EntityDiscoverSection {
  key: string;
  title: string;
  reason?: string | null;
  links: EntityDiscoverLink[];
}

export interface EntityDiscoverResponse {
  entity_type: 'artist' | 'album' | 'label';
  entity_label: string;
  generated_at: number;
  summary: string;
  sections: EntityDiscoverSection[];
  provider?: string | null;
  model?: string | null;
  ai_used: boolean;
  fallback_used: boolean;
}

export async function getAssistantStatus(): Promise<AssistantStatus> {
  return fetchApi<AssistantStatus>('/api/assistant/status');
}

export async function getAssistantSession(sessionId: string, limit: number = 120): Promise<AssistantSessionResponse> {
  const sid = encodeURIComponent(String(sessionId || ''));
  return fetchApi<AssistantSessionResponse>(`/api/assistant/session/${sid}?limit=${encodeURIComponent(String(limit))}`);
}

export async function postAssistantChat(input: {
  message: string;
  session_id?: string;
  context?: { artist_id?: number; context_inferred?: boolean; [k: string]: unknown };
}): Promise<AssistantChatResponse> {
  return fetchApi<AssistantChatResponse>('/api/assistant/chat', {
    method: 'POST',
    body: JSON.stringify(input),
  });
}

export async function postEntityDiscover(input: {
  entity_type: 'artist' | 'album' | 'label';
  entity_id?: number;
  artist_id?: number;
  album_id?: number;
  label?: string;
}): Promise<EntityDiscoverResponse> {
  return fetchApi<EntityDiscoverResponse>('/api/library/entity-discover', {
    method: 'POST',
    body: JSON.stringify(input),
  });
}
