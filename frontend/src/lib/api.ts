// PMDA API Client
// Empty = same origin (e.g. when served from the same Docker container). Set VITE_PMDA_API_URL for dev (must be full URL, e.g. http://192.168.3.2:5005).
const _raw = import.meta.env.VITE_PMDA_API_URL ?? '';
const API_BASE_URL = typeof _raw === 'string' && (_raw.startsWith('http://') || _raw.startsWith('https://')) ? _raw : '';

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

export interface ScanProgress {
  scanning: boolean;
  progress: number;
  total: number;
  /** Progress including in-progress artist albums (so bar moves during scan) */
  effective_progress?: number;
  status: 'running' | 'paused' | 'stopped' | 'idle';
  scan_type?: 'full' | 'changed_only' | 'incomplete_only';
  scan_resume_run_id?: string | null;
  /** Current scan phase: format_analysis | identification_tags | ia_analysis | finalizing | moving_dupes | post_processing */
  phase?: 'format_analysis' | 'identification_tags' | 'ia_analysis' | 'finalizing' | 'moving_dupes' | 'post_processing' | null;
  /** Micro-step for live indicators: analyzing_format | fetching_mb_id | searching_mb | comparing_versions | detecting_best | done */
  current_step?: string | null;
  /** Total albums in this scan (for N/M display in findings) */
  total_albums?: number;
  /** AI provider name (e.g. OpenAI) when AI is used */
  ai_provider?: string;
  /** AI model name (e.g. gpt-4o-mini) when AI is used */
  ai_model?: string;
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
  scan_discogs_matched?: number;
  scan_lastfm_matched?: number;
  scan_bandcamp_matched?: number;
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
  /** Sync target resolved for this scan. */
  scan_pipeline_sync_target?: string | null;
  /** Number of incomplete albums auto-moved in this scan. */
  scan_incomplete_moved_count?: number;
  /** Total size moved for incompletes (MB) in this scan. */
  scan_incomplete_moved_mb?: number;
  /** Player sync telemetry. */
  scan_player_sync_target?: string | null;
  scan_player_sync_ok?: boolean | null;
  scan_player_sync_message?: string | null;
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
    roots: string[];
    dirty_count: number;
    last_event_at?: number | null;
    last_event_path?: string | null;
  };
}

export interface LogTailResponse {
  path: string;
  lines: string[];
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
  /** Root folder for the clean export library. */
  EXPORT_ROOT?: string;
  /** Naming template used when exporting via hardlinks/symlinks/copies. */
  EXPORT_NAMING_TEMPLATE?: string;
  /** Link strategy for export: hardlink | symlink | copy | move. */
  EXPORT_LINK_STRATEGY?: 'hardlink' | 'symlink' | 'copy' | 'move';
  /** NVMe-friendly media cache root for pre-rendered artwork thumbnails (album/artist). */
  MEDIA_CACHE_ROOT?: string;
  /** When true (Files mode), rebuild the export library automatically after a Magic scan. */
  AUTO_EXPORT_LIBRARY?: boolean;
  
  // AI Provider
  AI_PROVIDER: 'openai' | 'anthropic' | 'google' | 'ollama';
  OPENAI_API_KEY: string;
  OPENAI_MODEL: string;
  OPENAI_MODEL_FALLBACKS: string;
  ANTHROPIC_API_KEY: string;
  GOOGLE_API_KEY: string;
  OLLAMA_URL: string;
  /** Custom AI prompt for duplicate selection (advanced). Empty = use default. */
  AI_PROMPT?: string;
  
  // MusicBrainz & Notifications
  USE_MUSICBRAINZ: boolean;
  MUSICBRAINZ_EMAIL: string;
  /** How to apply MusicBrainz artist credits to tags and grouping. */
  ARTIST_CREDIT_MODE?: 'album_artist_strict' | 'musicbrainz_full_credit' | 'picard_like_default';
  /** Re-query MusicBrainz for albums previously cached as "not found" on each scan */
  MB_RETRY_NOT_FOUND?: boolean;
  /** Advanced: ignore MusicBrainz cache and stored MBIDs, forcing a full lookup every scan (slower; for testing). */
  MB_DISABLE_CACHE?: boolean;
  /** Use AI to choose among multiple MusicBrainz candidates (title-only prompt). */
  USE_AI_FOR_MB_MATCH?: boolean;
  /** Use AI to verify MusicBrainz match (artist, title, track count/titles). Can recover e.g. "Volume I" vs "volume i". */
  USE_AI_FOR_MB_VERIFY?: boolean;
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
  LOG_LEVEL: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR';
  LOG_FILE: string;
  AUTO_MOVE_DUPES: boolean;
  PIPELINE_ENABLE_MATCH_FIX?: boolean;
  PIPELINE_ENABLE_DEDUPE?: boolean;
  PIPELINE_ENABLE_INCOMPLETE_MOVE?: boolean;
  PIPELINE_ENABLE_EXPORT?: boolean;
  PIPELINE_ENABLE_PLAYER_SYNC?: boolean;
  PIPELINE_PLAYER_TARGET?: 'none' | 'plex' | 'jellyfin' | 'navidrome';
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
}

type ApiError = Error & { response?: Response; body?: unknown };

const isApiError = (error: unknown): error is ApiError => {
  return error instanceof Error && typeof (error as ApiError).response !== 'undefined';
};

const GET_REQUEST_TIMEOUT_MS = 12000;
const inFlightGetRequests = new Map<string, Promise<unknown>>();

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

async function executeFetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const method = String(options?.method ?? 'GET').toUpperCase();
  const timeoutMs = method === 'GET' ? GET_REQUEST_TIMEOUT_MS : 0;
  const { signal, cleanup } = createFetchSignal(options?.signal, timeoutMs);
  let timedOut = false;
  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      ...options,
      signal: signal ?? options?.signal,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
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
      return response.json();
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
async function fetchApi<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const method = String(options?.method ?? 'GET').toUpperCase();
  if (method !== 'GET') {
    return executeFetchApi<T>(endpoint, options);
  }
  const key = `${method}:${API_BASE_URL}${endpoint}`;
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
}

export async function getLibraryStats(options?: { includeUnmatched?: boolean }): Promise<LibraryStats> {
  const q = new URLSearchParams();
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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

export type LibraryDiscoverSectionKey = 'genre' | 'artists' | 'similar' | 'labels' | 'year' | 'random';

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
  options?: { includeUnmatched?: boolean }
): Promise<LibraryDiscoverResponse> {
  const qs = new URLSearchParams();
  qs.set('days', String(Math.max(7, Math.min(365, days))));
  qs.set('limit', String(Math.max(6, Math.min(36, limit))));
  if (refresh) qs.set('refresh', '1');
  if (options?.includeUnmatched != null) qs.set('include_unmatched', options.includeUnmatched ? '1' : '0');
  return fetchApi<LibraryDiscoverResponse>(`/api/library/discover?${qs.toString()}`);
}

export type LibrarySearchItemType = 'artist' | 'album' | 'track' | 'genre';

export interface LibrarySearchSuggestionItem {
  type: LibrarySearchItemType;
  title: string;
  subtitle?: string;
  thumb?: string | null;
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
  options?: { includeUnmatched?: boolean }
): Promise<LibrarySearchSuggestResponse> {
  if (!query.trim()) return { query: '', items: [] };
  const qs = new URLSearchParams();
  qs.set('q', query.trim());
  qs.set('limit', String(Math.max(1, Math.min(40, limit))));
  if (options?.includeUnmatched != null) qs.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
  mb_identified?: boolean;
  thumb?: string | null;
  artist_id: number;
  artist_name: string;
  short_description?: string | null;
  profile_source?: string | null;
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
  options?: { includeUnmatched?: boolean }
): Promise<LibraryDigestResponse> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(36, limit))));
  if (!trigger) q.set('trigger', '0');
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
  return fetchApi<LibraryDigestResponse>(`/api/library/digest?${q.toString()}`);
}

export async function getLibraryAlbums(options?: {
  search?: string;
  sort?: 'recent' | 'year_desc' | 'alpha' | 'artist';
  genre?: string;
  label?: string;
  year?: number;
  limit?: number;
  offset?: number;
  includeUnmatched?: boolean;
}): Promise<LibraryAlbumsResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.sort) q.set('sort', options.sort);
  if (options?.genre) q.set('genre', options.genre);
  if (options?.label) q.set('label', options.label);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(240, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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

export async function getLibraryFacets(options?: { includeUnmatched?: boolean }): Promise<LibraryFacetsResponse> {
  const q = new URLSearchParams();
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
  filters?: { label?: string; year?: number | null; includeUnmatched?: boolean }
): Promise<LibraryGenresSuggestResponse> {
  const q = new URLSearchParams();
  if (query.trim()) q.set('q', query.trim());
  q.set('limit', String(Math.max(1, Math.min(80, limit))));
  if (refresh) q.set('refresh', '1');
  if (filters?.label) q.set('label', String(filters.label));
  if (filters?.year != null) q.set('year', String(filters.year));
  if (filters?.includeUnmatched != null) q.set('include_unmatched', filters.includeUnmatched ? '1' : '0');
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
  filters?: { genre?: string; year?: number | null; includeUnmatched?: boolean }
): Promise<LibraryLabelsSuggestResponse> {
  const q = new URLSearchParams();
  if (query.trim()) q.set('q', query.trim());
  q.set('limit', String(Math.max(1, Math.min(80, limit))));
  if (refresh) q.set('refresh', '1');
  if (filters?.genre) q.set('genre', String(filters.genre));
  if (filters?.year != null) q.set('year', String(filters.year));
  if (filters?.includeUnmatched != null) q.set('include_unmatched', filters.includeUnmatched ? '1' : '0');
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
  options?: { includeUnmatched?: boolean }
): Promise<LibraryGenreLabelsResponse> {
  const g = genre.trim();
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (refresh) q.set('refresh', '1');
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
  options?: { includeUnmatched?: boolean; offset?: number }
): Promise<LibraryRecentlyPlayedAlbumsResponse> {
  const q = new URLSearchParams();
  q.set('days', String(Math.max(7, Math.min(365, days))));
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (refresh) q.set('refresh', '1');
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
  return fetchApi<LibraryRecentlyPlayedAlbumsResponse>(`/api/library/recently-played/albums?${q.toString()}`);
}

export interface LibraryArtistItem {
  artist_id: number;
  artist_name: string;
  album_count: number;
  broken_albums_count?: number;
  artist_thumb?: string | null;
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
  genre?: string;
  label?: string;
  year?: number;
  limit?: number;
  offset?: number;
  includeUnmatched?: boolean;
}): Promise<LibraryArtistsResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.genre) q.set('genre', options.genre);
  if (options?.label) q.set('label', options.label);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(500, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
}): Promise<LibraryGenresResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.label) q.set('label', options.label);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(200, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
}): Promise<LibraryLabelsResponse> {
  const q = new URLSearchParams();
  if (options?.search) q.set('search', options.search);
  if (options?.genre) q.set('genre', options.genre);
  if (options?.year != null) q.set('year', String(options.year));
  if (options?.limit != null) q.set('limit', String(Math.max(1, Math.min(200, options.limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
  options?: { includeUnmatched?: boolean; offset?: number }
): Promise<TopArtistsResponse> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(200, limit))));
  if (options?.offset != null) q.set('offset', String(Math.max(0, options.offset)));
  if (days && days > 0) q.set('days', String(Math.max(1, Math.min(3650, days))));
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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

export async function getRecentlyAddedArtists(limit = 18, offset = 0, options?: { includeUnmatched?: boolean }): Promise<RecentlyAddedArtistsResponse> {
  const q = new URLSearchParams();
  q.set('limit', String(Math.max(1, Math.min(60, limit))));
  q.set('offset', String(Math.max(0, offset)));
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
  return fetchApi<RecentlyAddedArtistsResponse>(`/api/library/artists/recent?${q.toString()}`);
}

export type LikeEntityType = 'artist' | 'album' | 'track';

export interface LikeItem {
  entity_id: number;
  liked: boolean;
  updated_at: number;
}

export interface LikesResponse {
  entity_type: LikeEntityType;
  items: LikeItem[];
  error?: string;
}

export async function getLikes(entityType: LikeEntityType, ids?: number[]): Promise<LikesResponse> {
  const q = new URLSearchParams();
  q.set('entity_type', entityType);
  if (ids && ids.length > 0) q.set('ids', ids.join(','));
  return fetchApi<LikesResponse>(`/api/library/likes?${q.toString()}`);
}

export async function setLike(payload: { entity_type: LikeEntityType; entity_id: number; liked: boolean; source?: string }): Promise<{ entity_type: LikeEntityType; entity_id: number; liked: boolean; updated_at: number }> {
  return fetchApi(`/api/library/likes`, {
    method: 'PUT',
    body: JSON.stringify(payload),
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
  options?: { limit_artists?: number; refresh?: boolean; includeUnmatched?: boolean }
): Promise<GenreProfileResponse> {
  const q = new URLSearchParams();
  if (options?.limit_artists != null) q.set('limit_artists', String(Math.max(1, Math.min(120, options.limit_artists))));
  if (options?.refresh) q.set('refresh', '1');
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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
  error?: string;
}

export async function getLabelProfile(
  label: string,
  options?: { limit_artists?: number; limit_genres?: number; refresh?: boolean; includeUnmatched?: boolean }
): Promise<LabelProfileResponse> {
  const q = new URLSearchParams();
  if (options?.limit_artists != null) q.set('limit_artists', String(Math.max(1, Math.min(120, options.limit_artists))));
  if (options?.limit_genres != null) q.set('limit_genres', String(Math.max(1, Math.min(120, options.limit_genres))));
  if (options?.refresh) q.set('refresh', '1');
  if (options?.includeUnmatched != null) q.set('include_unmatched', options.includeUnmatched ? '1' : '0');
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

export interface AlbumDetailResponse {
  album_id: number;
  title: string;
  year?: number | null;
  date_text?: string;
  genre?: string;
  label?: string;
  format?: string;
  is_lossless?: boolean;
  track_count: number;
  total_duration_sec: number;
  has_cover?: boolean;
  cover_url?: string | null;
  bandcamp_album_url?: string | null;
  metadata_source?: string | null;
  artist_id: number;
  artist_name: string;
  review?: AlbumDetailReview;
  tracks: AlbumDetailTrack[];
}

export async function getAlbumDetail(albumId: number): Promise<AlbumDetailResponse> {
  return fetchApi<AlbumDetailResponse>(`/api/library/album/${encodeURIComponent(String(albumId))}`);
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

export async function getScanLogsTail(lines: number = 180): Promise<LogTailResponse> {
  const n = Number.isFinite(lines) ? Math.max(20, Math.min(1200, Math.trunc(lines))) : 180;
  return fetchApi<LogTailResponse>(`/api/logs/tail?lines=${n}`);
}

export interface ScanPreflightResult {
  musicbrainz: { ok: boolean; message: string };
  ai: { ok: boolean; message: string; provider: string };
  discogs?: { ok: boolean; message: string };
  lastfm?: { ok: boolean; message: string };
  bandcamp?: { ok: boolean; message: string };
  serper?: { ok: boolean; message: string };
  acoustid?: { ok: boolean; message: string };
  paths?: { music_rw: boolean; dupes_rw: boolean };
}

export async function getScanPreflight(): Promise<ScanPreflightResult> {
  return fetchApi<ScanPreflightResult>('/api/scan/preflight');
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

export async function startScan(options?: StartScanOptions): Promise<{ status: string; scan_type?: string; run_improve_after?: boolean }> {
  return fetchApi('/scan/start', {
    method: 'POST',
    body: options ? JSON.stringify(options) : undefined,
    headers: options ? { 'Content-Type': 'application/json' } : undefined,
  });
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
  const base = API_BASE_URL ? API_BASE_URL.replace(/\/$/, '') : '';
  const url = `${base}/api/drop/improve`;
  const res = await fetch(url, { method: 'POST', body: formData });
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
export async function getFilesystemDirectories(path?: string): Promise<FilesystemDirectoryList> {
  const query = path ? `?path=${encodeURIComponent(path)}` : '';
  return fetchApi<FilesystemDirectoryList>(`/api/fs/list${query}`);
}

/** Test Plex connection. Pass current form values to test before saving. */
export async function checkPlexConnection(host?: string, token?: string): Promise<{ success: boolean; message: string; libraries?: Array<{ id: string; name: string }> }> {
  try {
    const body = host != null || token != null ? { PLEX_HOST: host ?? '', PLEX_TOKEN: token ?? '' } : undefined;
    return await fetchApi<{ success: boolean; message: string; libraries?: Array<{ id: string; name: string }> }>(
      '/api/plex/check',
      body ? { method: 'POST', body: JSON.stringify(body) } : undefined
    );
  } catch (error) {
    const msg = error instanceof Error ? error.message : 'Failed to connect to Plex server';
    return { success: false, message: msg };
  }
}

/** Create a Plex.tv PIN for sign-in (user goes to plex.tv/link and enters code). */
export async function createPlexPin(): Promise<{ success: boolean; id?: number; code?: string; link_url?: string; message?: string }> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/plex/pin`, { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) return { success: false, message: (data as { message?: string }).message || 'Failed to create PIN' };
    return { success: true, id: (data as { id?: number }).id, code: (data as { code?: string }).code, link_url: (data as { link_url?: string }).link_url };
  } catch (e) {
    return { success: false, message: e instanceof Error ? e.message : 'Failed to create PIN' };
  }
}

/** Poll PIN status; when user has linked on plex.tv/link, returns { status: 'linked', token }. */
export async function pollPlexPin(pinId: number): Promise<{ success: boolean; status: string; token?: string; message?: string }> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/plex/pin?id=${encodeURIComponent(pinId)}`);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) return { success: false, status: 'error', message: (data as { message?: string }).message };
    return { success: true, status: (data as { status?: string }).status ?? 'waiting', token: (data as { token?: string }).token, message: (data as { message?: string }).message };
  } catch (e) {
    return { success: false, status: 'error', message: e instanceof Error ? e.message : 'Poll failed' };
  }
}

/** Plex server entry returned by GET plex.tv/servers (Tautulli-style). */
export interface PlexServerEntry {
  name: string;
  uri: string;
  address?: string;
  port?: string;
  scheme?: string;
  localAddresses?: string;
  machineIdentifier?: string;
}

/** List Plex servers for the given token (Plex.tv API). */
export async function getPlexServers(token: string): Promise<{ success: boolean; servers: PlexServerEntry[]; message?: string }> {
  try {
    const res = await fetch(`${API_BASE_URL}/api/plex/servers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ PLEX_TOKEN: token }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { success: false, servers: [], message: (data as { message?: string }).message || res.statusText || 'Failed to fetch Plex servers' };
    }
    const payload = data as { success?: boolean; servers?: PlexServerEntry[]; message?: string };
    return payload.success !== false
      ? { success: true, servers: payload.servers ?? [], message: payload.message }
      : { success: false, servers: [], message: payload.message };
  } catch (error) {
    return { success: false, servers: [], message: error instanceof Error ? error.message : 'Failed to fetch Plex servers' };
  }
}

/** Get the client IP (browser machine) so we can ask the backend to scan that subnet for Plex. */
export async function getPlexClientIp(): Promise<{ client_ip: string }> {
  const res = await fetchApi<{ client_ip: string }>('/api/plex/client-ip');
  return res ?? { client_ip: '' };
}

/** Discover Plex servers: GDM, host fallback, and optionally subnet scan from client IP.
 *  - clientIp: scan the /24 of the machine viewing the WebUI (no URL needed).
 *  - plexHostHint: also probe the port from this URL if non-standard.
 */
export async function getPlexDiscover(options?: { clientIp?: string; plexHostHint?: string }): Promise<{ success: boolean; servers: PlexServerEntry[]; message?: string }> {
  try {
    const body: { client_ip?: string; PLEX_HOST?: string } = {};
    if (options?.clientIp) body.client_ip = options.clientIp;
    if (options?.plexHostHint) body.PLEX_HOST = options.plexHostHint;
    const res = await fetchApi<{ success: boolean; servers: PlexServerEntry[]; message?: string }>(
      '/api/plex/discover',
      Object.keys(body).length ? { method: 'POST', body: JSON.stringify(body) } : undefined
    );
    return res ?? { success: false, servers: [] };
  } catch (error) {
    return { success: false, servers: [], message: 'Discovery failed' };
  }
}

export interface PlexDatabasePathHint {
  platform: string;
  path: string;
  note: string;
}

/** Common Plex database directory locations by platform (for wizard help). */
export async function getPlexDatabasePaths(): Promise<{ success: boolean; paths: PlexDatabasePathHint[] }> {
  try {
    const res = await fetchApi<{ success: boolean; paths: PlexDatabasePathHint[] }>('/api/plex/database-paths');
    return res ?? { success: false, paths: [] };
  } catch {
    return { success: false, paths: [] };
  }
}

/** Verify that PMDA has read access to the Plex database (current config). */
export async function verifyPlexDb(): Promise<{ success: boolean; message?: string }> {
  try {
    const res = await fetchApi<{ success: boolean; message?: string }>('/api/plex/verify-db');
    return res ?? { success: false, message: 'Request failed' };
  } catch (e) {
    return { success: false, message: e instanceof Error ? e.message : 'Request failed' };
  }
}

export async function testMusicBrainz(useMusicBrainz?: boolean): Promise<{ success: boolean; message: string }> {
  try {
    const body = useMusicBrainz !== undefined ? { USE_MUSICBRAINZ: useMusicBrainz } : undefined;
    const response = await fetch(`${API_BASE_URL}/api/musicbrainz/test`, {
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

export async function startOpenAIDeviceOAuth(): Promise<OpenAIDeviceOAuthStartResponse> {
  return fetchApi<OpenAIDeviceOAuthStartResponse>('/api/openai/oauth/device/start', {
    method: 'POST',
  });
}

export async function pollOpenAIDeviceOAuth(sessionId: string): Promise<OpenAIDeviceOAuthPollResponse> {
  return fetchApi<OpenAIDeviceOAuthPollResponse>('/api/openai/oauth/device/poll', {
    method: 'POST',
    body: JSON.stringify({ session_id: sessionId }),
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
  provider: 'openai' | 'anthropic' | 'google' | 'ollama',
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
      if (provider === 'openai') {
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

// Autodetection (pass config so backend uses wizard values)
export async function autodetectPaths(body?: {
  PLEX_HOST?: string;
  PLEX_TOKEN?: string;
  SECTION_IDS?: string;
}): Promise<{ success: boolean; paths: Record<string, string>; message?: string }> {
  try {
    return await fetchApi('/api/autodetect/paths', {
      method: 'POST',
      body: JSON.stringify(body ?? {}),
    });
  } catch {
    return { success: false, paths: {}, message: 'Failed to autodetect paths' };
  }
}

export async function discoverPaths(body: {
  PATH_MAP: Record<string, string>;
  PLEX_DB_PATH?: string;
  MUSIC_PARENT_PATH?: string;
  CROSSCHECK_SAMPLES?: number;
}): Promise<{ success: boolean; paths: Record<string, string>; results: PathVerifyResult[]; message?: string }> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/paths/discover`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = (await response.json().catch(() => ({}))) as {
      success?: boolean;
      paths?: Record<string, string>;
      results?: PathVerifyResult[];
      message?: string;
    };
    if (!response.ok) {
      return {
        success: false,
        paths: data.paths ?? {},
        results: data.results ?? [],
        message: data.message ?? `Discover failed (${response.status})`,
      };
    }
    return {
      success: true,
      paths: data.paths ?? {},
      results: data.results ?? [],
      message: data.message,
    };
  } catch (e) {
    return {
      success: false,
      paths: {},
      results: [],
      message: e instanceof Error ? e.message : 'Discover failed',
    };
  }
}

export async function discoverPathOne(body: {
  plex_root: string;
  PLEX_DB_PATH?: string;
  MUSIC_PARENT_PATH?: string;
  CROSSCHECK_SAMPLES?: number;
}): Promise<{ success: boolean; host_root: string | null; result: PathVerifyResult | null; message?: string }> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/paths/discover-one`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = (await response.json().catch(() => ({}))) as {
      success?: boolean;
      host_root?: string | null;
      result?: PathVerifyResult | null;
      message?: string;
    };
    if (!response.ok) {
      return {
        success: false,
        host_root: null,
        result: null,
        message: data.message ?? `Discover failed (${response.status})`,
      };
    }
    return {
      success: data.success ?? true,
      host_root: data.host_root ?? null,
      result: data.result ?? null,
      message: data.message,
    };
  } catch (e) {
    return {
      success: false,
      host_root: null,
      result: null,
      message: e instanceof Error ? e.message : 'Discover failed',
    };
  }
}

export interface PathVerifyResult {
  plex_root: string;
  host_root: string;
  status: 'ok' | 'fail';
  samples_checked: number;
  message: string;
}

export async function getPathVerifyLast(): Promise<{ results: PathVerifyResult[] | null; at: number | null }> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/paths/verify/last`);
    const data = (await response.json().catch(() => ({}))) as { results?: PathVerifyResult[] | null; at?: number | null };
    return { results: data.results ?? null, at: data.at ?? null };
  } catch {
    return { results: null, at: null };
  }
}

export async function verifyPaths(body: {
  PATH_MAP?: Record<string, string>;
  PLEX_DB_PATH?: string;
  CROSSCHECK_SAMPLES?: number;
}): Promise<{ success: boolean; results: PathVerifyResult[]; message?: string; hint?: string }> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/paths/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = (await response.json().catch(() => ({}))) as { success?: boolean; results?: PathVerifyResult[]; message?: string; hint?: string };
    if (!response.ok) {
      return {
        success: false,
        results: data.results ?? [],
        message: data.message ?? `Verification failed (${response.status})`,
        hint: data.hint,
      };
    }
    return data as { success: boolean; results: PathVerifyResult[]; message?: string; hint?: string };
  } catch (e) {
    return { success: false, results: [], message: e instanceof Error ? e.message : 'Verification failed' };
  }
}

export async function autodetectLibraries(plexHost?: string, plexToken?: string): Promise<{ success: boolean; libraries: Array<{ id: string; name: string; type?: string }>; message?: string }> {
  try {
    const body: Record<string, string> = {};
    if (plexHost?.trim()) body.PLEX_HOST = plexHost.trim();
    if (plexToken?.trim()) body.PLEX_TOKEN = plexToken.trim();
    return await fetchApi('/api/autodetect/libraries', { method: 'POST', body: JSON.stringify(body) });
  } catch {
    return { success: false, libraries: [], message: 'Failed to autodetect libraries' };
  }
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

export async function getScanMoves(scanId: number): Promise<ScanMove[]> {
  return fetchApi<ScanMove[]>(`/api/scan-history/${scanId}/moves`);
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
  missing_indices: Array<[number, number]>;
  musicbrainz_release_group_id?: string;
  detected_at: number;
  sent_to_lidarr: boolean;
}

export interface LidarrConfig {
  LIDARR_URL?: string;
  LIDARR_API_KEY?: string;
}

export async function testLidarr(url: string, apiKey: string): Promise<{ success: boolean; message: string }> {
  const response = await fetch(`${API_BASE_URL}/api/lidarr/test`, {
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
  const response = await fetch(`${API_BASE_URL}/api/autobrr/test`, {
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
  const response = await fetch(`${API_BASE_URL}/api/broken-albums`);
  if (!response.ok) throw new Error('Failed to fetch broken albums');
  return response.json();
}

export async function addAlbumToLidarr(album: BrokenAlbum): Promise<{ success: boolean; message: string }> {
  const response = await fetch(`${API_BASE_URL}/api/lidarr/add-album`, {
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
  ai_ready: boolean;
  ai_error?: string | null;
  postgres_ready: boolean;
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
