"""SQLite schema bootstrap and compatibility migrations for PMDA.

This module owns state/settings database DDL so the application bootstrap can
wire database paths and retry helpers without carrying thousands of schema
statements inline.
"""

from __future__ import annotations

import json
import sqlite3
import time


def init_state_db(
    *,
    state_db_file,
    state_db_busy_timeout_seconds: float,
    enable_wal,
    ai_pricing_default_rows,
    ai_pricing_version: str,
) -> None:
    con = sqlite3.connect(
        str(state_db_file),
        timeout=state_db_busy_timeout_seconds,
        check_same_thread=False,
    )
    # Enable WAL mode up front to allow concurrent reads/writes. Retry because
    # diagnostic imports can happen while a long scan is already writing.
    enable_wal(con, label="init_state_db")
    con.commit()
    cur = con.cursor()
    # Table for duplicate "best" entries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS duplicates_best (
            artist      TEXT,
            album_id    INTEGER,
            title_raw   TEXT,
            album_norm  TEXT,
            folder      TEXT,
            fmt_text    TEXT,
            br          INTEGER,
            sr          INTEGER,
            bd          INTEGER,
            dur         INTEGER,
            discs       INTEGER,
            rationale   TEXT,
            merge_list  TEXT,
            ai_used     INTEGER DEFAULT 0,
            PRIMARY KEY (artist, album_id)
        )
    """)
    # Extend schema: add meta_json if missing
    cur.execute("PRAGMA table_info(duplicates_best)")
    cols = [r[1] for r in cur.fetchall()]
    if "meta_json" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN meta_json TEXT")
    if "ai_provider" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN ai_provider TEXT")
    if "ai_model" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN ai_model TEXT")
    if "evidence_json" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN evidence_json TEXT")
    if "size_mb" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN size_mb INTEGER")
    if "track_count" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN track_count INTEGER")
    if "match_verified_by_ai" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN match_verified_by_ai INTEGER DEFAULT 0")
    if "dupe_signal" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN dupe_signal TEXT")
    if "no_move" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN no_move INTEGER DEFAULT 0")
    if "manual_review" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN manual_review INTEGER DEFAULT 0")
    if "same_folder" not in cols:
        cur.execute("ALTER TABLE duplicates_best ADD COLUMN same_folder INTEGER DEFAULT 0")
    # Add indexes for faster lookups
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_duplicates_best_artist ON duplicates_best(artist)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_duplicates_best_album_id ON duplicates_best(album_id)")
    except sqlite3.OperationalError:
        pass

    # Table for broken albums (missing tracks)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS broken_albums (
            artist TEXT,
            album_id INTEGER,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            missing_indices TEXT,
            musicbrainz_release_group_id TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fixed_at TIMESTAMP,
            sent_to_lidarr BOOLEAN DEFAULT 0,
            review_status TEXT DEFAULT '',
            album_title TEXT DEFAULT '',
            folder_path TEXT DEFAULT '',
            metadata_source TEXT DEFAULT '',
            strict_match_provider TEXT DEFAULT '',
            strict_reject_reason TEXT DEFAULT '',
            provider_refs_json TEXT DEFAULT '{}',
            reason_summary TEXT DEFAULT '',
            local_tracks_json TEXT DEFAULT '[]',
            expected_tracks_json TEXT DEFAULT '[]',
            missing_required_tags_json TEXT DEFAULT '[]',
            classification TEXT DEFAULT '',
            classification_confidence REAL DEFAULT 0.0,
            classification_source TEXT DEFAULT '',
            quarantine_eligible INTEGER DEFAULT 0,
            evidence_json TEXT DEFAULT '{}',
            ai_verdict_json TEXT DEFAULT '{}',
            PRIMARY KEY (artist, album_id)
        )
    """)
    try:
        broken_cols = [row[1] for row in cur.execute("PRAGMA table_info(broken_albums)").fetchall()]
    except Exception:
        broken_cols = []
    if "review_status" not in broken_cols:
        cur.execute("ALTER TABLE broken_albums ADD COLUMN review_status TEXT DEFAULT ''")
    broken_optional_columns = [
        ("album_title", "TEXT DEFAULT ''"),
        ("folder_path", "TEXT DEFAULT ''"),
        ("metadata_source", "TEXT DEFAULT ''"),
        ("strict_match_provider", "TEXT DEFAULT ''"),
        ("strict_reject_reason", "TEXT DEFAULT ''"),
        ("provider_refs_json", "TEXT DEFAULT '{}'"),
        ("reason_summary", "TEXT DEFAULT ''"),
        ("local_tracks_json", "TEXT DEFAULT '[]'"),
        ("expected_tracks_json", "TEXT DEFAULT '[]'"),
        ("missing_required_tags_json", "TEXT DEFAULT '[]'"),
        ("classification", "TEXT DEFAULT ''"),
        ("classification_confidence", "REAL DEFAULT 0.0"),
        ("classification_source", "TEXT DEFAULT ''"),
        ("quarantine_eligible", "INTEGER DEFAULT 0"),
        ("evidence_json", "TEXT DEFAULT '{}'"),
        ("ai_verdict_json", "TEXT DEFAULT '{}'"),
    ]
    for col_name, col_def in broken_optional_columns:
        if col_name not in broken_cols:
            try:
                cur.execute(f"ALTER TABLE broken_albums ADD COLUMN {col_name} {col_def}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise
    # Compatibility table for removed external acquisition monitoring.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitored_artists (
            artist_id INTEGER PRIMARY KEY,
            artist_name TEXT,
            musicbrainz_artist_id TEXT,
            lidarr_artist_id INTEGER,
            monitored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(artist_id)
        )
    """)
    # Table for duplicate "loser" entries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS duplicates_loser (
            artist      TEXT,
            album_id    INTEGER,
            folder      TEXT,
            fmt_text    TEXT,
            br          INTEGER,
            sr          INTEGER,
            bd          INTEGER,
            size_mb     INTEGER,
            FOREIGN KEY (artist, album_id) REFERENCES duplicates_best(artist, album_id)
        )
    """)
    # Add indexes for faster lookups
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_duplicates_loser_artist_album ON duplicates_loser(artist, album_id)")
    except sqlite3.OperationalError:
        pass
    # Migration: loser_album_id = Plex metadata_item id of this edition (loser). Required so /details
    # and load_scan_from_db have the correct album_id per edition (tracks, title_raw, path display).
    cur.execute("PRAGMA table_info(duplicates_loser)")
    loser_cols = {r[1] for r in cur.fetchall()}
    if "loser_album_id" not in loser_cols:
        cur.execute("ALTER TABLE duplicates_loser ADD COLUMN loser_album_id INTEGER")

    # AI cache for dupe selection decisions across scans.
    # This is separate from duplicates_best (scan results) because duplicates_best is cleared on each scan.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dupe_ai_cache (
            artist      TEXT NOT NULL,
            group_key   TEXT NOT NULL,
            best_folder TEXT,
            rationale   TEXT,
            merge_list  TEXT,
            ai_provider TEXT,
            ai_model    TEXT,
            confidence  INTEGER,
            created_at  REAL,
            updated_at  REAL,
            PRIMARY KEY (artist, group_key)
        )
        """
    )
    cur.execute("PRAGMA table_info(dupe_ai_cache)")
    dupe_ai_cols = {r[1] for r in cur.fetchall()}
    for col, col_type in (
        ("best_folder", "TEXT"),
        ("rationale", "TEXT"),
        ("merge_list", "TEXT"),
        ("ai_provider", "TEXT"),
        ("ai_model", "TEXT"),
        ("confidence", "INTEGER"),
        ("created_at", "REAL"),
        ("updated_at", "REAL"),
    ):
        if col not in dupe_ai_cols:
            try:
                cur.execute(f"ALTER TABLE dupe_ai_cache ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dupe_ai_cache_artist ON dupe_ai_cache(artist)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dupe_ai_cache_updated ON dupe_ai_cache(updated_at DESC)")
    except sqlite3.OperationalError:
        pass

    # User feedback loop for duplicates detection (pairs labeled as "dupe" or "not_dupe").
    # Stored by folder paths (stable across Files scans where album_id is run-local).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dupe_feedback_pairs (
            artist     TEXT NOT NULL,
            folder_a   TEXT NOT NULL,
            folder_b   TEXT NOT NULL,
            label      TEXT NOT NULL,
            updated_at REAL,
            note       TEXT,
            PRIMARY KEY (artist, folder_a, folder_b)
        )
        """
    )
    cur.execute("PRAGMA table_info(dupe_feedback_pairs)")
    dupe_fb_cols = {r[1] for r in cur.fetchall()}
    for col, col_type in (
        ("updated_at", "REAL"),
        ("note", "TEXT"),
    ):
        if col not in dupe_fb_cols:
            try:
                cur.execute(f"ALTER TABLE dupe_feedback_pairs ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dupe_feedback_pairs_artist ON dupe_feedback_pairs(artist)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dupe_feedback_pairs_updated ON dupe_feedback_pairs(updated_at DESC)")
    except sqlite3.OperationalError:
        pass
    # Table for stats like space_saved and removed_dupes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            key   TEXT PRIMARY KEY,
            value INTEGER
        )
    """)
    # Initialize stats if missing
    for stat_key in ("space_saved", "removed_dupes"):
        cur.execute("INSERT OR IGNORE INTO stats(key, value) VALUES(?, 0)", (stat_key,))
    # Small settings table in state.db for runtime values (e.g. last_completed_scan_id)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Table for scan history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_history (
            scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time REAL NOT NULL,
            end_time REAL,
            duration_seconds INTEGER,
            scan_type TEXT DEFAULT 'full',
            albums_scanned INTEGER DEFAULT 0,
            duplicates_found INTEGER DEFAULT 0,
            artists_processed INTEGER DEFAULT 0,
            artists_total INTEGER DEFAULT 0,
            ai_used_count INTEGER DEFAULT 0,
            mb_used_count INTEGER DEFAULT 0,
            ai_enabled INTEGER DEFAULT 0,
            mb_enabled INTEGER DEFAULT 0,
            auto_move_enabled INTEGER DEFAULT 0,
            space_saved_mb INTEGER DEFAULT 0,
            albums_moved INTEGER DEFAULT 0,
            status TEXT DEFAULT 'completed',
            duplicate_groups_count INTEGER DEFAULT 0,
            total_duplicates_count INTEGER DEFAULT 0,
            broken_albums_count INTEGER DEFAULT 0,
            missing_albums_count INTEGER DEFAULT 0,
            albums_without_artist_image INTEGER DEFAULT 0,
            albums_without_album_image INTEGER DEFAULT 0,
            albums_without_complete_tags INTEGER DEFAULT 0,
            albums_without_mb_id INTEGER DEFAULT 0,
            albums_without_artist_mb_id INTEGER DEFAULT 0,
            ai_tokens_total INTEGER DEFAULT 0,
            ai_cost_usd_total REAL DEFAULT 0.0,
            ai_unpriced_calls INTEGER DEFAULT 0,
            ai_lifecycle_complete INTEGER DEFAULT 0
        )
    """)
    # Add index for faster scan history queries
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_history_start_time ON scan_history(start_time DESC)")
    except sqlite3.OperationalError:
        pass
    # Add new columns if they don't exist (migration for existing databases)
    cur.execute("PRAGMA table_info(scan_history)")
    cols = [r[1] for r in cur.fetchall()]
    new_cols = [
        ("scan_type", "TEXT DEFAULT 'full'"),
        ("duplicate_groups_count", "INTEGER DEFAULT 0"),
        ("total_duplicates_count", "INTEGER DEFAULT 0"),
        ("broken_albums_count", "INTEGER DEFAULT 0"),
        ("missing_albums_count", "INTEGER DEFAULT 0"),
        ("albums_without_artist_image", "INTEGER DEFAULT 0"),
        ("albums_without_album_image", "INTEGER DEFAULT 0"),
        ("albums_without_complete_tags", "INTEGER DEFAULT 0"),
        ("albums_without_mb_id", "INTEGER DEFAULT 0"),
        ("albums_without_artist_mb_id", "INTEGER DEFAULT 0"),
        ("ai_tokens_total", "INTEGER DEFAULT 0"),
        ("ai_cost_usd_total", "REAL DEFAULT 0.0"),
        ("ai_unpriced_calls", "INTEGER DEFAULT 0"),
        ("ai_lifecycle_complete", "INTEGER DEFAULT 0"),
        ("summary_json", "TEXT"),
        ("entry_type", "TEXT DEFAULT 'scan'"),
    ]
    for col_name, col_type in new_cols:
        if col_name not in cols:
            cur.execute(f"ALTER TABLE scan_history ADD COLUMN {col_name} {col_type}")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_history_status_type_end ON scan_history(status, scan_type, end_time DESC)")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mcp_review_proposals (
            proposal_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            scan_id INTEGER,
            target_key TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            recommendation TEXT NOT NULL DEFAULT '',
            confidence REAL,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            proposed_actions_json TEXT NOT NULL DEFAULT '[]',
            created_by_token_id TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            reviewed_at REAL,
            reviewed_by TEXT NOT NULL DEFAULT '',
            review_note TEXT NOT NULL DEFAULT ''
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mcp_review_proposals_status ON mcp_review_proposals(status, created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mcp_review_proposals_scan ON mcp_review_proposals(scan_id, kind)")
    except sqlite3.OperationalError:
        pass
    # Table for scan moves (tracking file movements)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_moves (
            move_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            original_path TEXT NOT NULL,
            moved_to_path TEXT NOT NULL,
            size_mb INTEGER,
            moved_at REAL NOT NULL,
            restored INTEGER DEFAULT 0,
            FOREIGN KEY (scan_id) REFERENCES scan_history(scan_id)
        )
    """)
    # Migrate scan_moves: add album_title and fmt_text if missing
    cur.execute("PRAGMA table_info(scan_moves)")
    move_cols = [r[1] for r in cur.fetchall()]
    for col_name, col_type in [
        ("album_title", "TEXT"),
        ("fmt_text", "TEXT"),
        ("move_reason", "TEXT DEFAULT 'dedupe'"),
        ("winner_album_id", "INTEGER"),
        ("winner_title", "TEXT"),
        ("winner_path", "TEXT"),
        ("decision_source", "TEXT"),
        ("decision_provider", "TEXT"),
        ("decision_reason", "TEXT"),
        ("decision_confidence", "REAL"),
        ("source_path", "TEXT"),
        ("destination_path", "TEXT"),
        ("materialization_strategy", "TEXT"),
        ("arbitration_result", "TEXT"),
        ("details_json", "TEXT"),
    ]:
        if col_name not in move_cols:
            cur.execute(f"ALTER TABLE scan_moves ADD COLUMN {col_name} {col_type}")
    try:
        cur.execute(
            """
            UPDATE scan_moves
            SET source_path = original_path
            WHERE COALESCE(source_path, '') = ''
              AND COALESCE(original_path, '') <> ''
            """
        )
        cur.execute(
            """
            UPDATE scan_moves
            SET destination_path = moved_to_path
            WHERE COALESCE(destination_path, '') = ''
              AND COALESCE(moved_to_path, '') <> ''
            """
        )
        cur.execute(
            """
            SELECT move_id,
                   COALESCE(move_reason, ''),
                   COALESCE(decision_reason, ''),
                   COALESCE(details_json, '')
            FROM scan_moves
            WHERE COALESCE(materialization_strategy, '') = ''
               OR COALESCE(arbitration_result, '') = ''
            """
        )
        move_backfill_rows = cur.fetchall()
        for move_id, move_reason_raw, decision_reason_raw, details_json_raw in move_backfill_rows:
            move_reason_norm = str(move_reason_raw or "").strip().lower()
            decision_reason_norm = str(decision_reason_raw or "").strip().lower()
            details_obj = {}
            try:
                details_obj = json.loads(details_json_raw or "{}") if details_json_raw else {}
            except Exception:
                details_obj = {}
            if not isinstance(details_obj, dict):
                details_obj = {}
            strategy = str(details_obj.get("export_strategy") or "").strip().lower()
            if not strategy:
                for candidate in ("hardlink", "copy", "move", "symlink"):
                    if decision_reason_norm.startswith(f"{candidate}_"):
                        strategy = candidate
                        break
            arbitration = ""
            if move_reason_norm == "matched_export":
                arbitration = "promoted"
            elif move_reason_norm == "matched_export_conflict":
                arbitration = "kept_existing"
            elif move_reason_norm == "incomplete_quarantine":
                arbitration = "incomplete_quarantine"
            elif "materialized_existing" in decision_reason_norm:
                arbitration = "materialized_existing"
            if strategy or arbitration:
                cur.execute(
                    """
                    UPDATE scan_moves
                    SET materialization_strategy = CASE
                            WHEN COALESCE(materialization_strategy, '') = '' THEN COALESCE(?, materialization_strategy)
                            ELSE materialization_strategy
                        END,
                        arbitration_result = CASE
                            WHEN COALESCE(arbitration_result, '') = '' THEN COALESCE(?, arbitration_result)
                            ELSE arbitration_result
                        END
                    WHERE move_id = ?
                    """,
                    (
                        strategy or None,
                        arbitration or None,
                        int(move_id or 0),
                    ),
                )
    except sqlite3.OperationalError:
        pass
    # Table for explicit admin curation actions (move/delete) triggered from Tools.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS library_curation_actions (
            action_id INTEGER PRIMARY KEY AUTOINCREMENT,
            album_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_title TEXT NOT NULL,
            folder_path TEXT NOT NULL,
            action TEXT NOT NULL,
            destination_path TEXT,
            status TEXT NOT NULL DEFAULT 'completed',
            reason_json TEXT NOT NULL DEFAULT '[]',
            created_at REAL NOT NULL,
            user_id INTEGER NOT NULL DEFAULT 0,
            username TEXT NOT NULL DEFAULT ''
        )
    """)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_library_curation_actions_album ON library_curation_actions(album_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_library_curation_actions_created ON library_curation_actions(created_at DESC)")
    except sqlite3.OperationalError:
        pass
    # Table for incomplete-albums scan diagnostics (double-check Plex vs disk)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS incomplete_album_diagnostics (
            run_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            title_raw TEXT,
            folder TEXT,
            classification TEXT,
            missing_in_plex TEXT,
            missing_on_disk TEXT,
            track_titles TEXT,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            detected_at REAL,
            PRIMARY KEY (run_id, artist, album_id),
            FOREIGN KEY (run_id) REFERENCES scan_history(scan_id)
        )
    """)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_incomplete_diagnostics_run ON incomplete_album_diagnostics(run_id)")
    except sqlite3.OperationalError:
        pass
    # Table for per-edition scan truth (Library, Tag Fixer read from here when available)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_editions (
            scan_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            title_raw TEXT,
            folder TEXT,
            fmt_text TEXT,
            br INTEGER,
            sr INTEGER,
            bd INTEGER,
            meta_json TEXT,
            musicbrainz_id TEXT,
            musicbrainz_release_id TEXT,
            is_broken INTEGER DEFAULT 0,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            missing_indices TEXT,
            has_cover INTEGER DEFAULT 0,
            missing_required_tags TEXT,
            strict_match_verified INTEGER NOT NULL DEFAULT 0,
            strict_match_provider TEXT DEFAULT '',
            strict_reject_reason TEXT DEFAULT '',
            strict_tracklist_score REAL NOT NULL DEFAULT 0.0,
            PRIMARY KEY (scan_id, artist, album_id),
            FOREIGN KEY (scan_id) REFERENCES scan_history(scan_id)
        )
    """)
    # scan_editions provider identity columns (for Library badges/source-of-truth in Plex mode)
    try:
        cur.execute("PRAGMA table_info(scan_editions)")
        se_cols = [r[1] for r in cur.fetchall()]
        if "musicbrainz_release_id" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN musicbrainz_release_id TEXT")
        if "discogs_release_id" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN discogs_release_id TEXT")
        if "lastfm_album_mbid" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN lastfm_album_mbid TEXT")
        if "bandcamp_album_url" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN bandcamp_album_url TEXT")
        if "metadata_source" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN metadata_source TEXT")
        if "strict_match_verified" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN strict_match_verified INTEGER NOT NULL DEFAULT 0")
        if "strict_match_provider" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN strict_match_provider TEXT DEFAULT ''")
        if "strict_reject_reason" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN strict_reject_reason TEXT DEFAULT ''")
        if "strict_tracklist_score" not in se_cols:
            cur.execute("ALTER TABLE scan_editions ADD COLUMN strict_tracklist_score REAL NOT NULL DEFAULT 0.0")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_editions_strict_match_verified ON scan_editions(scan_id, strict_match_verified)")
    except sqlite3.OperationalError:
        pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_pipeline_trace (
            scan_id INTEGER NOT NULL,
            artist TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            album_title TEXT,
            folder TEXT NOT NULL DEFAULT '',
            folder_name TEXT DEFAULT '',
            fmt_text TEXT DEFAULT '',
            metadata_source TEXT DEFAULT '',
            strict_match_verified INTEGER NOT NULL DEFAULT 0,
            strict_match_provider TEXT DEFAULT '',
            strict_reject_reason TEXT DEFAULT '',
            strict_tracklist_score REAL NOT NULL DEFAULT 0.0,
            has_cover INTEGER DEFAULT 0,
            is_broken INTEGER DEFAULT 0,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            missing_indices TEXT,
            missing_required_tags TEXT,
            has_musicbrainz INTEGER DEFAULT 0,
            has_discogs INTEGER DEFAULT 0,
            has_lastfm INTEGER DEFAULT 0,
            has_bandcamp INTEGER DEFAULT 0,
            musicbrainz_release_id TEXT DEFAULT '',
            discogs_release_id TEXT DEFAULT '',
            lastfm_album_mbid TEXT DEFAULT '',
            bandcamp_album_url TEXT DEFAULT '',
            dupe_role TEXT DEFAULT 'none',
            dupe_signal TEXT DEFAULT '',
            dupe_peer_count INTEGER DEFAULT 0,
            dupe_needs_ai INTEGER DEFAULT 0,
            no_move INTEGER DEFAULT 0,
            manual_review INTEGER DEFAULT 0,
            same_folder INTEGER DEFAULT 0,
            winner_album_id INTEGER,
            winner_title TEXT DEFAULT '',
            ai_used INTEGER DEFAULT 0,
            ai_provider TEXT DEFAULT '',
            ai_model TEXT DEFAULT '',
            pipeline_status TEXT DEFAULT 'active',
            move_reason TEXT DEFAULT '',
            move_status TEXT DEFAULT 'none',
            moved_to_path TEXT DEFAULT '',
            decision_provider TEXT DEFAULT '',
            decision_reason TEXT DEFAULT '',
            decision_confidence REAL,
            timeline_json TEXT NOT NULL DEFAULT '[]',
            meta_summary_json TEXT NOT NULL DEFAULT '{}',
            updated_at REAL NOT NULL,
            PRIMARY KEY (scan_id, artist, album_id, folder),
            FOREIGN KEY (scan_id) REFERENCES scan_history(scan_id)
        )
    """)
    cur.execute("PRAGMA table_info(scan_pipeline_trace)")
    trace_cols = [r[1] for r in cur.fetchall()]
    for col_name, col_type in [
        ("pipeline_status", "TEXT DEFAULT 'active'"),
        ("move_reason", "TEXT DEFAULT ''"),
        ("move_status", "TEXT DEFAULT 'none'"),
        ("moved_to_path", "TEXT DEFAULT ''"),
        ("decision_provider", "TEXT DEFAULT ''"),
        ("decision_reason", "TEXT DEFAULT ''"),
        ("decision_confidence", "REAL"),
    ]:
        if col_name not in trace_cols:
            cur.execute(f"ALTER TABLE scan_pipeline_trace ADD COLUMN {col_name} {col_type}")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_pipeline_trace_scan_updated ON scan_pipeline_trace(scan_id, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_pipeline_trace_scan_artist ON scan_pipeline_trace(scan_id, artist)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_pipeline_trace_scan_dupe ON scan_pipeline_trace(scan_id, dupe_role)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_pipeline_trace_scan_broken ON scan_pipeline_trace(scan_id, is_broken)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_pipeline_trace_scan_folder ON scan_pipeline_trace(scan_id, folder)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_pipeline_trace_scan_status ON scan_pipeline_trace(scan_id, pipeline_status)")
    except sqlite3.OperationalError:
        pass
    # Persistent resume state for interrupted scans (artist-level status machine).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_resume_runs (
            run_id TEXT PRIMARY KEY,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            mode TEXT NOT NULL,
            scan_type TEXT NOT NULL,
            source_signature TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            scan_id INTEGER,
            detected_artists_total INTEGER NOT NULL DEFAULT 0,
            detected_albums_total INTEGER NOT NULL DEFAULT 0,
            detected_tracks_total INTEGER NOT NULL DEFAULT 0,
            plan_snapshot_ready INTEGER NOT NULL DEFAULT 0,
            discovery_snapshot_ready INTEGER NOT NULL DEFAULT 0,
            discovery_stage TEXT NOT NULL DEFAULT '',
            discovery_state_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_resume_artists (
            run_id TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            artist_signature TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            album_count INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL,
            error TEXT,
            PRIMARY KEY (run_id, artist_name),
            FOREIGN KEY (run_id) REFERENCES scan_resume_runs(run_id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_resume_files_plan (
            run_id TEXT NOT NULL,
            album_id INTEGER NOT NULL,
            artist_name TEXT NOT NULL,
            artist_order INTEGER NOT NULL DEFAULT 0,
            album_order INTEGER NOT NULL DEFAULT 0,
            album_title TEXT,
            album_norm TEXT,
            folder_path TEXT NOT NULL,
            fingerprint TEXT,
            file_count INTEGER NOT NULL DEFAULT 0,
            source_id INTEGER,
            has_cover INTEGER NOT NULL DEFAULT 0,
            has_artist_image INTEGER NOT NULL DEFAULT 0,
            has_mbid INTEGER NOT NULL DEFAULT 0,
            has_identity INTEGER NOT NULL DEFAULT 0,
            identity_provider TEXT,
            strict_match_verified INTEGER NOT NULL DEFAULT 0,
            strict_match_provider TEXT,
            strict_reject_reason TEXT,
            strict_tracklist_score REAL NOT NULL DEFAULT 0.0,
            musicbrainz_id TEXT,
            discogs_release_id TEXT,
            lastfm_album_mbid TEXT,
            bandcamp_album_url TEXT,
            metadata_source TEXT,
            missing_required_tags_json TEXT NOT NULL DEFAULT '[]',
            skip_heavy_processing INTEGER NOT NULL DEFAULT 0,
            lookup_artist_name TEXT,
            lookup_album_title TEXT,
            storage_provider TEXT,
            storage_device_id TEXT,
            storage_device_label TEXT,
            storage_bucket_order INTEGER NOT NULL DEFAULT 0,
            storage_rel_path TEXT,
            storage_access_path TEXT,
            PRIMARY KEY (run_id, album_id),
            FOREIGN KEY (run_id) REFERENCES scan_resume_runs(run_id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_storage_buckets (
            run_id TEXT NOT NULL,
            bucket_order INTEGER NOT NULL DEFAULT 0,
            storage_provider TEXT NOT NULL DEFAULT '',
            storage_device_id TEXT NOT NULL DEFAULT '',
            storage_device_label TEXT NOT NULL DEFAULT '',
            canonical_root TEXT NOT NULL DEFAULT '',
            access_root TEXT NOT NULL DEFAULT '',
            albums_total INTEGER NOT NULL DEFAULT 0,
            albums_done INTEGER NOT NULL DEFAULT 0,
            started_at REAL,
            finished_at REAL,
            status TEXT NOT NULL DEFAULT 'pending',
            message TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (run_id, bucket_order, storage_device_id, canonical_root),
            FOREIGN KEY (run_id) REFERENCES scan_resume_runs(run_id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_jobs (
            job_type TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'global',
            run_id TEXT,
            status TEXT NOT NULL DEFAULT 'idle',
            phase TEXT NOT NULL DEFAULT '',
            current INTEGER NOT NULL DEFAULT 0,
            total INTEGER NOT NULL DEFAULT 0,
            current_item TEXT NOT NULL DEFAULT '',
            message TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            started_at REAL,
            heartbeat_at REAL,
            finished_at REAL,
            meta_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (job_type, scope)
        )
    """)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status ON pipeline_jobs(status, heartbeat_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_type ON pipeline_jobs(job_type, scope)")
    except sqlite3.OperationalError:
        pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_resume_discovery_files (
            run_id TEXT NOT NULL,
            root_index INTEGER NOT NULL DEFAULT 0,
            file_path TEXT NOT NULL,
            PRIMARY KEY (run_id, file_path),
            FOREIGN KEY (run_id) REFERENCES scan_resume_runs(run_id) ON DELETE CASCADE
        )
    """)
    # Fast incremental cache (folder fingerprint + quality flags) used by changed-only scans.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files_album_scan_cache (
            folder_path TEXT PRIMARY KEY,
            source_id INTEGER,
            fingerprint TEXT NOT NULL,
            ordered_paths_json TEXT NOT NULL DEFAULT '[]',
            artist_name TEXT,
            album_title TEXT,
            has_cover INTEGER NOT NULL DEFAULT 0,
            has_artist_image INTEGER NOT NULL DEFAULT 0,
            has_complete_tags INTEGER NOT NULL DEFAULT 0,
            has_mbid INTEGER NOT NULL DEFAULT 0,
            has_identity INTEGER NOT NULL DEFAULT 0,
            identity_provider TEXT,
            strict_match_verified INTEGER NOT NULL DEFAULT 0,
            strict_match_provider TEXT,
            strict_reject_reason TEXT,
            strict_tracklist_score REAL NOT NULL DEFAULT 0.0,
            musicbrainz_id TEXT,
            musicbrainz_release_id TEXT,
            discogs_release_id TEXT,
            lastfm_album_mbid TEXT,
            bandcamp_album_url TEXT,
            metadata_source TEXT,
            missing_required_tags TEXT NOT NULL DEFAULT '[]',
            last_scan_id INTEGER,
            updated_at REAL NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files_dir_scan_cache (
            dir_path TEXT PRIMARY KEY,
            source_id INTEGER,
            root_path TEXT,
            relative_depth INTEGER NOT NULL DEFAULT 0,
            fingerprint TEXT NOT NULL,
            subtree_audio_count INTEGER NOT NULL DEFAULT 0,
            subtree_album_count INTEGER NOT NULL DEFAULT 0,
            subtree_entry_estimate INTEGER NOT NULL DEFAULT 0,
            album_folders_json TEXT NOT NULL DEFAULT '[]',
            updated_at REAL NOT NULL
        )
    """)
    # Files watcher queue: pending changed folders/albums to speed up changed-only scans.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files_pending_changes (
            folder_path TEXT PRIMARY KEY,
            source_id INTEGER,
            event_kind TEXT,
            event_path TEXT,
            reason TEXT,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 1
        )
    """)
    # Bootstrap/autonomy lifecycle state.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_bootstrap_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            bootstrap_required INTEGER NOT NULL DEFAULT 1,
            autonomous_mode INTEGER NOT NULL DEFAULT 0,
            first_full_scan_id INTEGER,
            first_full_completed_at REAL,
            updated_at REAL NOT NULL
        )
        """
    )
    # Files source roots with explicit role (library/incoming) + winner root.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS files_source_roots (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL DEFAULT 'library',
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            is_winner_root INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_source_roots_role_enabled ON files_source_roots(role, enabled, priority)")
    except sqlite3.OperationalError:
        pass
    # Task event stream (used by in-app notifications/toasts and scheduler observability).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS task_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            job_type TEXT NOT NULL,
            scope TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            metrics_json TEXT,
            summary_json TEXT,
            error TEXT,
            source TEXT,
            started_at REAL NOT NULL,
            ended_at REAL,
            duration_ms INTEGER
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_task_events_event_id ON task_events(event_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_task_events_started ON task_events(started_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_task_events_job_status ON task_events(job_type, status)")
    except sqlite3.OperationalError:
        pass
    # Scheduler rules (interval / weekly with job scope and optional post-scan chaining).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            trigger_type TEXT NOT NULL,
            interval_min INTEGER,
            days_of_week TEXT,
            time_local TEXT,
            scope TEXT NOT NULL DEFAULT 'both',
            post_scan_chain INTEGER NOT NULL DEFAULT 0,
            priority INTEGER NOT NULL DEFAULT 50,
            max_concurrency INTEGER NOT NULL DEFAULT 1,
            next_run_ts REAL,
            last_run_ts REAL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_rules_enabled ON scheduler_rules(enabled, job_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_rules_next_run ON scheduler_rules(next_run_ts)")
    except sqlite3.OperationalError:
        pass
    # Scheduler jobs/runs (queue + runtime history).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduler_jobs (
            job_run_id TEXT PRIMARY KEY,
            rule_id INTEGER,
            job_type TEXT NOT NULL,
            scope TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            metrics_json TEXT,
            error TEXT,
            origin_scan_id INTEGER,
            created_at REAL NOT NULL,
            started_at REAL,
            ended_at REAL,
            duration_ms INTEGER,
            FOREIGN KEY (rule_id) REFERENCES scheduler_rules(rule_id)
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_status ON scheduler_jobs(status, created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_job_type ON scheduler_jobs(job_type, created_at DESC)")
    except sqlite3.OperationalError:
        pass
    # Scheduler jobs migration: origin scan attribution for post-scan chained jobs.
    cur.execute("PRAGMA table_info(scheduler_jobs)")
    scheduler_job_cols = {r[1] for r in cur.fetchall()}
    if "origin_scan_id" not in scheduler_job_cols:
        try:
            cur.execute("ALTER TABLE scheduler_jobs ADD COLUMN origin_scan_id INTEGER")
        except sqlite3.OperationalError:
            pass
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scheduler_jobs_origin_status ON scheduler_jobs(origin_scan_id, status)")
    except sqlite3.OperationalError:
        pass
    # Persisted per-AI-call usage + pricing snapshot (ledger).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_call_usage (
            call_id TEXT PRIMARY KEY,
            created_at REAL NOT NULL,
            scan_id INTEGER,
            origin_scan_id INTEGER,
            album_id INTEGER,
            album_artist TEXT,
            album_title TEXT,
            scheduler_job_id TEXT,
            run_id TEXT,
            phase TEXT NOT NULL,
            job_type TEXT,
            scope TEXT,
            analysis_type TEXT NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            endpoint_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            latency_ms INTEGER,
            request_id TEXT,
            input_tokens INTEGER,
            cached_input_tokens INTEGER,
            output_tokens INTEGER,
            total_tokens INTEGER,
            image_inputs INTEGER,
            pricing_version TEXT,
            rate_input_microusd_per_1m INTEGER,
            rate_cached_input_microusd_per_1m INTEGER,
            rate_output_microusd_per_1m INTEGER,
            rate_image_microusd_per_image INTEGER,
            cost_input_microusd INTEGER,
            cost_cached_input_microusd INTEGER,
            cost_output_microusd INTEGER,
            cost_image_microusd INTEGER,
            cost_total_microusd INTEGER,
            usage_source TEXT NOT NULL,
            error_code TEXT,
            error_message TEXT,
            metadata_json TEXT
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_call_usage_scan ON ai_call_usage(scan_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_call_usage_origin ON ai_call_usage(origin_scan_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_call_usage_analysis ON ai_call_usage(analysis_type, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_call_usage_album ON ai_call_usage(album_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_call_usage_scan_album ON ai_call_usage(scan_id, album_id, created_at)")
    except sqlite3.OperationalError:
        pass
    cur.execute("PRAGMA table_info(ai_call_usage)")
    ai_usage_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("album_id", "INTEGER"),
        ("album_artist", "TEXT"),
        ("album_title", "TEXT"),
    ]:
        if col_name not in ai_usage_cols:
            try:
                cur.execute(f"ALTER TABLE ai_call_usage ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError:
                pass
    # Per-scan rollups (lifecycle view = scan + origin_scan_id chained jobs).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_scan_cost_rollups (
            scan_id INTEGER PRIMARY KEY,
            calls_total INTEGER NOT NULL DEFAULT 0,
            input_tokens INTEGER NOT NULL DEFAULT 0,
            cached_input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            cost_total_microusd INTEGER NOT NULL DEFAULT 0,
            unpriced_calls INTEGER NOT NULL DEFAULT 0,
            lifecycle_complete INTEGER NOT NULL DEFAULT 0,
            breakdown_by_analysis_json TEXT NOT NULL DEFAULT '{}',
            breakdown_by_job_json TEXT NOT NULL DEFAULT '{}',
            breakdown_by_model_json TEXT NOT NULL DEFAULT '{}',
            updated_at REAL NOT NULL
        )
        """
    )
    # Persistent web/review query cache (positive + negative), used to avoid repeated external lookups.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_query_cache (
            cache_key TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            query_text TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT,
            results_json TEXT NOT NULL DEFAULT '[]',
            hit_count INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            expires_at REAL NOT NULL
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_query_cache_scope_status ON ai_query_cache(scope, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_query_cache_expires ON ai_query_cache(expires_at)")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_override_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            domain TEXT NOT NULL,
            target_key TEXT NOT NULL,
            action TEXT NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_override_events_domain ON ai_override_events(domain, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_override_events_target ON ai_override_events(target_key, created_at)")
    except sqlite3.OperationalError:
        pass
    # Metadata worker queue scaffolding (local orchestrator can enqueue normalized album manifests
    # for external/hybrid metadata workers without moving raw audio off-box).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata_jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'queued',
            priority INTEGER NOT NULL DEFAULT 50,
            queue_name TEXT NOT NULL DEFAULT 'metadata',
            scope TEXT NOT NULL DEFAULT 'album',
            album_manifest_json TEXT NOT NULL,
            provider_hints_json TEXT NOT NULL DEFAULT '{}',
            cache_keys_json TEXT NOT NULL DEFAULT '[]',
            selected_provider TEXT,
            strict_match_verified INTEGER NOT NULL DEFAULT 0,
            strict_reject_reason TEXT,
            confidence REAL,
            result_json TEXT NOT NULL DEFAULT '{}',
            run_id TEXT,
            scan_id INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            worker_id TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            started_at REAL,
            finished_at REAL,
            last_error TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata_job_attempts (
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL,
            worker_id TEXT,
            status TEXT NOT NULL,
            provider TEXT,
            trace_json TEXT NOT NULL DEFAULT '{}',
            error TEXT,
            created_at REAL NOT NULL,
            started_at REAL,
            finished_at REAL,
            FOREIGN KEY (job_id) REFERENCES metadata_jobs(job_id) ON DELETE CASCADE
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metadata_jobs_status_priority ON metadata_jobs(status, priority DESC, created_at ASC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metadata_jobs_scan ON metadata_jobs(scan_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metadata_attempts_job ON metadata_job_attempts(job_id, attempt_no DESC)")
    except sqlite3.OperationalError:
        pass
    # Pricing catalog (versioned rates used to compute exact per-call USD).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_pricing_catalog (
            pricing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            endpoint_kind TEXT NOT NULL,
            pricing_version TEXT NOT NULL,
            rate_input_microusd_per_1m INTEGER,
            rate_cached_input_microusd_per_1m INTEGER,
            rate_output_microusd_per_1m INTEGER,
            rate_image_microusd_per_image INTEGER,
            effective_from REAL NOT NULL,
            effective_to REAL,
            UNIQUE(provider, model, endpoint_kind, pricing_version, effective_from)
        )
        """
    )
    for (
        provider,
        model,
        endpoint_kind,
        rate_input,
        rate_cached_input,
        rate_output,
        rate_image,
        pricing_version_num,
        effective_from,
        effective_to,
    ) in ai_pricing_default_rows:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO ai_pricing_catalog
                (provider, model, endpoint_kind, pricing_version,
                 rate_input_microusd_per_1m, rate_cached_input_microusd_per_1m,
                 rate_output_microusd_per_1m, rate_image_microusd_per_image,
                 effective_from, effective_to)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(provider or "").strip().lower(),
                    str(model or "").strip(),
                    str(endpoint_kind or "text").strip().lower(),
                    f"{ai_pricing_version}:{int(pricing_version_num or 1)}",
                    int(rate_input or 0),
                    int(rate_cached_input or 0),
                    int(rate_output or 0),
                    int(rate_image or 0),
                    float(effective_from or 0.0),
                    float(effective_to) if effective_to is not None else None,
                ),
            )
        except sqlite3.OperationalError:
            pass
    # Progressive files-library publication state (source for live Files index rebuilds during scan).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files_library_published_albums (
            folder_path TEXT PRIMARY KEY,
            source_id INTEGER,
            scan_id INTEGER,
            artist_name TEXT NOT NULL,
            artist_norm TEXT NOT NULL,
            album_title TEXT NOT NULL,
            title_norm TEXT NOT NULL,
            year INTEGER,
            date_text TEXT,
            genre TEXT,
            label TEXT,
            tags_json TEXT NOT NULL DEFAULT '[]',
            format TEXT,
            is_lossless INTEGER NOT NULL DEFAULT 0,
            has_cover INTEGER NOT NULL DEFAULT 0,
            cover_path TEXT,
            has_artist_image INTEGER NOT NULL DEFAULT 0,
            artist_image_path TEXT,
            mb_identified INTEGER NOT NULL DEFAULT 0,
            strict_match_verified INTEGER NOT NULL DEFAULT 0,
            strict_match_provider TEXT,
            strict_reject_reason TEXT,
            strict_tracklist_score REAL NOT NULL DEFAULT 0.0,
            musicbrainz_release_group_id TEXT,
            musicbrainz_release_id TEXT,
            discogs_release_id TEXT,
            lastfm_album_mbid TEXT,
            bandcamp_album_url TEXT,
            primary_metadata_source TEXT,
            track_count INTEGER NOT NULL DEFAULT 0,
            total_duration_sec INTEGER NOT NULL DEFAULT 0,
            is_broken INTEGER NOT NULL DEFAULT 0,
            expected_track_count INTEGER,
            actual_track_count INTEGER,
            missing_indices_json TEXT NOT NULL DEFAULT '[]',
            missing_required_tags_json TEXT NOT NULL DEFAULT '[]',
            primary_tags_json TEXT NOT NULL DEFAULT '{}',
            tracks_json TEXT NOT NULL DEFAULT '[]',
            fingerprint TEXT,
            updated_at REAL NOT NULL
        )
    """)
    # Backward-compatible schema evolution for files_album_scan_cache.
    cur.execute("PRAGMA table_info(files_album_scan_cache)")
    files_cache_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("source_id", "INTEGER"),
        ("ordered_paths_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("musicbrainz_id", "TEXT"),
        ("has_identity", "INTEGER NOT NULL DEFAULT 0"),
        ("identity_provider", "TEXT"),
        ("discogs_release_id", "TEXT"),
        ("lastfm_album_mbid", "TEXT"),
        ("bandcamp_album_url", "TEXT"),
        ("metadata_source", "TEXT"),
        ("strict_match_verified", "INTEGER NOT NULL DEFAULT 0"),
        ("strict_match_provider", "TEXT"),
        ("strict_reject_reason", "TEXT"),
        ("strict_tracklist_score", "REAL NOT NULL DEFAULT 0.0"),
        ("musicbrainz_release_id", "TEXT"),
    ]:
        if col_name not in files_cache_cols:
            cur.execute(f"ALTER TABLE files_album_scan_cache ADD COLUMN {col_name} {col_type}")
    cur.execute("PRAGMA table_info(files_dir_scan_cache)")
    files_dir_cache_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("source_id", "INTEGER"),
        ("root_path", "TEXT"),
        ("relative_depth", "INTEGER NOT NULL DEFAULT 0"),
        ("fingerprint", "TEXT NOT NULL DEFAULT ''"),
        ("subtree_audio_count", "INTEGER NOT NULL DEFAULT 0"),
        ("subtree_album_count", "INTEGER NOT NULL DEFAULT 0"),
        ("subtree_entry_estimate", "INTEGER NOT NULL DEFAULT 0"),
        ("album_folders_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("updated_at", "REAL NOT NULL DEFAULT 0"),
    ]:
        if col_name not in files_dir_cache_cols:
            cur.execute(f"ALTER TABLE files_dir_scan_cache ADD COLUMN {col_name} {col_type}")
    # Backward-compatible schema evolution for files_library_published_albums.
    cur.execute("PRAGMA table_info(files_library_published_albums)")
    files_published_cols = {r[1] for r in cur.fetchall()}
    files_published_new_cols = [
        ("source_id", "INTEGER"),
        ("scan_id", "INTEGER"),
        ("artist_name", "TEXT NOT NULL DEFAULT ''"),
        ("artist_norm", "TEXT NOT NULL DEFAULT ''"),
        ("album_title", "TEXT NOT NULL DEFAULT ''"),
        ("title_norm", "TEXT NOT NULL DEFAULT ''"),
        ("year", "INTEGER"),
        ("date_text", "TEXT"),
        ("genre", "TEXT"),
        ("label", "TEXT"),
        ("tags_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("format", "TEXT"),
        ("is_lossless", "INTEGER NOT NULL DEFAULT 0"),
        ("has_cover", "INTEGER NOT NULL DEFAULT 0"),
        ("cover_path", "TEXT"),
        ("has_artist_image", "INTEGER NOT NULL DEFAULT 0"),
        ("artist_image_path", "TEXT"),
        ("mb_identified", "INTEGER NOT NULL DEFAULT 0"),
        ("strict_match_verified", "INTEGER NOT NULL DEFAULT 0"),
        ("strict_match_provider", "TEXT"),
        ("strict_reject_reason", "TEXT"),
        ("strict_tracklist_score", "REAL NOT NULL DEFAULT 0.0"),
        ("musicbrainz_release_group_id", "TEXT"),
        ("musicbrainz_release_id", "TEXT"),
        ("discogs_release_id", "TEXT"),
        ("lastfm_album_mbid", "TEXT"),
        ("bandcamp_album_url", "TEXT"),
        ("primary_metadata_source", "TEXT"),
        ("track_count", "INTEGER NOT NULL DEFAULT 0"),
        ("total_duration_sec", "INTEGER NOT NULL DEFAULT 0"),
        ("is_broken", "INTEGER NOT NULL DEFAULT 0"),
        ("expected_track_count", "INTEGER"),
        ("actual_track_count", "INTEGER"),
        ("missing_indices_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("missing_required_tags_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("primary_tags_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("tracks_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("fingerprint", "TEXT"),
        ("updated_at", "REAL NOT NULL DEFAULT 0"),
    ]
    for col_name, col_type in files_published_new_cols:
        if col_name not in files_published_cols:
            cur.execute(f"ALTER TABLE files_library_published_albums ADD COLUMN {col_name} {col_type}")
    # Backward-compatible schema evolution for files_pending_changes.
    cur.execute("PRAGMA table_info(files_pending_changes)")
    files_pending_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("source_id", "INTEGER"),
        ("event_kind", "TEXT"),
        ("event_path", "TEXT"),
    ]:
        if col_name not in files_pending_cols:
            cur.execute(f"ALTER TABLE files_pending_changes ADD COLUMN {col_name} {col_type}")
    # Ensure bootstrap singleton row exists.
    now_bootstrap = time.time()
    cur.execute(
        """
        INSERT OR IGNORE INTO pipeline_bootstrap_state
        (id, bootstrap_required, autonomous_mode, first_full_scan_id, first_full_completed_at, updated_at)
        VALUES (1, 1, 0, NULL, NULL, ?)
        """,
        (now_bootstrap,),
    )
    cur.execute("PRAGMA table_info(scan_resume_runs)")
    resume_run_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("detected_artists_total", "INTEGER NOT NULL DEFAULT 0"),
        ("detected_albums_total", "INTEGER NOT NULL DEFAULT 0"),
        ("detected_tracks_total", "INTEGER NOT NULL DEFAULT 0"),
        ("plan_snapshot_ready", "INTEGER NOT NULL DEFAULT 0"),
        ("discovery_snapshot_ready", "INTEGER NOT NULL DEFAULT 0"),
        ("discovery_stage", "TEXT NOT NULL DEFAULT ''"),
        ("discovery_state_json", "TEXT NOT NULL DEFAULT '{}'"),
    ]:
        if col_name not in resume_run_cols:
            cur.execute(f"ALTER TABLE scan_resume_runs ADD COLUMN {col_name} {col_type}")
    cur.execute("PRAGMA table_info(scan_resume_files_plan)")
    resume_plan_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("storage_provider", "TEXT"),
        ("storage_device_id", "TEXT"),
        ("storage_device_label", "TEXT"),
        ("storage_bucket_order", "INTEGER NOT NULL DEFAULT 0"),
        ("storage_rel_path", "TEXT"),
        ("storage_access_path", "TEXT"),
    ]:
        if col_name not in resume_plan_cols:
            cur.execute(f"ALTER TABLE scan_resume_files_plan ADD COLUMN {col_name} {col_type}")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_resume_runs_source ON scan_resume_runs(source_signature, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_resume_artists_run_status ON scan_resume_artists(run_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_resume_files_plan_run_artist ON scan_resume_files_plan(run_id, artist_order, album_order)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_resume_files_plan_run_folder ON scan_resume_files_plan(run_id, folder_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_resume_files_plan_run_storage ON scan_resume_files_plan(run_id, storage_bucket_order, storage_device_id, album_order)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_storage_buckets_run_status ON scan_storage_buckets(run_id, status, bucket_order)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scan_resume_discovery_files_run_root ON scan_resume_discovery_files(run_id, root_index, file_path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_album_scan_cache_updated ON files_album_scan_cache(updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_album_scan_cache_source_updated ON files_album_scan_cache(source_id, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_dir_scan_cache_root_depth ON files_dir_scan_cache(root_path, relative_depth)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_dir_scan_cache_updated ON files_dir_scan_cache(updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_artist_norm ON files_library_published_albums(artist_norm)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_mb_rg ON files_library_published_albums(musicbrainz_release_group_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_artist_album ON files_library_published_albums(artist_norm, title_norm)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_source_updated ON files_library_published_albums(source_id, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_updated ON files_library_published_albums(updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_visible_updated ON files_library_published_albums(is_broken, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_visible_artist ON files_library_published_albums(is_broken, artist_norm, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_visible_strict_updated ON files_library_published_albums(is_broken, strict_match_verified, mb_identified, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_published_year_updated ON files_library_published_albums(year, updated_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_pending_changes_last_seen ON files_pending_changes(last_seen DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_pending_changes_source_last_seen ON files_pending_changes(source_id, last_seen DESC)")
    except sqlite3.OperationalError:
        pass
    con.commit()
    con.close()



def init_settings_db(*, settings_db_file) -> None:
    """Initialize the dedicated settings.db used for all persistent configuration."""
    con = sqlite3.connect(str(settings_db_file), timeout=10)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    con.commit()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            password_salt TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            can_download INTEGER NOT NULL DEFAULT 0,
            can_view_statistics INTEGER NOT NULL DEFAULT 0,
            allow_ai_calls INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            accept_shares INTEGER NOT NULL DEFAULT 1,
            share_liked_public INTEGER NOT NULL DEFAULT 0,
            share_recommendations_public INTEGER NOT NULL DEFAULT 0,
            avatar_data_url TEXT,
            concerts_filter_enabled INTEGER NOT NULL DEFAULT 0,
            concerts_home_lat TEXT,
            concerts_home_lon TEXT,
            concerts_radius_km TEXT NOT NULL DEFAULT '150',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_login_at INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_sessions (
            token_hash TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            last_used_at INTEGER NOT NULL,
            ip TEXT,
            user_agent TEXT,
            FOREIGN KEY(user_id) REFERENCES auth_users(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_failure_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            failure_ts INTEGER NOT NULL,
            path TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT ''
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_ip_bans (
            ip TEXT PRIMARY KEY,
            ban_until INTEGER NOT NULL,
            fail_count INTEGER NOT NULL DEFAULT 0,
            window_sec INTEGER NOT NULL DEFAULT 0,
            last_reason TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_users_admin_active ON auth_users(is_admin, is_active)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_users_username ON auth_users(username)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_failure_events_subject ON auth_failure_events(kind, subject_key, failure_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_failure_events_ts ON auth_failure_events(failure_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_ip_bans_until ON auth_ip_bans(ban_until)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_auth_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 0,
            provider_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            account_id TEXT,
            access_token_enc TEXT,
            refresh_token_enc TEXT,
            expires_at INTEGER,
            meta_json TEXT NOT NULL DEFAULT '{}',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_provider_preferences (
            user_id INTEGER PRIMARY KEY,
            interactive_provider_id TEXT NOT NULL,
            batch_provider_id TEXT NOT NULL,
            web_search_provider_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_auth_profiles_provider_active ON ai_auth_profiles(provider_id, is_active, updated_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_auth_profiles_user_provider ON ai_auth_profiles(user_id, provider_id, is_active)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_runtime_bundles (
            bundle_type TEXT PRIMARY KEY,
            mode TEXT NOT NULL DEFAULT 'absent',
            state TEXT NOT NULL DEFAULT 'idle',
            phase TEXT NOT NULL DEFAULT '',
            phase_message TEXT NOT NULL DEFAULT '',
            config_root TEXT NOT NULL DEFAULT '',
            data_root TEXT NOT NULL DEFAULT '',
            install_root TEXT NOT NULL DEFAULT '',
            effective_url TEXT NOT NULL DEFAULT '',
            ownership TEXT NOT NULL DEFAULT '',
            health_json TEXT NOT NULL DEFAULT '{}',
            services_json TEXT NOT NULL DEFAULT '[]',
            meta_json TEXT NOT NULL DEFAULT '{}',
            last_error TEXT NOT NULL DEFAULT '',
            update_state_json TEXT NOT NULL DEFAULT '{}',
            created_at REAL NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_runtime_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bundle_type TEXT NOT NULL,
            service_name TEXT NOT NULL DEFAULT '',
            level TEXT NOT NULL DEFAULT 'info',
            message TEXT NOT NULL,
            created_at REAL NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managed_runtime_actions (
            action_id TEXT PRIMARY KEY,
            bundle_type TEXT NOT NULL,
            action TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            payload_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL DEFAULT 0,
            completed_at REAL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_runtime_logs_bundle_ts ON managed_runtime_logs(bundle_type, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_managed_runtime_actions_bundle_status ON managed_runtime_actions(bundle_type, status, created_at DESC)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mcp_service_tokens (
            token_id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT 'default',
            token_hash TEXT NOT NULL UNIQUE,
            scopes_json TEXT NOT NULL DEFAULT '[]',
            active INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            expires_at INTEGER,
            last_used_at INTEGER,
            revoked_at INTEGER
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mcp_service_tokens_active ON mcp_service_tokens(active, created_at DESC)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mcp_audit_log (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id TEXT,
            tool TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            message TEXT NOT NULL DEFAULT '',
            args_json TEXT NOT NULL DEFAULT '{}',
            duration_ms INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            ip TEXT NOT NULL DEFAULT '',
            user_agent TEXT NOT NULL DEFAULT ''
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mcp_audit_log_created ON mcp_audit_log(created_at DESC)")

    # Backward-compatible schema upgrades.
    cur.execute("PRAGMA table_info(auth_users)")
    auth_user_cols = {r[1] for r in cur.fetchall()}
    if "can_download" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN can_download INTEGER NOT NULL DEFAULT 0")
    if "can_view_statistics" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN can_view_statistics INTEGER NOT NULL DEFAULT 0")
    if "allow_ai_calls" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN allow_ai_calls INTEGER NOT NULL DEFAULT 1")
    if "is_active" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if "accept_shares" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN accept_shares INTEGER NOT NULL DEFAULT 1")
    if "share_liked_public" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN share_liked_public INTEGER NOT NULL DEFAULT 0")
    if "share_recommendations_public" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN share_recommendations_public INTEGER NOT NULL DEFAULT 0")
    if "avatar_data_url" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN avatar_data_url TEXT")
    if "concerts_filter_enabled" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN concerts_filter_enabled INTEGER NOT NULL DEFAULT 0")
    if "concerts_home_lat" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN concerts_home_lat TEXT")
    if "concerts_home_lon" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN concerts_home_lon TEXT")
    if "concerts_radius_km" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN concerts_radius_km TEXT NOT NULL DEFAULT '150'")
    if "last_login_at" not in auth_user_cols:
        cur.execute("ALTER TABLE auth_users ADD COLUMN last_login_at INTEGER")

    cur.execute("PRAGMA table_info(ai_auth_profiles)")
    ai_auth_cols = {r[1] for r in cur.fetchall()}
    ai_auth_new_cols = [
        ("account_id", "TEXT"),
        ("access_token_enc", "TEXT"),
        ("refresh_token_enc", "TEXT"),
        ("expires_at", "INTEGER"),
        ("meta_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("is_active", "INTEGER NOT NULL DEFAULT 1"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
        ("updated_at", "INTEGER NOT NULL DEFAULT 0"),
    ]
    for col_name, col_type in ai_auth_new_cols:
        if col_name not in ai_auth_cols:
            cur.execute(f"ALTER TABLE ai_auth_profiles ADD COLUMN {col_name} {col_type}")

    cur.execute("PRAGMA table_info(ai_provider_preferences)")
    ai_pref_cols = {r[1] for r in cur.fetchall()}
    for col_name, col_type in [
        ("interactive_provider_id", "TEXT NOT NULL DEFAULT 'openai-codex'"),
        ("batch_provider_id", "TEXT NOT NULL DEFAULT 'openai-codex'"),
        ("web_search_provider_id", "TEXT NOT NULL DEFAULT 'openai-codex'"),
        ("updated_at", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        if col_name not in ai_pref_cols:
            cur.execute(f"ALTER TABLE ai_provider_preferences ADD COLUMN {col_name} {col_type}")

    con.commit()
    con.close()
