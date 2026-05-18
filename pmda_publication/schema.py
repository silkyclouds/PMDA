"""PostgreSQL schema bootstrap for the published files library index."""

from __future__ import annotations

import logging


def init_files_pg_schema(
    *,
    schema_ready: bool,
    files_pg_connect,
    migrate_external_artist_images_norm_keys,
    backfill_artist_canonical_fields,
    backfill_artist_alias_table,
    merge_duplicate_person_artists,
    relink_external_artist_images_to_canonical_norm,
    purge_weak_classical_artist_images,
    logger=None,
) -> bool:
    logger = logger or logging
    if schema_ready:
        return True
    conn = files_pg_connect(autocommit=True)
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            except Exception as e:
                logger.debug("pg_trgm extension unavailable (continuing without it): %s", e)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_index_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_artists (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    name_norm TEXT NOT NULL UNIQUE,
                    canonical_name TEXT,
                    canonical_name_norm TEXT,
                    canonical_mbid TEXT,
                    entity_kind TEXT NOT NULL DEFAULT 'artist',
                    roles_json TEXT NOT NULL DEFAULT '[]',
                    aliases_json TEXT NOT NULL DEFAULT '[]',
                    album_count INTEGER NOT NULL DEFAULT 0,
                    track_count INTEGER NOT NULL DEFAULT 0,
                    broken_albums_count INTEGER NOT NULL DEFAULT 0,
                    has_image BOOLEAN NOT NULL DEFAULT FALSE,
                    image_path TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            for col_name, col_sql in [
                ("canonical_name", "TEXT"),
                ("canonical_name_norm", "TEXT"),
                ("canonical_mbid", "TEXT"),
                ("entity_kind", "TEXT NOT NULL DEFAULT 'artist'"),
                ("roles_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("aliases_json", "TEXT NOT NULL DEFAULT '[]'"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE files_artists ADD COLUMN IF NOT EXISTS {col_name} {col_sql}")
                except Exception:
                    pass
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artists_canonical_name_norm ON files_artists(canonical_name_norm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artists_canonical_mbid ON files_artists(canonical_mbid)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_albums (
                    id BIGSERIAL PRIMARY KEY,
                    artist_id BIGINT NOT NULL REFERENCES files_artists(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    title_norm TEXT NOT NULL,
                    folder_path TEXT NOT NULL UNIQUE,
                    year INTEGER,
                    date_text TEXT,
                    genre TEXT,
                    label TEXT,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    format TEXT,
                    is_lossless BOOLEAN NOT NULL DEFAULT FALSE,
                    sample_rate INTEGER,
                    bit_depth INTEGER,
                    has_cover BOOLEAN NOT NULL DEFAULT FALSE,
                    cover_path TEXT,
                    mb_identified BOOLEAN NOT NULL DEFAULT FALSE,
                    strict_match_verified BOOLEAN NOT NULL DEFAULT FALSE,
                    strict_match_provider TEXT,
                    strict_reject_reason TEXT,
                    strict_tracklist_score REAL NOT NULL DEFAULT 0.0,
                    musicbrainz_release_group_id TEXT,
                    musicbrainz_release_id TEXT,
                    discogs_release_id TEXT,
                    lastfm_album_mbid TEXT,
                    bandcamp_album_url TEXT,
                    metadata_source TEXT,
                    track_count INTEGER NOT NULL DEFAULT 0,
                    total_duration_sec INTEGER NOT NULL DEFAULT 0,
                    is_broken BOOLEAN NOT NULL DEFAULT FALSE,
                    expected_track_count INTEGER,
                    actual_track_count INTEGER,
                    missing_indices_json TEXT NOT NULL DEFAULT '[]',
                    missing_required_tags_json TEXT NOT NULL DEFAULT '[]',
                    primary_tags_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_artist_album_links (
                    artist_id BIGINT NOT NULL REFERENCES files_artists(id) ON DELETE CASCADE,
                    album_id BIGINT NOT NULL REFERENCES files_albums(id) ON DELETE CASCADE,
                    role TEXT NOT NULL DEFAULT 'artist',
                    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (artist_id, album_id, role)
                )
            """)
            # Backward-compatible schema evolution for files_albums.
            for col_name, col_sql in [
                ("label", "TEXT"),
                ("musicbrainz_release_id", "TEXT"),
                ("discogs_release_id", "TEXT"),
                ("lastfm_album_mbid", "TEXT"),
                ("bandcamp_album_url", "TEXT"),
                ("metadata_source", "TEXT"),
                ("sample_rate", "INTEGER"),
                ("bit_depth", "INTEGER"),
                ("strict_match_verified", "BOOLEAN NOT NULL DEFAULT FALSE"),
                ("strict_match_provider", "TEXT"),
                ("strict_reject_reason", "TEXT"),
                ("strict_tracklist_score", "REAL NOT NULL DEFAULT 0.0"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE files_albums ADD COLUMN IF NOT EXISTS {col_name} {col_sql}")
                except Exception:
                    pass
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_tracks (
                    id BIGSERIAL PRIMARY KEY,
                    album_id BIGINT NOT NULL REFERENCES files_albums(id) ON DELETE CASCADE,
                    file_path TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    disc_num INTEGER NOT NULL DEFAULT 1,
                    track_num INTEGER NOT NULL DEFAULT 0,
                    duration_sec INTEGER NOT NULL DEFAULT 0,
                    format TEXT,
                    bitrate INTEGER,
                    sample_rate INTEGER,
                    bit_depth INTEGER,
                    file_size_bytes BIGINT NOT NULL DEFAULT 0,
                    primary_tags_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            try:
                cur.execute("ALTER TABLE files_tracks ADD COLUMN IF NOT EXISTS primary_tags_json TEXT NOT NULL DEFAULT '{}'")
            except Exception:
                pass
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_track_embeddings (
                    track_id BIGINT PRIMARY KEY REFERENCES files_tracks(id) ON DELETE CASCADE,
                    embed_json TEXT NOT NULL,
                    norm REAL NOT NULL DEFAULT 1.0,
                    source TEXT NOT NULL DEFAULT 'pmda_hash_v1',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_reco_track_stats (
                    track_id BIGINT PRIMARY KEY REFERENCES files_tracks(id) ON DELETE CASCADE,
                    play_count BIGINT NOT NULL DEFAULT 0,
                    completion_count BIGINT NOT NULL DEFAULT 0,
                    partial_count BIGINT NOT NULL DEFAULT 0,
                    skip_count BIGINT NOT NULL DEFAULT 0,
                    last_event_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_reco_events (
                    id BIGSERIAL PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    track_id BIGINT NOT NULL REFERENCES files_tracks(id) ON DELETE CASCADE,
                    album_id BIGINT REFERENCES files_albums(id) ON DELETE CASCADE,
                    artist_id BIGINT REFERENCES files_artists(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    played_seconds INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # User listening telemetry (single-user install). This powers listening statistics charts.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_playback_events (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL DEFAULT 1,
                    track_id BIGINT NOT NULL REFERENCES files_tracks(id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    played_seconds INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_reco_sessions (
                    session_id TEXT PRIMARY KEY,
                    last_event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    total_events BIGINT NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_artist_profiles (
                    name_norm TEXT PRIMARY KEY,
                    artist_name TEXT NOT NULL,
                    bio TEXT,
                    short_bio TEXT,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    similar_json TEXT NOT NULL DEFAULT '[]',
                    source TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_album_profiles (
                    artist_norm TEXT NOT NULL,
                    title_norm TEXT NOT NULL,
                    album_title TEXT NOT NULL,
                    description TEXT,
                    short_description TEXT,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    public_rating DOUBLE PRECISION,
                    public_rating_votes INTEGER NOT NULL DEFAULT 0,
                    public_rating_source TEXT,
                    discogs_have_count INTEGER NOT NULL DEFAULT 0,
                    discogs_want_count INTEGER NOT NULL DEFAULT 0,
                    bandcamp_supporter_count INTEGER NOT NULL DEFAULT 0,
                    bandcamp_supporter_comments_json TEXT NOT NULL DEFAULT '[]',
                    lastfm_scrobbles BIGINT NOT NULL DEFAULT 0,
                    lastfm_listeners BIGINT NOT NULL DEFAULT 0,
                    heat_score DOUBLE PRECISION,
                    heat_label TEXT,
                    source TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (artist_norm, title_norm)
                )
            """)
            for col_name, col_sql in [
                ("public_rating", "DOUBLE PRECISION"),
                ("public_rating_votes", "INTEGER NOT NULL DEFAULT 0"),
                ("public_rating_source", "TEXT"),
                ("discogs_have_count", "INTEGER NOT NULL DEFAULT 0"),
                ("discogs_want_count", "INTEGER NOT NULL DEFAULT 0"),
                ("bandcamp_supporter_count", "INTEGER NOT NULL DEFAULT 0"),
                ("bandcamp_supporter_comments_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("lastfm_scrobbles", "BIGINT NOT NULL DEFAULT 0"),
                ("lastfm_listeners", "BIGINT NOT NULL DEFAULT 0"),
                ("heat_score", "DOUBLE PRECISION"),
                ("heat_label", "TEXT"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE files_album_profiles ADD COLUMN IF NOT EXISTS {col_name} {col_sql}")
                except Exception:
                    pass
            # Auth users live in SQLite settings.db, not in PostgreSQL. Keep the
            # local user_id as an opaque identifier here and only enforce the
            # album foreign key inside the files index.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_user_album_ratings (
                    user_id BIGINT NOT NULL,
                    album_id BIGINT NOT NULL REFERENCES files_albums(id) ON DELETE CASCADE,
                    rating SMALLINT NOT NULL,
                    review_text TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'ui',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, album_id)
                )
            """)
            try:
                cur.execute("ALTER TABLE files_user_album_ratings ADD COLUMN IF NOT EXISTS review_text TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
            # Cached artist images that do not live on disk next to audio files.
            # This allows us to persist external artwork across index rebuilds and
            # to show images for "similar artists" even when they are not in the
            # local library.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_external_artist_images (
                    name_norm TEXT PRIMARY KEY,
                    artist_name TEXT NOT NULL,
                    provider TEXT NOT NULL DEFAULT 'lastfm',
                    image_path TEXT,
                    image_url TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_external_artist_images_updated_at ON files_external_artist_images(updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_external_label_images (
                    label_norm TEXT PRIMARY KEY,
                    label_name TEXT NOT NULL,
                    provider TEXT NOT NULL DEFAULT 'bandcamp',
                    image_path TEXT,
                    image_url TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_external_label_images_updated_at ON files_external_label_images(updated_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_artist_aliases (
                    id BIGSERIAL PRIMARY KEY,
                    artist_id BIGINT NOT NULL REFERENCES files_artists(id) ON DELETE CASCADE,
                    artist_name_norm TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    alias_norm TEXT NOT NULL,
                    alias_signature TEXT NOT NULL DEFAULT '',
                    is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
                    source TEXT NOT NULL DEFAULT 'artist',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (artist_id, alias_norm)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_aliases_artist_id ON files_artist_aliases(artist_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_aliases_artist_name_norm ON files_artist_aliases(artist_name_norm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_aliases_alias_norm ON files_artist_aliases(alias_norm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_aliases_alias_signature ON files_artist_aliases(alias_signature)")
            # Manual match/rematch audit trail (album-level diagnostics shown in UI).
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS files_match_audit (
                    id BIGSERIAL PRIMARY KEY,
                    album_id BIGINT,
                    folder_path TEXT,
                    artist_name TEXT,
                    album_title TEXT,
                    run_kind TEXT NOT NULL DEFAULT 'manual',
                    status TEXT NOT NULL DEFAULT 'completed',
                    match_type TEXT,
                    confidence REAL,
                    ai_used BOOLEAN NOT NULL DEFAULT FALSE,
                    ai_confidence INTEGER,
                    provider_used TEXT,
                    summary TEXT,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_match_audit_album_created ON files_match_audit(album_id, created_at DESC)")
            try:
                cur.execute("ALTER TABLE files_match_audit ADD COLUMN IF NOT EXISTS folder_path TEXT")
            except Exception:
                pass
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_match_audit_folder_created ON files_match_audit(folder_path, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_match_audit_created ON files_match_audit(created_at DESC)")
            # ───────────────────── Playlists (Files mode) ─────────────────────
            # Lightweight local playlists stored in PostgreSQL so they are fast and
            # available to both UI and assistant logic. Items are ordered via `position`.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_playlists (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL DEFAULT 1,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("ALTER TABLE files_playlists ADD COLUMN IF NOT EXISTS user_id INTEGER NOT NULL DEFAULT 1")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_playlists_user_updated ON files_playlists(user_id, updated_at DESC, id DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_playlist_items (
                    id BIGSERIAL PRIMARY KEY,
                    playlist_id BIGINT NOT NULL REFERENCES files_playlists(id) ON DELETE CASCADE,
                    track_id BIGINT NOT NULL REFERENCES files_tracks(id) ON DELETE CASCADE,
                    position INTEGER NOT NULL DEFAULT 0,
                    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_playlist_items_playlist_pos ON files_playlist_items(playlist_id, position ASC, id ASC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_playlist_items_track_id ON files_playlist_items(track_id)")

            # ───────────────────── Likes / Favorites (Files mode) ─────────────────────
            # Persist user preferences (artist/album/track). Stored in PostgreSQL so the
            # assistant and recommendation engine can reuse the same preference signals.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_entity_likes (
                    entity_type TEXT NOT NULL,
                    entity_id BIGINT NOT NULL,
                    liked BOOLEAN NOT NULL DEFAULT TRUE,
                    source TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (entity_type, entity_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_entity_likes_updated_at ON files_entity_likes(updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_entity_likes_type_liked ON files_entity_likes(entity_type, liked)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_user_entity_likes (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id BIGINT NOT NULL DEFAULT 0,
                    entity_key TEXT NOT NULL DEFAULT '',
                    liked BOOLEAN NOT NULL DEFAULT TRUE,
                    source TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_files_user_entity_likes UNIQUE (user_id, entity_type, entity_id, entity_key)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_user_entity_likes_user_updated ON files_user_entity_likes(user_id, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_user_entity_likes_lookup ON files_user_entity_likes(user_id, entity_type, liked)")
            cur.execute(
                """
                INSERT INTO files_user_entity_likes(user_id, entity_type, entity_id, entity_key, liked, source, created_at, updated_at)
                SELECT
                    1,
                    entity_type,
                    entity_id,
                    '',
                    liked,
                    source,
                    created_at,
                    updated_at
                FROM files_entity_likes
                ON CONFLICT (user_id, entity_type, entity_id, entity_key) DO NOTHING
                """
            )
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_social_recommendations (
                    id BIGSERIAL PRIMARY KEY,
                    sender_user_id INTEGER NOT NULL,
                    sender_username TEXT NOT NULL DEFAULT '',
                    recipient_user_id INTEGER NOT NULL,
                    recipient_username TEXT NOT NULL DEFAULT '',
                    entity_type TEXT NOT NULL,
                    entity_id BIGINT NOT NULL DEFAULT 0,
                    entity_key TEXT NOT NULL DEFAULT '',
                    entity_label TEXT NOT NULL DEFAULT '',
                    entity_subtitle TEXT NOT NULL DEFAULT '',
                    entity_href TEXT NOT NULL DEFAULT '',
                    entity_thumb TEXT NOT NULL DEFAULT '',
                    entity_meta_json TEXT NOT NULL DEFAULT '{}',
                    message TEXT,
                    parent_recommendation_id BIGINT NULL REFERENCES files_social_recommendations(id) ON DELETE SET NULL,
                    liked_by_recipient BOOLEAN NOT NULL DEFAULT FALSE,
                    liked_at TIMESTAMPTZ NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    read_at TIMESTAMPTZ NULL,
                    status TEXT NOT NULL DEFAULT 'sent'
                )
            """)
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS sender_username TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS recipient_user_id INTEGER NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS recipient_username TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS entity_subtitle TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS entity_href TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS entity_thumb TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS entity_meta_json TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS parent_recommendation_id BIGINT NULL")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS liked_by_recipient BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS liked_at TIMESTAMPTZ NULL")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ NULL")
            cur.execute("ALTER TABLE files_social_recommendations ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'sent'")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_social_recommendations_recipient_created ON files_social_recommendations(recipient_user_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_social_recommendations_sender_created ON files_social_recommendations(sender_user_id, created_at DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_user_notifications (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    actor_user_id INTEGER NULL,
                    actor_username TEXT NOT NULL DEFAULT '',
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL DEFAULT '',
                    entity_type TEXT NOT NULL DEFAULT '',
                    entity_id BIGINT NOT NULL DEFAULT 0,
                    entity_key TEXT NOT NULL DEFAULT '',
                    recommendation_id BIGINT NULL REFERENCES files_social_recommendations(id) ON DELETE SET NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    is_read BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    read_at TIMESTAMPTZ NULL
                )
            """)
            cur.execute("ALTER TABLE files_user_notifications ADD COLUMN IF NOT EXISTS actor_user_id INTEGER NULL")
            cur.execute("ALTER TABLE files_user_notifications ADD COLUMN IF NOT EXISTS actor_username TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE files_user_notifications ADD COLUMN IF NOT EXISTS recommendation_id BIGINT NULL")
            cur.execute("ALTER TABLE files_user_notifications ADD COLUMN IF NOT EXISTS payload_json TEXT NOT NULL DEFAULT '{}'")
            cur.execute("ALTER TABLE files_user_notifications ADD COLUMN IF NOT EXISTS is_read BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE files_user_notifications ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ NULL")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_user_notifications_user_created ON files_user_notifications(user_id, is_read, created_at DESC)")

            # ───────────────────── Concert Cache (Files mode) ─────────────────────
            # Cached upcoming concerts for artist pages. Providers may be added later
            # (Songkick, etc.). We keep the raw-ish provider payload as JSON for
            # forward compatibility.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS files_artist_concerts (
                    artist_id BIGINT PRIMARY KEY REFERENCES files_artists(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL DEFAULT 'bandsintown',
                    events_json TEXT NOT NULL DEFAULT '[]',
                    source_url TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_concerts_updated_at ON files_artist_concerts(updated_at DESC)")
            # ───────────────────── Assistant (RAG + Chat Traces) ─────────────────────
            # Stored in PostgreSQL so "what the AI learned" is inspectable, attributable,
            # and can be garbage-collected safely without losing core library data.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS assistant_sessions (
                    session_id TEXT PRIMARY KEY,
                    title TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS assistant_messages (
                    id BIGSERIAL PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES assistant_sessions(session_id) ON DELETE CASCADE,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    context_json TEXT NOT NULL DEFAULT '{}',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS assistant_docs (
                    id BIGSERIAL PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    entity_id BIGINT NOT NULL,
                    doc_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    provider TEXT,
                    model TEXT,
                    title TEXT,
                    url TEXT,
                    lang TEXT,
                    content TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (entity_type, entity_id, doc_type, source)
                )
            """)
            # Backward-compatible schema evolution for assistant_docs.
            for col_name, col_sql in [
                ("provider", "TEXT"),
                ("model", "TEXT"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE assistant_docs ADD COLUMN IF NOT EXISTS {col_name} {col_sql}")
                except Exception:
                    pass
            cur.execute("""
                CREATE TABLE IF NOT EXISTS assistant_doc_chunks (
                    id BIGSERIAL PRIMARY KEY,
                    doc_id BIGINT NOT NULL REFERENCES assistant_docs(id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    embed_json TEXT NOT NULL DEFAULT '[]',
                    norm REAL NOT NULL DEFAULT 1.0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (doc_id, chunk_index)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS assistant_entity_facts (
                    entity_type TEXT NOT NULL,
                    entity_id BIGINT NOT NULL,
                    facts_json TEXT NOT NULL DEFAULT '{}',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    source TEXT NOT NULL DEFAULT 'ai_extracted_v1',
                    provider TEXT,
                    model TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (entity_type, entity_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artists_name ON files_artists(name)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artists_entity_kind ON files_artists(entity_kind)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_artist_id ON files_albums(artist_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_album_links_artist ON files_artist_album_links(artist_id, role, album_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_album_links_album ON files_artist_album_links(album_id, artist_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_title ON files_albums(title)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_title_norm ON files_albums(title_norm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_genre ON files_albums(genre)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_label ON files_albums(label)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_recent ON files_albums(created_at DESC, id DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_updated_recent ON files_albums(updated_at DESC, id DESC)")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_files_albums_visible_recent
                ON files_albums(created_at DESC, id DESC)
                WHERE COALESCE(is_broken, FALSE) = FALSE
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_files_albums_visible_artist_recent
                ON files_albums(artist_id, created_at DESC, id DESC)
                WHERE COALESCE(is_broken, FALSE) = FALSE
                """
            )
            matched_identity_expr = "(COALESCE(strict_match_verified, FALSE) = TRUE OR COALESCE(mb_identified, FALSE) = TRUE)"
            # Partial indexes for "matched-only" library mode at large scale.
            # These avoid full scans when filtering strictly on provider IDs.
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_files_albums_matched_recent
                ON files_albums(created_at DESC, id DESC)
                WHERE {matched_identity_expr}
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_files_albums_matched_artist
                ON files_albums(artist_id, id DESC)
                WHERE {matched_identity_expr}
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_files_albums_matched_year
                ON files_albums(year DESC, id DESC)
                WHERE {matched_identity_expr} AND year IS NOT NULL
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_files_albums_matched_label
                ON files_albums(label)
                WHERE {matched_identity_expr} AND COALESCE(TRIM(label), '') <> ''
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_tracks_album_id ON files_tracks(album_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_tracks_title ON files_tracks(title)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_tracks_order ON files_tracks(album_id, disc_num, track_num, id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_track_embeddings_updated_at ON files_track_embeddings(updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_reco_events_session_time ON files_reco_events(session_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_reco_events_track_time ON files_reco_events(track_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_playback_events_time ON files_playback_events(created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_playback_events_track_time ON files_playback_events(track_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_reco_track_stats_score ON files_reco_track_stats(play_count DESC, completion_count DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_reco_sessions_last_event ON files_reco_sessions(last_event_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_profiles_updated_at ON files_artist_profiles(updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_album_profiles_updated_at ON files_album_profiles(updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_album_profiles_heat_score ON files_album_profiles(heat_score DESC NULLS LAST, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_album_profiles_public_rating ON files_album_profiles(public_rating DESC NULLS LAST, public_rating_votes DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_user_album_ratings_album_id ON files_user_album_ratings(album_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_files_user_album_ratings_user_updated ON files_user_album_ratings(user_id, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_assistant_messages_session_time ON assistant_messages(session_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_assistant_messages_created_at ON assistant_messages(created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_assistant_docs_entity ON assistant_docs(entity_type, entity_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_assistant_doc_chunks_doc_id ON assistant_doc_chunks(doc_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_assistant_entity_facts_updated_at ON assistant_entity_facts(updated_at DESC)")
            try:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artists_name_trgm ON files_artists USING gin (name gin_trgm_ops)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artists_canonical_name_trgm ON files_artists USING gin (canonical_name gin_trgm_ops)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_artist_aliases_alias_trgm ON files_artist_aliases USING gin (alias gin_trgm_ops)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_albums_title_trgm ON files_albums USING gin (title gin_trgm_ops)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_files_tracks_title_trgm ON files_tracks USING gin (title gin_trgm_ops)")
            except Exception:
                pass
            # Migrate legacy external-artist-image keys to the same strict normalization used by files_artists.name_norm.
            # This keeps joins fast and prevents repeated re-downloads when the library contains punctuation/&/unicode.
            try:
                migrate_external_artist_images_norm_keys(cur)
            except Exception:
                pass
            try:
                backfill_artist_canonical_fields(conn)
            except Exception:
                logger.debug("Artist canonical field backfill failed", exc_info=True)
            try:
                backfill_artist_alias_table(conn)
            except Exception:
                logger.debug("Artist alias table backfill failed", exc_info=True)
            try:
                merge_duplicate_person_artists(conn)
            except Exception:
                logger.debug("Artist person merge backfill failed", exc_info=True)
            try:
                relink_external_artist_images_to_canonical_norm(conn)
            except Exception:
                logger.debug("External artist image relink backfill failed", exc_info=True)
            try:
                purge_weak_classical_artist_images(conn)
            except Exception:
                logger.debug("Artist image policy backfill failed", exc_info=True)
        return True
    except Exception as e:
        logger.exception("Failed to initialize files PG schema: %s", e)
        return False
    finally:
        conn.close()
