"""Profile enrichment support helpers extracted from pmda.py.

This module owns cache/profile persistence helpers, profile backfill probes,
artist image relinking, and disk-aware profile scope guards. Public PMDA
wrappers bind the live runtime before each call so existing monkeypatch-based
unit tests and cross-module dependencies keep working during the larger
monolith split.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None

_IMPL_NAMES = {
    "_dt_to_epoch_impl",
    "_is_profile_stale_impl",
    "_profile_title_norm_variants_impl",
    "_files_get_artist_profile_cached_impl",
    "_files_get_album_profiles_cached_impl",
    "_files_upsert_artist_profile_impl",
    "_files_upsert_album_profile_impl",
    "_album_profile_has_payload_impl",
    "_album_profile_has_text_impl",
    "_files_album_profile_fetch_allowed_impl",
    "_files_album_profile_fetch_strength_impl",
    "_files_album_cover_refresh_allowed_impl",
    "_files_build_local_artist_profile_impl",
    "_files_ensure_local_artist_profile_impl",
    "_files_enrich_artists_blocking_impl",
    "_enqueue_files_profile_enrichment_impl",
    "_files_profile_job_is_active_impl",
    "_files_profile_enrichment_priority_flags_impl",
    "_files_profile_backfill_stage_specs_impl",
    "_files_profile_backfill_pending_work_impl",
    "_files_profile_backfill_maybe_start_idle_impl",
    "_trigger_files_profile_backfill_async_impl",
    "_files_relink_external_artist_images_for_artist_impl",
    "_storage_profile_backfill_scope_impl",
    "_storage_profile_backfill_scope_signature_impl",
    "_storage_profile_enrichment_scope_for_artist_impl",
}


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals while preserving this module's implementations."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = set(_IMPL_NAMES) | {"_bind_runtime", "_RUNTIME", "_IMPL_NAMES"}
    globals().update(
        {
            key: value
            for key, value in vars(runtime).items()
            if key not in blocked and not (key.startswith("__") and key.endswith("__"))
        }
    )

def _dt_to_epoch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dt_to_epoch_impl(*args, **kwargs)


def _is_profile_stale_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _is_profile_stale_impl(*args, **kwargs)


def _profile_title_norm_variants_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _profile_title_norm_variants_impl(*args, **kwargs)


def _files_get_artist_profile_cached_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_get_artist_profile_cached_impl(*args, **kwargs)


def _files_get_album_profiles_cached_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_get_album_profiles_cached_impl(*args, **kwargs)


def _files_upsert_artist_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_upsert_artist_profile_impl(*args, **kwargs)


def _files_upsert_album_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_upsert_album_profile_impl(*args, **kwargs)


def _album_profile_has_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _album_profile_has_payload_impl(*args, **kwargs)


def _album_profile_has_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _album_profile_has_text_impl(*args, **kwargs)


def _files_album_profile_fetch_allowed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_album_profile_fetch_allowed_impl(*args, **kwargs)


def _files_album_profile_fetch_strength_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_album_profile_fetch_strength_impl(*args, **kwargs)


def _files_album_cover_refresh_allowed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_album_cover_refresh_allowed_impl(*args, **kwargs)


def _files_build_local_artist_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_build_local_artist_profile_impl(*args, **kwargs)


def _files_ensure_local_artist_profile_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_ensure_local_artist_profile_impl(*args, **kwargs)


def _files_enrich_artists_blocking_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_enrich_artists_blocking_impl(*args, **kwargs)


def _enqueue_files_profile_enrichment_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _enqueue_files_profile_enrichment_impl(*args, **kwargs)


def _files_profile_job_is_active_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_profile_job_is_active_impl(*args, **kwargs)


def _files_profile_enrichment_priority_flags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_profile_enrichment_priority_flags_impl(*args, **kwargs)


def _files_profile_backfill_stage_specs_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_profile_backfill_stage_specs_impl(*args, **kwargs)


def _files_profile_backfill_pending_work_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_profile_backfill_pending_work_impl(*args, **kwargs)


def _files_profile_backfill_maybe_start_idle_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_profile_backfill_maybe_start_idle_impl(*args, **kwargs)


def _trigger_files_profile_backfill_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _trigger_files_profile_backfill_async_impl(*args, **kwargs)


def _files_relink_external_artist_images_for_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_relink_external_artist_images_for_artist_impl(*args, **kwargs)


def _storage_profile_backfill_scope_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _storage_profile_backfill_scope_impl(*args, **kwargs)


def _storage_profile_backfill_scope_signature_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _storage_profile_backfill_scope_signature_impl(*args, **kwargs)


def _storage_profile_enrichment_scope_for_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _storage_profile_enrichment_scope_for_artist_impl(*args, **kwargs)


def _dt_to_epoch_impl(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if hasattr(value, "timestamp"):
        try:
            return float(value.timestamp())
        except Exception:
            return 0.0
    return 0.0


def _is_profile_stale_impl(updated_at) -> bool:
    ts = _dt_to_epoch(updated_at)
    if ts <= 0:
        return True
    return (time.time() - ts) > _FILES_PROFILE_MAX_AGE_SEC


def _profile_title_norm_variants_impl(*values: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    def _add(raw: str) -> None:
        text = str(raw or "").strip()
        if not text or text in seen:
            return
        seen.add(text)
        out.append(text)

    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        _add(text)
        _add(norm_album(text))
        _add(norm_album_for_dedup(text, normalize_parenthetical=True))
        if "…" in text:
            _add(text.replace("…", "..."))
        if "..." in text:
            _add(text.replace("...", "…"))
    return out


def _files_get_artist_profile_cached_impl(artist_name: str, artist_norm: str) -> dict:
    if not artist_norm:
        return {}
    cache_key = f"library:artist_profile:{artist_norm}"
    cached = _files_cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached
    conn = _files_pg_connect()
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bio, short_bio, tags_json, similar_json, source, updated_at
                FROM files_artist_profiles
                WHERE name_norm = %s
                """,
                (artist_norm,),
            )
            row = cur.fetchone()
        if not row:
            return {}
        try:
            tags = json.loads(row[2] or "[]") if row[2] else []
        except Exception:
            tags = []
        try:
            similar = json.loads(row[3] or "[]") if row[3] else []
        except Exception:
            similar = []
        payload = {
            "artist_name": artist_name,
            "bio": row[0] or "",
            "short_bio": row[1] or "",
            "tags": tags if isinstance(tags, list) else [],
            "similar_artists": similar if isinstance(similar, list) else [],
            "source": row[4] or "",
            "updated_at": int(_dt_to_epoch(row[5])) if row[5] else 0,
            "stale": _is_profile_stale(row[5]),
        }
        _files_cache_set_json(cache_key, payload, ttl=1800)
        return payload
    except Exception:
        return {}
    finally:
        conn.close()


def _files_get_album_profiles_cached_impl(artist_norm: str, title_norms: list[str]) -> dict[str, dict]:
    if not artist_norm or not title_norms:
        return {}
    expanded_title_norms: list[str] = []
    for raw_norm in (title_norms or []):
        expanded_title_norms.extend(_profile_title_norm_variants(raw_norm))
    title_norms_clean = list(dict.fromkeys([t for t in expanded_title_norms if t]))
    if not title_norms_clean:
        return {}
    cache_key = f"library:album_profiles:{artist_norm}:{hashlib.sha1('|'.join(sorted(set(title_norms_clean))).encode('utf-8', errors='ignore')).hexdigest()}"
    cached = _files_cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached
    conn = _files_pg_connect()
    if conn is None:
        return {}
    try:
        placeholders = ",".join(["%s"] * len(title_norms_clean))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    title_norm,
                    description,
                    short_description,
                    tags_json,
                    source,
                    updated_at,
                    public_rating,
                    public_rating_votes,
                    public_rating_source,
                    discogs_have_count,
                    discogs_want_count,
                    bandcamp_supporter_count,
                    bandcamp_supporter_comments_json,
                    lastfm_scrobbles,
                    lastfm_listeners,
                    heat_score,
                    heat_label
                FROM files_album_profiles
                WHERE artist_norm = %s AND title_norm IN ({placeholders})
                """,
                [artist_norm, *title_norms_clean],
            )
            rows = cur.fetchall()
        out: dict[str, dict] = {}
        for row in rows:
            try:
                tags = json.loads(row[3] or "[]") if row[3] else []
            except Exception:
                tags = []
            try:
                bandcamp_supporter_comments = json.loads(row[12] or "[]") if row[12] else []
            except Exception:
                bandcamp_supporter_comments = []
            out[str(row[0] or "")] = {
                "description": row[1] or "",
                "short_description": row[2] or "",
                "tags": tags if isinstance(tags, list) else [],
                "source": row[4] or "",
                "updated_at": int(_dt_to_epoch(row[5])) if row[5] else 0,
                "stale": _is_profile_stale(row[5]),
                "public_rating": float(row[6]) if row[6] is not None else None,
                "public_rating_votes": int(row[7] or 0),
                "public_rating_source": str(row[8] or "").strip() or None,
                "discogs_have_count": int(row[9] or 0),
                "discogs_want_count": int(row[10] or 0),
                "bandcamp_supporter_count": int(row[11] or 0),
                "bandcamp_supporter_comments": bandcamp_supporter_comments if isinstance(bandcamp_supporter_comments, list) else [],
                "lastfm_scrobbles": int(row[13] or 0),
                "lastfm_listeners": int(row[14] or 0),
                "heat_score": float(row[15]) if row[15] is not None else None,
                "heat_label": str(row[16] or "").strip() or None,
            }
            for alt_key in _profile_title_norm_variants(str(row[0] or "")):
                if alt_key not in out:
                    out[alt_key] = out[str(row[0] or "")]
        _files_cache_set_json(cache_key, out, ttl=1800)
        return out
    except Exception:
        return {}
    finally:
        conn.close()


def _files_upsert_artist_profile_impl(conn, artist_norm: str, artist_name: str, profile: dict) -> None:
    tags_json = json.dumps((profile or {}).get("tags") or [])
    similar_json = json.dumps((profile or {}).get("similar") or (profile or {}).get("similar_artists") or [])
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO files_artist_profiles(name_norm, artist_name, bio, short_bio, tags_json, similar_json, source, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (name_norm) DO UPDATE
            SET artist_name = EXCLUDED.artist_name,
                bio = EXCLUDED.bio,
                short_bio = EXCLUDED.short_bio,
                tags_json = EXCLUDED.tags_json,
                similar_json = EXCLUDED.similar_json,
                source = EXCLUDED.source,
                updated_at = NOW()
            """,
            (
                artist_norm,
                artist_name or "",
                (profile or {}).get("bio") or "",
                (profile or {}).get("short_bio") or "",
                tags_json,
                similar_json,
                (profile or {}).get("source") or "",
            ),
        )


def _files_upsert_album_profile_impl(conn, artist_norm: str, title_norm: str, album_title: str, profile: dict) -> None:
    tags_json = json.dumps((profile or {}).get("tags") or [])
    description = str((profile or {}).get("description") or "").strip()
    short_description = str((profile or {}).get("short_description") or "").strip()
    source = str((profile or {}).get("source") or "").strip()
    bandcamp_supporter_comments_json = json.dumps(_normalize_bandcamp_supporter_comments((profile or {}).get("bandcamp_supporter_comments")))
    public_rating_raw = (profile or {}).get("public_rating")
    try:
        public_rating = float(public_rating_raw) if public_rating_raw is not None else None
    except Exception:
        public_rating = None
    if public_rating is not None:
        public_rating = max(0.0, min(5.0, public_rating))
    try:
        public_rating_votes = max(0, int((profile or {}).get("public_rating_votes") or 0))
    except Exception:
        public_rating_votes = 0
    try:
        discogs_have_count = max(0, int((profile or {}).get("discogs_have_count") or 0))
    except Exception:
        discogs_have_count = 0
    try:
        discogs_want_count = max(0, int((profile or {}).get("discogs_want_count") or 0))
    except Exception:
        discogs_want_count = 0
    try:
        bandcamp_supporter_count = max(0, int((profile or {}).get("bandcamp_supporter_count") or 0))
    except Exception:
        bandcamp_supporter_count = 0
    try:
        lastfm_scrobbles = max(0, int((profile or {}).get("lastfm_scrobbles") or 0))
    except Exception:
        lastfm_scrobbles = 0
    try:
        lastfm_listeners = max(0, int((profile or {}).get("lastfm_listeners") or 0))
    except Exception:
        lastfm_listeners = 0
    heat_score_raw = (profile or {}).get("heat_score")
    try:
        heat_score = float(heat_score_raw) if heat_score_raw is not None else None
    except Exception:
        heat_score = None
    if heat_score is not None:
        heat_score = max(0.0, min(100.0, heat_score))
    public_rating_source = str((profile or {}).get("public_rating_source") or "").strip()
    heat_label = str((profile or {}).get("heat_label") or "").strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO files_album_profiles(
                artist_norm,
                title_norm,
                album_title,
                description,
                short_description,
                tags_json,
                public_rating,
                public_rating_votes,
                public_rating_source,
                discogs_have_count,
                discogs_want_count,
                bandcamp_supporter_count,
                bandcamp_supporter_comments_json,
                lastfm_scrobbles,
                lastfm_listeners,
                heat_score,
                heat_label,
                source,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (artist_norm, title_norm) DO UPDATE
            SET album_title = EXCLUDED.album_title,
                description = CASE
                    WHEN BTRIM(EXCLUDED.description) <> '' THEN EXCLUDED.description
                    ELSE files_album_profiles.description
                END,
                short_description = CASE
                    WHEN BTRIM(EXCLUDED.short_description) <> '' THEN EXCLUDED.short_description
                    ELSE files_album_profiles.short_description
                END,
                tags_json = EXCLUDED.tags_json,
                public_rating = EXCLUDED.public_rating,
                public_rating_votes = EXCLUDED.public_rating_votes,
                public_rating_source = EXCLUDED.public_rating_source,
                discogs_have_count = EXCLUDED.discogs_have_count,
                discogs_want_count = EXCLUDED.discogs_want_count,
                bandcamp_supporter_count = EXCLUDED.bandcamp_supporter_count,
                bandcamp_supporter_comments_json = CASE
                    WHEN EXCLUDED.bandcamp_supporter_comments_json IS NOT NULL
                         AND BTRIM(EXCLUDED.bandcamp_supporter_comments_json) <> ''
                         AND EXCLUDED.bandcamp_supporter_comments_json <> '[]'
                    THEN EXCLUDED.bandcamp_supporter_comments_json
                    ELSE files_album_profiles.bandcamp_supporter_comments_json
                END,
                lastfm_scrobbles = EXCLUDED.lastfm_scrobbles,
                lastfm_listeners = EXCLUDED.lastfm_listeners,
                heat_score = EXCLUDED.heat_score,
                heat_label = EXCLUDED.heat_label,
                source = CASE
                    WHEN BTRIM(EXCLUDED.source) <> '' THEN EXCLUDED.source
                    ELSE files_album_profiles.source
                END,
                updated_at = NOW()
            """,
            (
                artist_norm,
                title_norm,
                album_title or "",
                description,
                short_description,
                tags_json,
                public_rating,
                public_rating_votes,
                public_rating_source or None,
                discogs_have_count,
                discogs_want_count,
                bandcamp_supporter_count,
                bandcamp_supporter_comments_json,
                lastfm_scrobbles,
                lastfm_listeners,
                heat_score,
                heat_label or None,
                source,
            ),
        )


def _album_profile_has_payload_impl(profile: dict | None) -> bool:
    if not isinstance(profile, dict):
        return False
    if str(profile.get("description") or "").strip():
        return True
    if str(profile.get("short_description") or "").strip():
        return True
    tags = profile.get("tags") or []
    if isinstance(tags, list) and any(str(t or "").strip() for t in tags):
        return True
    bandcamp_supporter_comments = profile.get("bandcamp_supporter_comments") or []
    if isinstance(bandcamp_supporter_comments, list) and any(
        str((item or {}).get("text") or "").strip()
        for item in bandcamp_supporter_comments
        if isinstance(item, dict)
    ):
        return True
    for key in (
        "public_rating",
        "public_rating_votes",
        "discogs_have_count",
        "discogs_want_count",
        "bandcamp_supporter_count",
        "lastfm_scrobbles",
        "lastfm_listeners",
        "heat_score",
    ):
        try:
            if float(profile.get(key) or 0) > 0:
                return True
        except Exception:
            continue
    return False


def _album_profile_has_text_impl(profile: dict | None) -> bool:
    if not isinstance(profile, dict):
        return False
    if str(profile.get("description") or "").strip():
        return True
    if str(profile.get("short_description") or "").strip():
        return True
    bandcamp_supporter_comments = profile.get("bandcamp_supporter_comments") or []
    if isinstance(bandcamp_supporter_comments, list) and any(
        str((item or {}).get("text") or "").strip()
        for item in bandcamp_supporter_comments
        if isinstance(item, dict)
    ):
        return True
    return False


def _files_album_profile_fetch_allowed_impl(
    *,
    strict_verified: bool = False,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
) -> bool:
    if bool(strict_verified):
        return True
    provider_hint = _normalize_identity_provider(str(metadata_source or "").strip())
    has_provider_hint = bool(
        provider_hint in {"musicbrainz", "discogs", "lastfm", "bandcamp"}
        or str(mbid or "").strip()
        or str(discogs_release_id or "").strip()
        or str(lastfm_album_mbid or "").strip()
        or str(bandcamp_album_url or "").strip()
    )
    return bool(has_provider_hint)


def _files_album_profile_fetch_strength_impl(
    *,
    strict_verified: bool = False,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
) -> int:
    return _enrichment_profiles.album_profile_fetch_strength(
        strict_verified=strict_verified,
        metadata_source=metadata_source,
        mbid=mbid,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    )


def _files_album_cover_refresh_allowed_impl(
    *,
    strict_verified: bool = False,
    metadata_source: str = "",
    mbid: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
) -> bool:
    if bool(strict_verified):
        return True
    provider_hint = _normalize_identity_provider(str(metadata_source or "").strip())
    return bool(
        provider_hint in {"musicbrainz", "discogs", "lastfm", "bandcamp", "itunes", "deezer", "spotify", "qobuz", "tidal", "audiodb"}
        or str(mbid or "").strip()
        or str(discogs_release_id or "").strip()
        or str(lastfm_album_mbid or "").strip()
        or str(bandcamp_album_url or "").strip()
    )


def _files_build_local_artist_profile_impl(
    conn,
    *,
    artist_id: int,
    artist_name: str,
    artist_norm: str,
    entity_kind: str = "artist",
    roles_json: str = "[]",
) -> dict[str, Any]:
    roles = _safe_json_load(roles_json or "[]", fallback=[])
    if not isinstance(roles, list):
        roles = []
    role_set = [str(role or "").strip().lower() for role in roles if str(role or "").strip()]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                alb.id,
                COALESCE(alb.title, ''),
                COALESCE(alb.year, 0),
                COALESCE(alb.label, ''),
                COALESCE(alb.genre, ''),
                COALESCE(alb.tags_json, '[]')
            FROM files_artist_album_links link
            JOIN files_albums alb ON alb.id = link.album_id
            WHERE link.artist_id = %s
            GROUP BY alb.id, alb.title, alb.year, alb.label, alb.genre, alb.tags_json
            ORDER BY COALESCE(alb.year, 0) DESC, alb.title ASC
            LIMIT 18
            """,
            (int(artist_id),),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT COUNT(DISTINCT album_id) FROM files_artist_album_links WHERE artist_id = %s",
            (int(artist_id),),
        )
        album_count = int((cur.fetchone() or [0])[0] or 0)
    years = [int(r[2] or 0) for r in rows if int(r[2] or 0) > 0]
    top_titles = [str(r[1] or "").strip() for r in rows if str(r[1] or "").strip()][:3]
    label_counts: dict[str, int] = {}
    genre_counts: dict[str, int] = {}
    for _album_id, _title, _year, label, genre, tags_json in rows:
        label_clean = re.sub(r"\s+", " ", str(label or "").strip())
        if label_clean:
            label_counts[label_clean] = label_counts.get(label_clean, 0) + 1
        try:
            tags = json.loads(tags_json or "[]") if tags_json else []
        except Exception:
            tags = []
        values: list[str] = []
        if isinstance(tags, list):
            values.extend([str(v or "").strip() for v in tags if str(v or "").strip()])
        if not values and str(genre or "").strip():
            values.extend(_split_genre_values(str(genre or "")))
        for value in values:
            clean = re.sub(r"\s+", " ", str(value or "").strip())
            if clean:
                genre_counts[clean] = genre_counts.get(clean, 0) + 1
    top_labels = [name for name, _ in sorted(label_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:3]]
    top_genres = [name for name, _ in sorted(genre_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:5]]
    appearance_only = bool(role_set) and not any(
        role in {"artist", "composer", "conductor", "orchestra", "ensemble", "soloist", "performer"}
        for role in role_set
    )
    if appearance_only and any(role in {"featured", "appearance"} for role in role_set):
        role_phrase = "appears on releases in your library"
    else:
        role_phrase = {
            "composer": "appears in your library as a composer",
            "conductor": "appears in your library as a conductor",
            "orchestra": "appears in your library as an orchestra",
            "ensemble": "appears in your library as an ensemble",
            "soloist": "appears in your library as a soloist",
            "performer": "appears in your library as a performer",
            "artist": "appears in your library as an artist",
        }.get(entity_kind or "artist", "appears in your library")
    year_phrase = ""
    if years:
        min_year = min(years)
        max_year = max(years)
        year_phrase = f" spanning {min_year}" if min_year == max_year else f" spanning {min_year} to {max_year}"
    title_phrase = ""
    if top_titles:
        if len(top_titles) == 1:
            title_phrase = f" Notable recording: {top_titles[0]}."
        else:
            title_phrase = " Notable recordings: " + ", ".join(top_titles[:3]) + "."
    label_phrase = f" Common labels here: {', '.join(top_labels)}." if top_labels else ""
    genre_phrase = f" Library cues: {', '.join(top_genres)}." if top_genres else ""
    role_tags = [role.replace("_", " ") for role in role_set if role]
    bio = (
        f"{artist_name} {role_phrase} on {album_count} album(s){year_phrase}."
        f"{' Roles seen: ' + ', '.join(role_tags[:4]) + '.' if role_tags else ''}"
        f"{title_phrase}{label_phrase}{genre_phrase}"
    ).strip()
    short_bio = f"{artist_name} {role_phrase} on {album_count} album(s).".strip()
    return {
        "bio": bio,
        "short_bio": short_bio,
        "tags": top_genres[:8],
        "similar": [],
        "source": "pmda-local",
    }


def _files_ensure_local_artist_profile_impl(conn, *, artist_id: int, artist_name: str, artist_norm: str, entity_kind: str = "artist", roles_json: str = "[]") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(bio, ''), COALESCE(short_bio, '')
            FROM files_artist_profiles
            WHERE name_norm = %s
            LIMIT 1
            """,
            (artist_norm,),
        )
        row = cur.fetchone()
    bio = str((row[0] if row else "") or "").strip()
    short_bio = str((row[1] if row else "") or "").strip()
    if not (_is_garbage_bio(bio) and _is_garbage_bio(short_bio)):
        return
    fallback = _files_build_local_artist_profile(
        conn,
        artist_id=int(artist_id or 0),
        artist_name=artist_name,
        artist_norm=artist_norm,
        entity_kind=entity_kind,
        roles_json=roles_json,
    )
    if _word_count(str(fallback.get("bio") or "")) > 4:
        _files_upsert_artist_profile(conn, artist_norm, artist_name, fallback)


def _files_enrich_artists_blocking_impl(
    artists_map: dict[str, dict[str, Any]],
    *,
    target_artist_norms: Optional[set[str]] = None,
    max_artists: int | None = None,
    total_budget_sec: float | None = None,
) -> None:
    if not artists_map:
        return
    target_norms = {str(v or "").strip() for v in (target_artist_norms or set()) if str(v or "").strip()}
    started_at = time.time()
    processed = 0
    items = list(artists_map.items())
    if target_norms:
        items.sort(key=lambda item: (0 if item[0] in target_norms else 1, item[0]))
        items = [item for item in items if item[0] in target_norms]
    total_items = len(items)
    for idx, (artist_norm, payload) in enumerate(items, start=1):
        if target_norms and artist_norm not in target_norms:
            continue
        if max_artists is not None and processed >= max_artists:
            break
        if total_budget_sec is not None and (time.time() - started_at) >= float(total_budget_sec):
            break
        name = str((payload or {}).get("name") or "").strip()
        if not artist_norm or not name:
            continue
        try:
            ratio_progress, eta_seconds, rate_per_sec = _files_index_progress_metrics(
                processed,
                total_items,
                started_at=started_at,
            )
            phase_progress = 96.0
            if ratio_progress is not None:
                phase_progress = min(98.9, 96.0 + ((float(ratio_progress) / 100.0) * 2.9))
            _files_index_set_state(
                phase="artist_enrichment",
                phase_message="Enriching artist pages",
                current_artist=name,
                current_folder=None,
                phase_item_done=int(processed),
                phase_item_total=int(total_items),
                phase_item_label="artist pages",
                phase_progress=phase_progress,
                phase_eta_seconds=eta_seconds,
                phase_rate_per_sec=rate_per_sec,
            )
            conn = _files_pg_connect()
            if conn is None:
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT a.id, COALESCE(a.entity_kind, 'artist'), COALESCE(a.roles_json, '[]'), COALESCE(prof.bio, ''), COALESCE(prof.short_bio, ''), COALESCE(ext.image_path, '')
                        FROM files_artists a
                        LEFT JOIN files_artist_profiles prof ON prof.name_norm = a.name_norm
                        LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                        WHERE a.name_norm = %s
                        LIMIT 1
                        """,
                        (artist_norm,),
                    )
                    prow = cur.fetchone()
                artist_id = int(prow[0] or 0) if prow else 0
                entity_kind = str((prow[1] if prow else "artist") or "artist")
                roles_json = str((prow[2] if prow else "[]") or "[]")
                bio = str((prow[3] if prow else "") or "").strip()
                short_bio = str((prow[4] if prow else "") or "").strip()
                ext_path = str((prow[5] if prow else "") or "").strip()
                local_has_image = bool((payload or {}).get("has_image")) and bool(str((payload or {}).get("image_path") or "").strip())
                has_profile = not (_is_garbage_bio(bio) and _is_garbage_bio(short_bio))
                has_external_image = bool(ext_path and _existing_file_path(ext_path))
                if not (has_profile and (local_has_image or has_external_image)):
                    _run_files_profile_enrichment_job(
                        job_key=f"publish:{artist_norm}",
                        artist_name=name,
                        artist_norm=artist_norm,
                        albums=[],
                        skip_album_profiles=True,
                        fast_mode=True,
                    )
                if artist_id > 0:
                    _files_ensure_local_artist_profile(
                        conn,
                        artist_id=artist_id,
                        artist_name=name,
                        artist_norm=artist_norm,
                        entity_kind=entity_kind,
                        roles_json=roles_json,
                    )
                processed += 1
                ratio_progress, eta_seconds, rate_per_sec = _files_index_progress_metrics(
                    processed,
                    total_items,
                    started_at=started_at,
                )
                phase_progress = 96.0
                if ratio_progress is not None:
                    phase_progress = min(98.9, 96.0 + ((float(ratio_progress) / 100.0) * 2.9))
                _files_index_set_state(
                    phase="artist_enrichment",
                    phase_message="Enriching artist pages",
                    current_artist=name,
                    current_folder=None,
                    phase_item_done=int(processed),
                    phase_item_total=int(total_items),
                    phase_item_label="artist pages",
                    phase_progress=phase_progress,
                    phase_eta_seconds=eta_seconds,
                    phase_rate_per_sec=rate_per_sec,
                )
            finally:
                conn.close()
        except Exception:
            logging.debug("Blocking artist enrichment failed for %s", name, exc_info=True)


def _enqueue_files_profile_enrichment_impl(
    artist_name: str,
    artist_norm: str,
    albums: list[tuple[str, str]],
    *,
    allow_soft_profiles: Optional[bool] = None,
    skip_album_profiles: bool = False,
    fast_mode: bool = False,
    force: bool = False,
    cover_only: bool = False,
    priority_mode: str = "all",
) -> bool:
    if _get_library_mode() != "files":
        return False
    if not artist_norm:
        return False
    priority_flags = _files_profile_enrichment_priority_flags(
        priority_mode=priority_mode,
        skip_album_profiles=skip_album_profiles,
        cover_only=cover_only,
    )
    priority_mode = str(priority_flags.get("priority_mode") or "all")
    with _files_profile_backfill_lock:
        if (
            bool(_files_profile_backfill_state.get("running"))
            and bool(_files_profile_backfill_state.get("cover_only"))
            and not (bool(cover_only) or priority_mode == "p0")
        ):
            return False
    storage_scope_gate = _storage_profile_enrichment_scope_for_artist(
        artist_norm,
        albums,
    )
    if not bool(storage_scope_gate.get("allowed")):
        return False
    now = time.time()
    norm_key = str(artist_norm or "").strip()
    albums_empty = not bool(albums)
    job_key = f"{artist_norm}"
    with _files_profile_jobs_lock:
        last_ts = float(_files_profile_jobs_last_ts.get(norm_key) or 0.0)
        if albums_empty and (now - last_ts) < _FILES_PROFILE_ENRICH_EMPTY_COOLDOWN_SEC:
            return False
        if (not albums_empty) and (not force) and (now - last_ts) < _FILES_PROFILE_ENRICH_COOLDOWN_SEC:
            return False
        if job_key in _files_profile_jobs_active:
            return True
        _files_profile_jobs_active.add(job_key)
    threading.Thread(
        target=_run_files_profile_enrichment_job,
        kwargs={
            "job_key": job_key,
            "artist_name": artist_name,
            "artist_norm": artist_norm,
            "albums": albums,
            "allow_soft_profiles": allow_soft_profiles,
            "skip_album_profiles": bool(skip_album_profiles),
            "fast_mode": bool(fast_mode),
            "cover_only": bool(cover_only),
            "priority_mode": priority_mode,
        },
        daemon=True,
        name=f"profile-enrich-{artist_norm[:24]}",
    ).start()
    return True


def _files_profile_job_is_active_impl(artist_norm: str) -> bool:
    key = str(artist_norm or "").strip().lower()
    if not key:
        return False
    with _files_profile_jobs_lock:
        return key in _files_profile_jobs_active


def _files_profile_enrichment_priority_flags_impl(
    *,
    priority_mode: str = "all",
    skip_album_profiles: bool = False,
    cover_only: bool = False,
) -> dict[str, Any]:
    return _enrichment_profiles.priority_flags(
        priority_mode=priority_mode,
        skip_album_profiles=skip_album_profiles,
        cover_only=cover_only,
    )


def _files_profile_backfill_stage_specs_impl(*, cover_only: bool = False) -> list[tuple[str, str]]:
    return _enrichment_profiles.backfill_stage_specs(cover_only=cover_only)


def _files_profile_backfill_pending_work_impl() -> dict[str, int]:
    stats = {
        "pending_artist_profiles": 0,
        "pending_album_profiles": 0,
        "eligible_album_profiles": 0,
        "pending_album_covers": 0,
    }
    if _get_library_mode() != "files":
        return stats
    conn = _files_pg_connect()
    if conn is None:
        return stats
    try:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        to_regclass('public.files_artists'),
                        to_regclass('public.files_albums'),
                        to_regclass('public.files_artist_album_links')
                    """
                )
                row = cur.fetchone() or ()
            if len(row) < 3 or any(not value for value in row[:3]):
                return stats
        except Exception:
            logging.debug("Files profile backfill pending-work probe skipped: files_* tables not ready", exc_info=True)
            return stats
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM files_artists a
                LEFT JOIN files_artist_profiles prof ON prof.name_norm = a.name_norm
                LEFT JOIN files_external_artist_images ext ON ext.name_norm = a.name_norm
                WHERE prof.name_norm IS NULL
                   OR prof.updated_at IS NULL
                   OR prof.updated_at < (NOW() - INTERVAL '30 days')
                   OR (
                        COALESCE(a.has_image, FALSE) = FALSE
                        AND COALESCE(ext.image_path, '') = ''
                      )
                """
            )
            row = cur.fetchone()
            stats["pending_artist_profiles"] = int((row[0] if row else 0) or 0)
            cur.execute(
                """
                SELECT
                    COUNT(*) AS eligible_total,
                    SUM(
                        CASE
                            WHEN prof.title_norm IS NULL
                                 OR prof.updated_at IS NULL
                                 OR prof.updated_at < (NOW() - INTERVAL '30 days')
                            THEN 1
                            ELSE 0
                        END
                    ) AS pending_total
                FROM files_albums alb
                JOIN files_artists ar ON ar.id = alb.artist_id
                LEFT JOIN files_album_profiles prof
                    ON prof.artist_norm = ar.name_norm AND prof.title_norm = alb.title_norm
                WHERE COALESCE(alb.strict_match_verified, FALSE)
                   OR COALESCE(alb.metadata_source, '') <> ''
                   OR COALESCE(alb.musicbrainz_release_group_id, '') <> ''
                   OR COALESCE(alb.musicbrainz_release_id, '') <> ''
                   OR COALESCE(alb.discogs_release_id, '') <> ''
                   OR COALESCE(alb.lastfm_album_mbid, '') <> ''
                   OR COALESCE(alb.bandcamp_album_url, '') <> ''
                """
            )
            row = cur.fetchone()
            stats["eligible_album_profiles"] = int((row[0] if row else 0) or 0)
            stats["pending_album_profiles"] = int((row[1] if row else 0) or 0)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM files_albums alb
                WHERE COALESCE(alb.has_cover, FALSE) = FALSE
                  AND (
                    COALESCE(alb.strict_match_verified, FALSE)
                    OR COALESCE(alb.metadata_source, '') IN ('musicbrainz', 'discogs', 'lastfm', 'bandcamp', 'itunes', 'deezer', 'spotify', 'qobuz', 'tidal', 'audiodb')
                    OR COALESCE(alb.musicbrainz_release_group_id, '') <> ''
                    OR COALESCE(alb.musicbrainz_release_id, '') <> ''
                    OR COALESCE(alb.discogs_release_id, '') <> ''
                    OR COALESCE(alb.lastfm_album_mbid, '') <> ''
                    OR COALESCE(alb.bandcamp_album_url, '') <> ''
                  )
                """
            )
            cover_row = cur.fetchone()
            stats["pending_album_covers"] = int((cover_row[0] if cover_row else 0) or 0)
    except Exception:
        logging.debug("Files profile backfill pending-work probe failed", exc_info=True)
    finally:
        conn.close()
    return stats


def _files_profile_backfill_maybe_start_idle_impl(*, now_ts: float | None = None, reason: str = "idle_autobackfill") -> bool:
    now_ts = float(now_ts or time.time())
    if _get_library_mode() != "files":
        return False
    if _storage_power_saver_active():
        _files_profile_backfill_idle_state.update(
            {
                "last_probe_at": now_ts,
                "last_reason": "storage_power_saver_enabled",
            }
        )
        return False
    if _files_index_is_running():
        return False
    with lock:
        if bool(state.get("scanning") or state.get("scan_finalizing") or state.get("scan_starting")):
            return False
    with _files_profile_backfill_lock:
        if bool(_files_profile_backfill_state.get("running")):
            return False
    with _files_profile_jobs_lock:
        if bool(_files_profile_jobs_active):
            return False
    last_probe_at = float(_files_profile_backfill_idle_state.get("last_probe_at") or 0.0)
    if last_probe_at > 0 and (now_ts - last_probe_at) < float(_FILES_PROFILE_IDLE_AUTOSTART_INTERVAL_SEC):
        return False
    stats = _files_profile_backfill_pending_work()
    _files_profile_backfill_idle_state.update(
        {
            "last_probe_at": now_ts,
            "last_reason": str(reason or "idle_autobackfill"),
            "pending_artist_profiles": int(stats.get("pending_artist_profiles") or 0),
            "pending_album_profiles": int(stats.get("pending_album_profiles") or 0),
            "eligible_album_profiles": int(stats.get("eligible_album_profiles") or 0),
            "pending_album_covers": int(stats.get("pending_album_covers") or 0),
        }
    )
    with _files_profile_backfill_lock:
        _files_profile_backfill_state["pending_artist_profiles"] = int(stats.get("pending_artist_profiles") or 0)
        _files_profile_backfill_state["pending_album_profiles"] = int(stats.get("pending_album_profiles") or 0)
        _files_profile_backfill_state["eligible_album_profiles"] = int(stats.get("eligible_album_profiles") or 0)
        _files_profile_backfill_state["pending_album_covers"] = int(stats.get("pending_album_covers") or 0)
        _files_profile_backfill_state["last_probe_at"] = int(now_ts)
    if (
        int(stats.get("pending_artist_profiles") or 0) <= 0
        and int(stats.get("pending_album_profiles") or 0) <= 0
        and int(stats.get("pending_album_covers") or 0) <= 0
    ):
        return False
    started = _trigger_files_profile_backfill_async(reason=reason)
    if started:
        _files_profile_backfill_idle_state["last_started_at"] = now_ts
        logging.info(
            "[Profile Enrich] idle autostart reason=%s pending_artists=%d pending_albums=%d eligible_albums=%d",
            str(reason or "idle_autobackfill"),
            int(stats.get("pending_artist_profiles") or 0),
            int(stats.get("pending_album_profiles") or 0),
            int(stats.get("eligible_album_profiles") or 0),
        )
    return bool(started)


def _trigger_files_profile_backfill_async_impl(reason: str = "manual", cover_only: bool = False) -> bool:
    if _files_index_is_running():
        return False
    with lock:
        discovery_running = bool(state.get("scan_discovery_running"))
        discovery_stage = str(state.get("scan_discovery_stage") or "").strip().lower()
    if _storage_power_saver_active() and discovery_running:
        now_ts = time.time()
        key = "last_storage_discovery_delay_log_at"
        if now_ts - float(_files_profile_backfill_idle_state.get(key) or 0.0) >= 60.0:
            _files_profile_backfill_idle_state[key] = now_ts
            logging.info(
                "[STORAGE] Files profile backfill delayed: disk-aware discovery is still running (stage=%s).",
                discovery_stage or "filesystem",
            )
        return False
    storage_scope = _storage_profile_backfill_scope()
    if bool(storage_scope.get("enabled") and storage_scope.get("scan_active")) and not list(storage_scope.get("allowed_device_ids") or []):
        now_ts = time.time()
        key = "last_storage_scope_delay_log_at"
        if now_ts - float(_files_profile_backfill_idle_state.get(key) or 0.0) >= 60.0:
            _files_profile_backfill_idle_state[key] = now_ts
            logging.info("[STORAGE] Files profile backfill delayed: no active disk scope is available yet.")
        return False
    with _files_profile_backfill_lock:
        if bool(_files_profile_backfill_state.get("running")):
            return False
        pending = _files_profile_backfill_pending_work()
        # Mark running here to close race conditions (thread may start a bit later).
        _files_profile_backfill_state["running"] = True
        _files_profile_backfill_state["reason"] = str(reason or "manual")
        _files_profile_backfill_state["started_at"] = int(time.time())
        _files_profile_backfill_state["finished_at"] = 0
        _files_profile_backfill_state["cover_only"] = bool(cover_only)
        _files_profile_backfill_state["current"] = 0
        _files_profile_backfill_state["total"] = 0
        _files_profile_backfill_state["current_artist"] = ""
        _files_profile_backfill_state["errors"] = 0
        _files_profile_backfill_state["pending_artist_profiles"] = int(pending.get("pending_artist_profiles") or 0)
        _files_profile_backfill_state["pending_album_profiles"] = int(pending.get("pending_album_profiles") or 0)
        _files_profile_backfill_state["eligible_album_profiles"] = int(pending.get("eligible_album_profiles") or 0)
        _files_profile_backfill_state["pending_album_covers"] = int(pending.get("pending_album_covers") or 0)
        _files_profile_backfill_state["last_probe_at"] = int(time.time())
        _files_profile_backfill_state["phase"] = ""
        _files_profile_backfill_state["phase_label"] = ""
        _files_profile_backfill_state["phase_index"] = 0
        _files_profile_backfill_state["phase_count"] = 0
        _files_profile_backfill_state["phase_current"] = 0
        _files_profile_backfill_state["phase_total"] = 0
        _files_profile_backfill_state["storage_scope_enabled"] = bool(storage_scope.get("enabled") and storage_scope.get("scan_active"))
        _files_profile_backfill_state["storage_scope_mode"] = str(storage_scope.get("mode") or "")
        _files_profile_backfill_state["storage_scope_devices"] = list(storage_scope.get("allowed_device_ids") or [])
        if (
            bool(cover_only)
            and int(pending.get("pending_album_covers") or 0) <= 0
            and int(pending.get("pending_artist_profiles") or 0) <= 0
        ):
            _files_profile_backfill_state["running"] = False
            _files_profile_backfill_state["cover_only"] = False
            _files_profile_backfill_state["storage_scope_enabled"] = False
            _files_profile_backfill_state["storage_scope_mode"] = ""
            _files_profile_backfill_state["storage_scope_devices"] = []
            return False
    threading.Thread(
        target=_run_files_profile_backfill,
        kwargs={"reason": str(reason or "manual"), "cover_only": bool(cover_only)},
        daemon=True,
        name="files-profile-backfill",
    ).start()
    return True


def _files_relink_external_artist_images_for_artist_impl(
    conn,
    *,
    artist_name: str,
    artist_norm: str,
    alias_candidates: list[str] | tuple[str, ...] | None = None,
) -> int:
    """
    Relink orphan external artist image rows stored under a non-canonical `name_norm`
    back to the canonical artist key for this exact artist identity.
    """
    if conn is None:
        return 0
    display_name = " ".join(str(artist_name or "").split()).strip()
    target_norm = str(artist_norm or "").strip()
    if not display_name or not target_norm:
        return 0
    accepted_names = [display_name]
    accepted_names.extend(
        [
            " ".join(str(value or "").split()).strip()
            for value in (alias_candidates or [])
            if " ".join(str(value or "").split()).strip()
        ]
    )
    lowered = [str(value or "").strip().casefold() for value in accepted_names if str(value or "").strip()]
    if not lowered:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    name_norm,
                    COALESCE(artist_name, ''),
                    COALESCE(provider, ''),
                    COALESCE(image_path, ''),
                    COALESCE(image_url, '')
                FROM files_external_artist_images
                WHERE LOWER(TRIM(COALESCE(artist_name, ''))) = ANY(%s)
                """,
                (lowered,),
            )
            rows = cur.fetchall()
    except Exception:
        logging.debug("Artist image relink-by-name query failed for %s", display_name, exc_info=True)
        return 0

    moved = 0
    for old_norm, ext_artist_name, provider, image_path, image_url in rows:
        old_key = str(old_norm or "").strip()
        ext_display = " ".join(str(ext_artist_name or "").split()).strip()
        if not old_key or old_key == target_norm or not ext_display:
            continue
        if not _artist_image_exact_name_match(
            display_name,
            ext_display,
            alias_candidates=accepted_names,
        ):
            continue
        try:
            _files_upsert_external_artist_image(
                conn,
                name_norm=target_norm,
                artist_name=ext_display,
                provider=str(provider or "").strip().lower() or "lastfm",
                image_path=str(image_path or "").strip() or None,
                image_url=str(image_url or "").strip() or None,
            )
            with conn.cursor() as cur:
                cur.execute("DELETE FROM files_external_artist_images WHERE name_norm = %s", (old_key,))
            moved += 1
        except Exception:
            logging.debug(
                "Artist image relink-by-name failed old=%s target=%s artist=%s",
                old_key,
                target_norm,
                display_name,
                exc_info=True,
            )
    if moved:
        logging.info("[Artist Image] relinked %d cached row(s) for %s", int(moved), display_name)
    return int(moved)


def _storage_profile_backfill_scope_impl(settings_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Return the bounded storage scope allowed for background profile backfill.

    When a disk-aware files scan is active, backfill must not fan out across the
    whole array. It is constrained to the currently active scan device, plus any
    extra devices explicitly budgeted by STORAGE_MAX_ACTIVE_DEVICES.
    """
    cfg = _storage_unraid_settings(settings_snapshot)
    if not bool(cfg.get("enabled")):
        return {
            "enabled": False,
            "scan_active": False,
            "max_active_devices": int(cfg.get("max_active_devices") or 1),
            "allowed_device_ids": [],
            "allowed_device_labels": [],
            "plan_entries": [],
            "mode": "disabled",
        }
    with lock:
        scan_active = bool(
            state.get("scanning")
            or state.get("scan_starting")
            or state.get("scan_finalizing")
            or state.get("scan_discovery_running")
        )
        current_device_id = str(state.get("storage_current_device_id") or "").strip()
        current_device_label = str(state.get("storage_current_device_label") or current_device_id).strip() or current_device_id
        plan_entries = [dict(item or {}) for item in list(state.get("storage_scan_plan") or []) if isinstance(item, dict)]
    max_active_devices = int(max(1, int(cfg.get("max_active_devices") or 1)))
    if not scan_active:
        return {
            "enabled": True,
            "scan_active": False,
            "max_active_devices": max_active_devices,
            "allowed_device_ids": [],
            "allowed_device_labels": [],
            "plan_entries": plan_entries,
            "mode": "idle_unbounded",
        }
    if not current_device_id:
        return {
            "enabled": True,
            "scan_active": True,
            "max_active_devices": max_active_devices,
            "allowed_device_ids": [],
            "allowed_device_labels": [],
            "plan_entries": plan_entries,
            "mode": "scan_waiting_for_current_device",
        }
    allowed: list[str] = []
    labels: list[str] = []
    if current_device_id:
        allowed.append(current_device_id)
        labels.append(current_device_label)
    remaining_slots = max(0, max_active_devices - len(set(allowed)))
    if remaining_slots > 0:
        for entry in plan_entries:
            device_id = str(entry.get("storage_device_id") or "").strip()
            if not device_id or device_id in allowed:
                continue
            status = str(entry.get("status") or "pending").strip().lower()
            if status not in {"pending", "running"}:
                continue
            allowed.append(device_id)
            labels.append(str(entry.get("storage_device_label") or device_id).strip() or device_id)
            if len(set(allowed)) >= max_active_devices:
                break
    # Keep order stable while deduping.
    seen_devices: set[str] = set()
    allowed_ids: list[str] = []
    allowed_labels: list[str] = []
    for idx, device_id in enumerate(allowed):
        if not device_id or device_id in seen_devices:
            continue
        seen_devices.add(device_id)
        allowed_ids.append(device_id)
        label = labels[idx] if idx < len(labels) else device_id
        allowed_labels.append(str(label or device_id).strip() or device_id)
    return {
        "enabled": True,
        "scan_active": True,
        "max_active_devices": max_active_devices,
        "allowed_device_ids": allowed_ids,
        "allowed_device_labels": allowed_labels,
        "plan_entries": plan_entries,
        "mode": "scan_budget",
    }


def _storage_profile_backfill_scope_signature_impl(scope: dict[str, Any] | None) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(device_id or "").strip()
                for device_id in list((scope or {}).get("allowed_device_ids") or [])
                if str(device_id or "").strip()
            }
        )
    )


def _storage_profile_enrichment_scope_for_artist_impl(
    artist_norm: str,
    albums: list[tuple[str, str]] | None,
    *,
    settings_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Gate opportunistic profile enrichment against the current disk-aware scope.

    When a disk-aware scan is active, on-demand enrichment must not wake albums
    that live outside the currently allowed storage device budget.
    """
    scope = _storage_profile_backfill_scope(settings_snapshot=settings_snapshot)
    result = {
        "enabled": bool(scope.get("enabled")),
        "scan_active": bool(scope.get("scan_active")),
        "allowed": True,
        "reason": "unbounded",
        "allowed_device_ids": list(scope.get("allowed_device_ids") or []),
        "matched_device_ids": [],
    }
    if not (bool(scope.get("enabled")) and bool(scope.get("scan_active"))):
        return result
    allowed_device_ids = {
        str(device_id or "").strip()
        for device_id in list(scope.get("allowed_device_ids") or [])
        if str(device_id or "").strip()
    }
    if not allowed_device_ids:
        result["allowed"] = False
        result["reason"] = str(scope.get("mode") or "scan_waiting_for_current_device")
        return result
    artist_norm_txt = str(artist_norm or "").strip()
    if not artist_norm_txt:
        result["allowed"] = False
        result["reason"] = "missing_artist_norm"
        return result
    album_norms = [
        str(norm or "").strip()
        for _title, norm in list(albums or [])
        if str(norm or "").strip()
    ]
    folder_rows: list[str] = []
    plan_entries = list(scope.get("plan_entries") or [])
    with _files_pg_connection() as conn:
        if conn is None:
            result["allowed"] = False
            result["reason"] = "storage_scope_db_unavailable"
            return result
        try:
            with conn.cursor() as cur:
                if album_norms:
                    placeholders = ",".join(["%s"] * len(album_norms))
                    cur.execute(
                        f"""
                        SELECT DISTINCT COALESCE(alb.folder_path, '')
                        FROM files_albums alb
                        JOIN files_artists ar ON ar.id = alb.artist_id
                        WHERE ar.name_norm = %s
                          AND alb.title_norm IN ({placeholders})
                        """,
                        [artist_norm_txt, *album_norms],
                    )
                else:
                    cur.execute(
                        """
                        SELECT DISTINCT COALESCE(alb.folder_path, '')
                        FROM files_albums alb
                        JOIN files_artists ar ON ar.id = alb.artist_id
                        WHERE ar.name_norm = %s
                        LIMIT 256
                        """,
                        (artist_norm_txt,),
                    )
                folder_rows = [str((row[0] if row else "") or "").strip() for row in cur.fetchall()]
        except Exception:
            logging.debug(
                "Storage scope probe failed for artist profile enrichment artist_norm=%s",
                artist_norm_txt,
                exc_info=True,
            )
            result["allowed"] = False
            result["reason"] = "storage_scope_probe_failed"
            return result
    matched_device_ids: set[str] = set()
    for folder_path_raw in folder_rows:
        if not folder_path_raw:
            continue
        entry = _storage_plan_entry_for_canonical_path(folder_path_raw, plan_entries)
        device_id = str((entry or {}).get("storage_device_id") or "").strip()
        if device_id:
            matched_device_ids.add(device_id)
    result["matched_device_ids"] = sorted(matched_device_ids)
    if not matched_device_ids:
        result["allowed"] = False
        result["reason"] = "artist_scope_unknown"
        return result
    if not matched_device_ids.issubset(allowed_device_ids):
        result["allowed"] = False
        result["reason"] = "artist_out_of_scope"
        return result
    result["reason"] = "scan_budget"
    return result
