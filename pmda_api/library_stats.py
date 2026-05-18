"""Library statistics API routes."""

from __future__ import annotations

import logging
import re
from typing import Any

from flask import Blueprint, jsonify, request


def create_library_stats_blueprint(*, runtime: Any) -> Blueprint:
    """Create library statistics routes while keeping HTTP paths stable."""

    blueprint = Blueprint("pmda_library_stats", __name__)

    @blueprint.get("/api/library/stats", endpoint="api_library_stats")
    def api_library_stats():
        """Return fast library counts for the selected browse scope."""
        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Files mode required"}), 400

        include_unmatched = runtime._library_include_unmatched_effective()
        scope = runtime._library_scope_effective()
        browse_source_requested = runtime._files_library_browse_source_requested()
        matched_where = runtime._library_albums_match_where(include_unmatched, "alb")
        scope_where = runtime._library_album_scope_where(scope, "alb")
        cache_key = (
            f"library:stats:{runtime._library_cache_scope_suffix(scope)}:"
            f"{runtime._library_cache_unmatched_suffix(include_unmatched)}"
        )
        cached = runtime._files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)
        snapshot = runtime._files_index_maybe_enqueue_published_catchup(
            include_unmatched=include_unmatched,
            scope=scope,
            reason=f"api_library_stats_{scope}",
        )
        browse_source = runtime._files_library_browse_source_effective(
            scope=scope,
            requested=browse_source_requested,
            snapshot=snapshot,
        )
        index_state = dict(snapshot.get("index_state") or {})
        index_running = bool(index_state.get("running"))
        bootstrap_required = bool(runtime._pipeline_bootstrap_status().get("bootstrap_required"))
        if browse_source == "published" or bool(snapshot.get("underbuilt")):
            if not index_running and not bootstrap_required and int(snapshot.get("published_albums") or 0) <= 0:
                runtime._trigger_files_index_rebuild_async_throttled(
                    reason="api_library_stats_bootstrap",
                    cooldown_sec=45.0,
                )
                index_state = runtime._files_index_get_state() or {}
                index_running = bool(index_state.get("running"))
            payload = {
                "artists": int(snapshot.get("published_artists") or 0),
                "albums": int(snapshot.get("published_albums") or 0),
                "tracks": int(snapshot.get("published_tracks") or 0),
                "index_running": index_running,
                "index_phase": str(index_state.get("phase") or ""),
                "scope": scope,
                "fallback_source": "published" if int(snapshot.get("published_albums") or 0) > 0 else None,
                "index_rehydrating": bool(snapshot.get("published_albums") or 0),
            }
            runtime._files_cache_set_json(cache_key, payload, ttl=10)
            return jsonify(payload)

        if not runtime._files_pg_init_schema():
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        conn = runtime._files_pg_connect(acquire_timeout_sec=0.75)
        if conn is None:
            cached = runtime._files_cache_get_json(cache_key)
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                return jsonify(payload)
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                with runtime._files_pg_statement_timeout(cur, 1500):
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM files_albums alb
                        WHERE {matched_where}
                          AND {scope_where}
                        """
                    )
                    albums = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute(
                        f"""
                        SELECT COUNT(DISTINCT alb.artist_id)
                        FROM files_albums alb
                        WHERE {matched_where}
                          AND {scope_where}
                        """
                    )
                    artists = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM files_tracks tr
                        JOIN files_albums alb ON alb.id = tr.album_id
                        WHERE {matched_where}
                          AND {scope_where}
                        """
                    )
                    tracks = int((cur.fetchone() or [0])[0] or 0)
            if runtime._files_library_should_fallback_to_published(snapshot, albums=albums, artists=artists):
                payload = {
                    "artists": int(snapshot.get("published_artists") or 0),
                    "albums": int(snapshot.get("published_albums") or 0),
                    "tracks": int(snapshot.get("published_tracks") or 0),
                    "scope": scope,
                    "fallback_source": "published",
                    "index_rehydrating": bool(snapshot.get("published_albums") or 0),
                }
                runtime._files_cache_set_json(cache_key, payload, ttl=10)
                return jsonify(payload)
            payload = {"artists": artists, "albums": albums, "tracks": tracks, "scope": scope}
            runtime._files_cache_set_json(cache_key, payload, ttl=30)
            return jsonify(payload)
        except Exception as e:
            logging.debug("library stats query falling back during scan: %s", e)
            if int(snapshot.get("published_albums") or 0) > 0:
                payload = {
                    "artists": int(snapshot.get("published_artists") or 0),
                    "albums": int(snapshot.get("published_albums") or 0),
                    "tracks": int(snapshot.get("published_tracks") or 0),
                    "scope": scope,
                    "fallback_source": "published",
                    "stale": True,
                    "index_rehydrating": bool(snapshot.get("published_albums") or 0),
                }
                runtime._files_cache_set_json(cache_key, payload, ttl=10)
                return jsonify(payload)
            cached = runtime._files_cache_get_json(cache_key)
            if cached is not None:
                payload = dict(cached)
                payload["stale"] = True
                return jsonify(payload)
            return jsonify({"error": "PostgreSQL unavailable", "stale": True}), 503
        finally:
            conn.close()

    @blueprint.get("/api/library/stats/library", endpoint="api_library_stats_library")
    def api_library_stats_library():
        """Return library-wide distributions for charts."""
        if runtime._get_library_mode() != "files":
            return jsonify({"error": "Files mode required"}), 400
        cache_key = "library:stats:library"
        cached = runtime._files_cache_get_json(cache_key)
        if cached is not None:
            return jsonify(cached)
        snapshot = runtime._files_index_maybe_enqueue_published_catchup(
            include_unmatched=True,
            scope="all",
            reason="api_library_stats_library",
        )
        browse_source = runtime._files_library_browse_source_effective(
            scope="all",
            requested=runtime._files_library_browse_source_requested(),
            snapshot=snapshot,
        )
        index_state = dict(snapshot.get("index_state") or {})
        index_running = bool(index_state.get("running"))
        bootstrap_required = bool(runtime._pipeline_bootstrap_status().get("bootstrap_required"))
        if browse_source == "published" or bool(snapshot.get("underbuilt")):
            if not index_running and not bootstrap_required and int(snapshot.get("published_albums") or 0) <= 0:
                runtime._trigger_files_index_rebuild_async_throttled(
                    reason="api_library_stats_library_bootstrap",
                    cooldown_sec=45.0,
                )
                index_state = runtime._files_index_get_state() or {}
                index_running = bool(index_state.get("running"))
            payload = {
                "artists": int(snapshot.get("published_artists") or 0),
                "albums": int(snapshot.get("published_albums") or 0),
                "tracks": int(snapshot.get("published_tracks") or 0),
                "years": [],
                "growth": [],
                "formats": [],
                "quality": {
                    "lossless": 0,
                    "lossy": 0,
                    "with_cover": 0,
                    "without_cover": max(0, int(snapshot.get("published_albums") or 0)),
                },
                "genres": [],
                "labels": [],
                "source_paths": [],
                "index_running": index_running,
                "index_phase": str(index_state.get("phase") or ""),
                "fallback_source": "published" if int(snapshot.get("published_albums") or 0) > 0 else None,
                "index_rehydrating": bool(snapshot.get("published_albums") or 0),
            }
            runtime._files_cache_set_json(cache_key, payload, ttl=10)
            return jsonify(payload)
        if not runtime._files_pg_init_schema():
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        conn = runtime._files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM files_artists")
                artists = int((cur.fetchone() or [0])[0] or 0)
                cur.execute("SELECT COUNT(*) FROM files_albums")
                albums = int((cur.fetchone() or [0])[0] or 0)
                cur.execute("SELECT COUNT(*) FROM files_tracks")
                tracks = int((cur.fetchone() or [0])[0] or 0)

                if runtime._files_library_should_fallback_to_published(snapshot, albums=albums, artists=artists):
                    payload = {
                        "artists": int(snapshot.get("published_artists") or 0),
                        "albums": int(snapshot.get("published_albums") or 0),
                        "tracks": int(snapshot.get("published_tracks") or 0),
                        "years": [],
                        "growth": [],
                        "formats": [],
                        "quality": {
                            "lossless": 0,
                            "lossy": 0,
                            "with_cover": 0,
                            "without_cover": max(0, int(snapshot.get("published_albums") or 0)),
                        },
                        "genres": [],
                        "labels": [],
                        "source_paths": [],
                        "index_running": index_running,
                        "index_phase": str(index_state.get("phase") or ""),
                        "fallback_source": "published",
                        "index_rehydrating": bool(snapshot.get("published_albums") or 0),
                    }
                    runtime._files_cache_set_json(cache_key, payload, ttl=10)
                    return jsonify(payload)

                cur.execute(
                    """
                    SELECT COALESCE(year, 0) AS year, COUNT(*) AS c
                    FROM files_albums
                    WHERE COALESCE(year, 0) > 0
                    GROUP BY COALESCE(year, 0)
                    ORDER BY year ASC
                    """
                )
                years = [{"year": int(r[0] or 0), "count": int(r[1] or 0)} for r in cur.fetchall() if int(r[0] or 0) > 0]

                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', created_at), 'YYYY-MM') AS ym, COUNT(*) AS c
                    FROM files_albums
                    GROUP BY ym
                    ORDER BY ym ASC
                    """
                )
                growth = [{"month": str(r[0] or ""), "count": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

                cur.execute(
                    """
                    SELECT UPPER(TRIM(COALESCE(format, ''))) AS fmt, COUNT(*) AS c
                    FROM files_albums
                    GROUP BY UPPER(TRIM(COALESCE(format, '')))
                    ORDER BY c DESC, fmt ASC
                    LIMIT 40
                    """
                )
                formats = [{"format": str(r[0] or "").strip() or "-", "count": int(r[1] or 0)} for r in cur.fetchall()]

                cur.execute("SELECT SUM(CASE WHEN has_cover THEN 1 ELSE 0 END), SUM(CASE WHEN is_lossless THEN 1 ELSE 0 END) FROM files_albums")
                row = cur.fetchone() or [0, 0]
                with_cover = int(row[0] or 0)
                lossless = int(row[1] or 0)

                cur.execute(
                    """
                    SELECT lower(trim(g.value)) AS genre, COUNT(*) AS c
                    FROM files_albums alb
                    JOIN LATERAL jsonb_array_elements_text(COALESCE(NULLIF(alb.tags_json, ''), '[]')::jsonb) AS g(value) ON TRUE
                    WHERE COALESCE(trim(g.value), '') <> ''
                    GROUP BY lower(trim(g.value))
                    ORDER BY c DESC, genre ASC
                    LIMIT 80
                    """
                )
                genre_rows = cur.fetchall()
                genres = [{"genre": str(r[0] or "").strip(), "count": int(r[1] or 0)} for r in genre_rows if str(r[0] or "").strip()]
                if not genres:
                    cur.execute(
                        """
                        SELECT lower(trim(COALESCE(genre, ''))) AS genre, COUNT(*) AS c
                        FROM files_albums
                        WHERE COALESCE(trim(genre), '') <> ''
                        GROUP BY lower(trim(COALESCE(genre, '')))
                        ORDER BY c DESC, genre ASC
                        LIMIT 80
                        """
                    )
                    genres = [{"genre": str(r[0] or "").strip(), "count": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

                cur.execute(
                    """
                    SELECT COUNT(DISTINCT TRIM(COALESCE(label, '')))
                    FROM files_albums
                    WHERE COALESCE(trim(label), '') <> ''
                    """
                )
                labels_total = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    """
                    SELECT TRIM(COALESCE(label, '')) AS label, COUNT(*) AS c
                    FROM files_albums
                    WHERE COALESCE(trim(label), '') <> ''
                    GROUP BY TRIM(COALESCE(label, ''))
                    ORDER BY c DESC, label ASC
                    LIMIT 80
                    """
                )
                labels = [{"label": str(r[0] or "").strip(), "count": int(r[1] or 0)} for r in cur.fetchall() if str(r[0] or "").strip()]

                def _norm_root_path(val: str) -> str:
                    s = re.sub(r"/+", "/", str(val or "").strip().replace("\\", "/"))
                    if not s:
                        return ""
                    if not s.startswith("/"):
                        s = "/" + s
                    return s.rstrip("/")

                roots_raw = runtime._parse_files_roots(
                    runtime._get_config_from_db(
                        "FILES_ROOTS",
                        ",".join(runtime.FILES_ROOTS) if isinstance(runtime.FILES_ROOTS, list) else (runtime.FILES_ROOTS or ""),
                    )
                )
                roots_norm: list[str] = []
                seen_roots: set[str] = set()
                for root in roots_raw or []:
                    nr = _norm_root_path(root)
                    if not nr or nr in seen_roots:
                        continue
                    seen_roots.add(nr)
                    roots_norm.append(nr)

                source_paths: list[dict[str, Any]] = []
                for root in roots_norm:
                    match_params = [root, f"{root}/%"]
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM files_albums alb
                        WHERE COALESCE(alb.folder_path, '') = %s
                           OR COALESCE(alb.folder_path, '') LIKE %s
                        """,
                        match_params,
                    )
                    root_albums = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT alb.artist_id)
                        FROM files_albums alb
                        WHERE COALESCE(alb.folder_path, '') = %s
                           OR COALESCE(alb.folder_path, '') LIKE %s
                        """,
                        match_params,
                    )
                    root_artists = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT lower(trim(COALESCE(alb.label, ''))))
                        FROM files_albums alb
                        WHERE (COALESCE(alb.folder_path, '') = %s OR COALESCE(alb.folder_path, '') LIKE %s)
                          AND COALESCE(trim(alb.label), '') <> ''
                        """,
                        match_params,
                    )
                    root_labels = int((cur.fetchone() or [0])[0] or 0)
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM files_tracks tr
                        JOIN files_albums alb ON alb.id = tr.album_id
                        WHERE COALESCE(alb.folder_path, '') = %s
                           OR COALESCE(alb.folder_path, '') LIKE %s
                        """,
                        match_params,
                    )
                    root_tracks = int((cur.fetchone() or [0])[0] or 0)
                    source_paths.append(
                        {
                            "path": root,
                            "albums": root_albums,
                            "artists": root_artists,
                            "labels": root_labels,
                            "tracks": root_tracks,
                            "albums_pct": round((root_albums / albums) * 100.0, 2) if albums > 0 else 0.0,
                            "artists_pct": round((root_artists / artists) * 100.0, 2) if artists > 0 else 0.0,
                            "labels_pct": round((root_labels / max(1, labels_total)) * 100.0, 2) if labels_total > 0 else 0.0,
                        }
                    )

                if roots_norm:
                    root_or = " OR ".join(["(COALESCE(alb.folder_path, '') = %s OR COALESCE(alb.folder_path, '') LIKE %s)"] * len(roots_norm))
                    root_params: list[str] = []
                    for root in roots_norm:
                        root_params.extend([root, f"{root}/%"])
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM files_albums alb
                        WHERE NOT ({root_or})
                        """,
                        root_params,
                    )
                    unknown_albums = int((cur.fetchone() or [0])[0] or 0)
                    if unknown_albums > 0:
                        cur.execute(
                            f"""
                            SELECT COUNT(DISTINCT alb.artist_id)
                            FROM files_albums alb
                            WHERE NOT ({root_or})
                            """,
                            root_params,
                        )
                        unknown_artists = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            f"""
                            SELECT COUNT(DISTINCT lower(trim(COALESCE(alb.label, ''))))
                            FROM files_albums alb
                            WHERE NOT ({root_or})
                              AND COALESCE(trim(alb.label), '') <> ''
                            """,
                            root_params,
                        )
                        unknown_labels = int((cur.fetchone() or [0])[0] or 0)
                        cur.execute(
                            f"""
                            SELECT COUNT(*)
                            FROM files_tracks tr
                            JOIN files_albums alb ON alb.id = tr.album_id
                            WHERE NOT ({root_or})
                            """,
                            root_params,
                        )
                        unknown_tracks = int((cur.fetchone() or [0])[0] or 0)
                        source_paths.append(
                            {
                                "path": "(outside configured roots)",
                                "albums": unknown_albums,
                                "artists": unknown_artists,
                                "labels": unknown_labels,
                                "tracks": unknown_tracks,
                                "albums_pct": round((unknown_albums / albums) * 100.0, 2) if albums > 0 else 0.0,
                                "artists_pct": round((unknown_artists / artists) * 100.0, 2) if artists > 0 else 0.0,
                                "labels_pct": round((unknown_labels / max(1, labels_total)) * 100.0, 2) if labels_total > 0 else 0.0,
                            }
                        )
                source_paths.sort(key=lambda item: int(item.get("albums") or 0), reverse=True)

            payload = {
                "artists": artists,
                "albums": albums,
                "tracks": tracks,
                "years": years,
                "growth": growth,
                "genres": genres,
                "labels": labels,
                "formats": formats,
                "quality": {
                    "with_cover": with_cover,
                    "without_cover": max(0, albums - with_cover),
                    "lossless": lossless,
                    "lossy": max(0, albums - lossless),
                },
                "source_paths": source_paths,
            }
            runtime._files_cache_set_json(cache_key, payload, ttl=30)
            return jsonify(payload)
        finally:
            conn.close()

    return blueprint
