"""Runtime-backed artist identity and alias publication helpers.

This module owns the files-library artist canonical identity and alias table
side effects extracted from ``pmda.py``. It binds to the live runtime for
normalization, classical-person heuristics, and MusicBrainz identity lookup.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Optional

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    '_library_artist_display_name',
    '_musicbrainz_artist_identity_lookup_cached',
    '_files_merge_artist_alias_values',
    '_files_upsert_artist_canonical_identity',
    '_files_artist_alias_rows_for_identity',
    '_files_sync_artist_aliases',
    '_files_backfill_artist_alias_table',
    '_files_best_person_entity_kind',
    '_files_merge_artist_album_links_to_winner',
    '_files_get_artist_alias_candidates',
    '_files_upsert_artist_external_aliases',
}


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("PMDA runtime is not bound")
    return _RUNTIME


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one artist identity operation."""
    global _RUNTIME
    _RUNTIME = runtime
    globals().update({key: value for key, value in vars(runtime).items() if key not in _EXTRACTED_NAMES})

def _library_artist_display_name(
    *,
    current_name: str,
    canonical_name: str = "",
    entity_kind: str = "",
    roles_json: Any = None,
    aliases_json: Any = None,
) -> str:
    current_txt = " ".join(str(current_name or "").split()).strip()
    canonical_txt = " ".join(str(canonical_name or "").split()).strip()
    role_hints = _artist_role_hints_from_roles_json(roles_json)
    aliases = _safe_json_load(aliases_json or "[]", fallback=[])
    alias_values = [str(value or "").strip() for value in aliases if str(value or "").strip()]
    if _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints):
        return (
            _select_classical_person_display_name(
                current_name=current_txt,
                primary_name=canonical_txt or current_txt,
                aliases=alias_values,
            )
            or canonical_txt
            or current_txt
        )
    return _choose_preferred_identity_display(current_txt, canonical_txt or current_txt) or canonical_txt or current_txt


@lru_cache(maxsize=2048)
def _musicbrainz_artist_identity_lookup_cached(
    artist_name: str,
    entity_kind: str = "",
    role_hints_key: tuple[str, ...] = (),
) -> dict[str, Any]:
    return _musicbrainz_artist_identity_lookup(
        artist_name,
        entity_kind=entity_kind,
        role_hints=list(role_hints_key or ()),
    )


def _files_merge_artist_alias_values(*sources: Any) -> list[str]:
    best_by_norm: dict[str, str] = {}
    order: list[str] = []
    for source in sources:
        raw = source
        if isinstance(raw, str):
            raw = _safe_json_load(raw, fallback=[raw])
        if not isinstance(raw, (list, tuple, set)):
            raw = [raw]
        for value in raw:
            clean = " ".join(str(value or "").split()).strip()
            norm = _norm_artist_key(clean)
            if not clean or not norm:
                continue
            current = best_by_norm.get(norm)
            if current is None:
                best_by_norm[norm] = clean
                order.append(norm)
                continue
            if _identity_display_quality_score(clean) > _identity_display_quality_score(current):
                best_by_norm[norm] = clean
    return [best_by_norm[norm] for norm in order if norm in best_by_norm]


def _files_upsert_artist_canonical_identity(
    conn,
    *,
    artist_id: int,
    artist_norm: str,
    artist_name: str,
    canonical_name: str,
    canonical_mbid: str = "",
    aliases: list[str] | tuple[str, ...] | None = None,
    entity_kind: str = "",
    roles_json: Any = None,
) -> None:
    if conn is None or int(artist_id or 0) <= 0:
        return
    current_name = " ".join(str(artist_name or "").split()).strip()
    canonical = " ".join(str(canonical_name or "").split()).strip() or current_name
    if not canonical:
        return
    role_hints = _artist_role_hints_from_roles_json(roles_json)
    person_like = _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
    if person_like:
        canonical = _choose_preferred_person_identity_name(current_name, canonical)
    existing_aliases = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(aliases_json, '[]') FROM files_artists WHERE id = %s LIMIT 1", (int(artist_id),))
            row = cur.fetchone()
        existing_aliases = _safe_json_load((row[0] if row else "[]") or "[]", fallback=[])
    except Exception:
        existing_aliases = []
    merged_aliases = _files_merge_artist_alias_values(existing_aliases, [current_name, canonical], aliases or [])
    if person_like:
        merged_aliases = _collapse_classical_person_aliases(merged_aliases)
        canonical = _select_classical_person_display_name(
            current_name=current_name,
            primary_name=canonical,
            aliases=merged_aliases,
        ) or canonical
    display_name = (
        _select_classical_person_display_name(
            current_name=current_name,
            primary_name=canonical,
            aliases=merged_aliases,
        )
        if person_like
        else _choose_preferred_identity_display(current_name, canonical)
    )
    canonical_norm = _norm_artist_key(canonical) or _norm_artist_key(display_name) or str(artist_norm or "").strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE files_artists
            SET name = %s,
                canonical_name = %s,
                canonical_name_norm = %s,
                canonical_mbid = COALESCE(NULLIF(%s, ''), canonical_mbid),
                aliases_json = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                display_name,
                canonical,
                canonical_norm,
                str(canonical_mbid or "").strip(),
                json.dumps(merged_aliases, ensure_ascii=False),
                int(artist_id),
            ),
        )
    _files_upsert_artist_external_aliases(
        conn,
        artist_id=int(artist_id),
        artist_norm=str(artist_norm or "").strip(),
        artist_name=display_name,
        aliases=merged_aliases,
        entity_kind=entity_kind,
        roles_json=role_hints,
        source="musicbrainz",
    )


def _files_artist_alias_rows_for_identity(
    *,
    artist_name: str,
    artist_norm: str,
    canonical_name: str = "",
    entity_kind: str = "",
    roles_json: Any = None,
    aliases_json: Any = None,
) -> list[dict[str, Any]]:
    name = " ".join(str(artist_name or "").split()).strip()
    canonical_name_txt = " ".join(str(canonical_name or "").split()).strip()
    canonical_norm = str(artist_norm or "").strip() or _norm_artist_key(name)
    if not (name or canonical_name_txt) or not canonical_norm:
        return []
    role_hints = _artist_role_hints_from_roles_json(roles_json)
    person_like = _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
    alias_values: list[tuple[str, str]] = []
    if canonical_name_txt:
        alias_values.append((canonical_name_txt, "canonical"))
    if name:
        alias_values.append((name, "name"))
    raw_aliases = aliases_json
    if isinstance(raw_aliases, str):
        raw_aliases = _safe_json_load(raw_aliases, fallback=[])
    if isinstance(raw_aliases, list):
        for value in raw_aliases:
            clean = " ".join(str(value or "").split()).strip()
            if clean:
                alias_values.append((clean, "alias_json"))
    classical_like = _artist_entity_is_classical_like(entity_kind=entity_kind, role_hints=role_hints)
    if classical_like:
        try:
            mb_identity = _musicbrainz_artist_identity_lookup_cached(
                canonical_name_txt or name,
                entity_kind=str(entity_kind or "").strip().lower(),
                role_hints_key=tuple(sorted({str(role or "").strip().lower() for role in role_hints if str(role or "").strip()})),
            ) or {}
        except Exception:
            mb_identity = {}
        if isinstance(mb_identity, dict):
            mb_name = " ".join(str(mb_identity.get("name") or "").split()).strip()
            mb_sort = " ".join(str(mb_identity.get("sort_name") or "").split()).strip()
            if mb_name:
                alias_values.append((mb_name, "musicbrainz:name"))
            if mb_sort:
                alias_values.append((mb_sort, "musicbrainz:sort_name"))
            for alias in (mb_identity.get("aliases") or []):
                clean = " ".join(str(alias or "").split()).strip()
                if clean:
                    alias_values.append((clean, "musicbrainz:alias"))
    if person_like:
        generated_rows: list[tuple[str, str]] = []
        for alias, source in list(alias_values):
            for generated in _classical_person_generated_aliases(alias):
                generated_rows.append((generated, f"{source}:generated"))
        alias_values.extend(generated_rows)
    rows: list[dict[str, Any]] = []
    seen_norms: set[str] = set()
    for alias, source in alias_values:
        alias_norm = _norm_artist_key(alias)
        if not alias_norm or alias_norm in seen_norms:
            continue
        seen_norms.add(alias_norm)
        rows.append(
            {
                "alias": alias,
                "alias_norm": alias_norm,
                "alias_signature": _classical_person_signature_key(alias) if person_like else "",
                "is_canonical": bool(alias_norm == canonical_norm),
                "source": source,
            }
        )
    return rows


def _files_sync_artist_aliases(
    conn,
    *,
    artists_map: Optional[dict[str, dict[str, Any]]] = None,
    artist_norms: Optional[list[str]] = None,
) -> None:
    norms: list[str] = []
    if artists_map:
        norms.extend(str(key or "").strip() for key in artists_map.keys())
    if artist_norms:
        norms.extend(str(key or "").strip() for key in artist_norms)
    norms = [value for value in dict.fromkeys(norms) if value]
    if not norms:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, name_norm, COALESCE(canonical_name, ''), COALESCE(entity_kind, 'artist'), COALESCE(roles_json, '[]'), COALESCE(aliases_json, '[]')
            FROM files_artists
            WHERE name_norm = ANY(%s)
            """,
            (norms,),
        )
        db_rows = {
            str(name_norm or "").strip(): {
                "artist_id": int(artist_id or 0),
                "name": str(name or "").strip(),
                "canonical_name": str(canonical_name or "").strip(),
                "entity_kind": str(entity_kind or "artist").strip() or "artist",
                "roles_json": roles_json or "[]",
                "aliases_json": aliases_json or "[]",
            }
            for artist_id, name, name_norm, canonical_name, entity_kind, roles_json, aliases_json in cur.fetchall()
            if str(name_norm or "").strip() and int(artist_id or 0) > 0
        }
        artist_ids = [int(row["artist_id"]) for row in db_rows.values() if int(row.get("artist_id") or 0) > 0]
        if artist_ids:
            cur.execute("DELETE FROM files_artist_aliases WHERE artist_id = ANY(%s)", (artist_ids,))
        insert_rows: list[tuple[Any, ...]] = []
        for norm in norms:
            db_row = db_rows.get(norm) or {}
            if not db_row:
                continue
            payload = (artists_map or {}).get(norm) or {}
            artist_name = str(payload.get("name") or db_row.get("name") or "").strip()
            canonical_name = str(payload.get("canonical_name") or "").strip() or str(db_row.get("canonical_name") or artist_name).strip()
            entity_kind = str(payload.get("entity_kind") or db_row.get("entity_kind") or "artist").strip() or "artist"
            roles_json = payload.get("roles_json") or db_row.get("roles_json") or "[]"
            aliases_json = payload.get("aliases_json") or db_row.get("aliases_json") or "[]"
            alias_rows = _files_artist_alias_rows_for_identity(
                artist_name=artist_name,
                artist_norm=norm,
                canonical_name=canonical_name,
                entity_kind=entity_kind,
                roles_json=roles_json,
                aliases_json=aliases_json,
            )
            for row in alias_rows:
                insert_rows.append(
                    (
                        int(db_row["artist_id"]),
                        norm,
                        str(row.get("alias") or "").strip(),
                        str(row.get("alias_norm") or "").strip(),
                        str(row.get("alias_signature") or "").strip(),
                        bool(row.get("is_canonical")),
                        str(row.get("source") or "artist").strip() or "artist",
                    )
                )
        if insert_rows:
            batch_size = max(25, int(os.getenv("FILES_ARTIST_ALIAS_BATCH_SIZE", "250") or 250))
            for start in range(0, len(insert_rows), batch_size):
                cur.executemany(
                    """
                    INSERT INTO files_artist_aliases (
                        artist_id,
                        artist_name_norm,
                        alias,
                        alias_norm,
                        alias_signature,
                        is_canonical,
                        source,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (artist_id, alias_norm)
                    DO UPDATE SET
                        alias = EXCLUDED.alias,
                        artist_name_norm = EXCLUDED.artist_name_norm,
                        alias_signature = EXCLUDED.alias_signature,
                        is_canonical = files_artist_aliases.is_canonical OR EXCLUDED.is_canonical,
                        source = CASE
                            WHEN files_artist_aliases.source = 'canonical' THEN files_artist_aliases.source
                            ELSE EXCLUDED.source
                        END,
                        updated_at = NOW()
                    """,
                    insert_rows[start : start + batch_size],
                )


def _files_backfill_artist_alias_table(conn) -> None:
    return _artist_maintenance.backfill_artist_alias_table(
        conn,
        files_sync_artist_aliases=_files_sync_artist_aliases,
        logger=logging,
    )


def _files_best_person_entity_kind(entity_kinds: list[str], role_values: list[str]) -> str:
    kinds = {str(value or "").strip().lower() for value in (entity_kinds or []) if str(value or "").strip()}
    roles = {str(value or "").strip().lower() for value in (role_values or []) if str(value or "").strip()}
    if "composer" in kinds or "composer" in roles:
        return "composer"
    if "conductor" in kinds or "conductor" in roles:
        return "conductor"
    if kinds.intersection({"soloist", "performer"}) or roles.intersection({"soloist", "performer"}):
        return "performer"
    return "artist"


def _files_merge_artist_album_links_to_winner(cur, *, winner_id: int, loser_ids: list[int]) -> None:
    if not loser_ids:
        return
    cur.execute(
        """
        WITH loser_links AS (
            SELECT
                album_id,
                role,
                BOOL_OR(COALESCE(is_primary, FALSE)) AS is_primary
            FROM files_artist_album_links
            WHERE artist_id = ANY(%s)
            GROUP BY album_id, role
        )
        INSERT INTO files_artist_album_links (artist_id, album_id, role, is_primary, created_at, updated_at)
        SELECT %s, album_id, role, is_primary, NOW(), NOW()
        FROM loser_links
        ON CONFLICT (artist_id, album_id, role) DO UPDATE
        SET is_primary = COALESCE(files_artist_album_links.is_primary, FALSE) OR COALESCE(EXCLUDED.is_primary, FALSE),
            updated_at = NOW()
        """,
        (loser_ids, int(winner_id)),
    )
    cur.execute(
        "DELETE FROM files_artist_album_links WHERE artist_id = ANY(%s)",
        (loser_ids,),
    )


def _files_get_artist_alias_candidates(
    conn,
    *,
    artist_norm: str,
    artist_name: str,
    limit: int = 12,
) -> list[str]:
    canonical_norm = str(artist_norm or "").strip() or _norm_artist_key(str(artist_name or "").strip())
    base_name = " ".join(str(artist_name or "").split()).strip()
    out: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        clean = " ".join(str(value or "").split()).strip()
        if not clean:
            return
        key = _norm_artist_key(clean)
        if not key or key in seen:
            return
        seen.add(key)
        out.append(clean)

    _push(base_name)
    for value in _split_artist_entities_for_profiles(base_name):
        _push(value)
    if conn is not None and canonical_norm:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT alias
                    FROM files_artist_aliases
                    WHERE artist_name_norm = %s
                    ORDER BY is_canonical DESC, length(alias) DESC, alias ASC
                    LIMIT %s
                    """,
                    (canonical_norm, max(4, int(limit or 12))),
                )
                for (alias,) in cur.fetchall():
                    _push(str(alias or "").strip())
        except Exception:
            logging.debug("Artist alias candidate fetch failed for %s", canonical_norm, exc_info=True)
    return out[: max(1, int(limit or 12))]


def _files_upsert_artist_external_aliases(
    conn,
    *,
    artist_id: int,
    artist_norm: str,
    artist_name: str,
    aliases: list[str] | tuple[str, ...],
    entity_kind: str = "",
    roles_json: Any = None,
    source: str = "musicbrainz",
) -> None:
    if conn is None or int(artist_id or 0) <= 0:
        return
    clean_aliases = [" ".join(str(value or "").split()).strip() for value in (aliases or [])]
    clean_aliases = [value for value in clean_aliases if value]
    if not clean_aliases:
        return
    alias_rows = _files_artist_alias_rows_for_identity(
        artist_name=artist_name,
        artist_norm=artist_norm,
        canonical_name=artist_name,
        entity_kind=entity_kind,
        roles_json=roles_json,
        aliases_json=clean_aliases,
    )
    if not alias_rows:
        return
    with conn.cursor() as cur:
        for row in alias_rows:
            cur.execute(
                """
                INSERT INTO files_artist_aliases (
                    artist_id,
                    artist_name_norm,
                    alias,
                    alias_norm,
                    alias_signature,
                    is_canonical,
                    source,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (artist_id, alias_norm)
                DO UPDATE SET
                    alias_signature = EXCLUDED.alias_signature,
                    is_canonical = files_artist_aliases.is_canonical OR EXCLUDED.is_canonical,
                    source = CASE
                        WHEN files_artist_aliases.source = 'canonical' THEN files_artist_aliases.source
                        ELSE EXCLUDED.source
                    END,
                    updated_at = NOW()
                """,
                (
                    int(artist_id),
                    artist_norm,
                    str(row.get("alias") or "").strip(),
                    str(row.get("alias_norm") or "").strip(),
                    str(row.get("alias_signature") or "").strip(),
                    bool(row.get("is_canonical")),
                    source,
                ),
            )

_ORIGINAL_EXTRACTED_FUNCTIONS = {
    "_library_artist_display_name": _library_artist_display_name,
    "_musicbrainz_artist_identity_lookup_cached": _musicbrainz_artist_identity_lookup_cached,
    "_files_merge_artist_alias_values": _files_merge_artist_alias_values,
    "_files_upsert_artist_canonical_identity": _files_upsert_artist_canonical_identity,
    "_files_artist_alias_rows_for_identity": _files_artist_alias_rows_for_identity,
    "_files_sync_artist_aliases": _files_sync_artist_aliases,
    "_files_backfill_artist_alias_table": _files_backfill_artist_alias_table,
    "_files_best_person_entity_kind": _files_best_person_entity_kind,
    "_files_merge_artist_album_links_to_winner": _files_merge_artist_album_links_to_winner,
    "_files_get_artist_alias_candidates": _files_get_artist_alias_candidates,
    "_files_upsert_artist_external_aliases": _files_upsert_artist_external_aliases,
}


def _library_artist_display_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _library_artist_display_name(*args, **kwargs)


def _musicbrainz_artist_identity_lookup_cached_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _musicbrainz_artist_identity_lookup_cached(*args, **kwargs)


def _files_merge_artist_alias_values_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_merge_artist_alias_values(*args, **kwargs)


def _files_upsert_artist_canonical_identity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_upsert_artist_canonical_identity(*args, **kwargs)


def _files_artist_alias_rows_for_identity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_artist_alias_rows_for_identity(*args, **kwargs)


def _files_sync_artist_aliases_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_sync_artist_aliases(*args, **kwargs)


def _files_backfill_artist_alias_table_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_backfill_artist_alias_table(*args, **kwargs)


def _files_best_person_entity_kind_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_best_person_entity_kind(*args, **kwargs)


def _files_merge_artist_album_links_to_winner_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_merge_artist_album_links_to_winner(*args, **kwargs)


def _files_get_artist_alias_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_get_artist_alias_candidates(*args, **kwargs)


def _files_upsert_artist_external_aliases_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_upsert_artist_external_aliases(*args, **kwargs)
