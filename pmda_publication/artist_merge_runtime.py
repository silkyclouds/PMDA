"""Runtime-owned artist/person merge maintenance for Files publication."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any


_LOCAL_NAMES = {
    '_bind_runtime',
    '_files_merge_duplicate_person_artists',
    '_files_merge_duplicate_person_artists_for_runtime',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value

def _files_merge_duplicate_person_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_merge_duplicate_person_artists(*args, **kwargs)


def _files_merge_duplicate_person_artists(conn, *, force: bool = False) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(value, '') FROM files_index_meta WHERE key = 'artist_person_merge_schema' LIMIT 1")
            row = cur.fetchone()
            if (not force) and row and str(row[0] or "").strip() == "v6":
                return
            cur.execute(
                """
                SELECT
                    a.id,
                    COALESCE(a.name, ''),
                    COALESCE(a.name_norm, ''),
                    COALESCE(a.canonical_name, ''),
                    COALESCE(a.canonical_name_norm, ''),
                    COALESCE(a.canonical_mbid, ''),
                    COALESCE(a.entity_kind, 'artist'),
                    COALESCE(a.roles_json, '[]'),
                    COALESCE(a.aliases_json, '[]'),
                    COALESCE(a.has_image, FALSE),
                    COALESCE(a.image_path, '')
                FROM files_artists a
                """
            )
            raw_rows = cur.fetchall()
            cur.execute(
                """
                SELECT artist_id, COALESCE(alias, ''), COALESCE(alias_norm, '')
                FROM files_artist_aliases
                """
            )
            alias_rows = cur.fetchall()
    except Exception:
        logging.debug("Artist person merge preload failed", exc_info=True)
        return

    alias_norms_by_id: dict[int, set[str]] = defaultdict(set)
    alias_values_by_id: dict[int, list[str]] = defaultdict(list)
    for artist_id, alias_value, alias_norm in alias_rows:
        aid = int(artist_id or 0)
        alias_txt = " ".join(str(alias_value or "").split()).strip()
        alias_key = str(alias_norm or "").strip() or _norm_artist_key(alias_txt)
        if aid <= 0 or not alias_key:
            continue
        alias_norms_by_id[aid].add(alias_key)
        if alias_txt:
            alias_values_by_id[aid].append(alias_txt)

    person_rows: list[dict[str, Any]] = []
    for artist_id, name, name_norm, canonical_name, canonical_name_norm, canonical_mbid, entity_kind, roles_json, aliases_json, has_image, image_path in raw_rows:
        aid = int(artist_id or 0)
        role_hints = _artist_role_hints_from_roles_json(roles_json or "[]")
        display_name = " ".join(str(name or "").split()).strip()
        canonical_name_txt = " ".join(str(canonical_name or "").split()).strip()
        person_like = bool(
            _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
            or _classical_person_alias_signature(canonical_name_txt or display_name)
            or bool(str(canonical_mbid or "").strip())
        )
        if aid <= 0 or not person_like:
            continue
        merged_aliases = _files_merge_artist_alias_values(
            [display_name, canonical_name_txt],
            aliases_json or [],
            alias_values_by_id.get(aid, []),
        )
        if person_like:
            merged_aliases = _collapse_classical_person_aliases(merged_aliases)
        alias_norms = {
            _norm_artist_key(value)
            for value in merged_aliases
            if _norm_artist_key(value)
        }
        alias_norms.add(str(name_norm or "").strip())
        if canonical_name_norm:
            alias_norms.add(str(canonical_name_norm or "").strip())
        person_rows.append(
            {
                "id": aid,
                "name": display_name,
                "name_norm": str(name_norm or "").strip(),
                "canonical_name": canonical_name_txt,
                "canonical_name_norm": str(canonical_name_norm or "").strip(),
                "canonical_mbid": str(canonical_mbid or "").strip(),
                "entity_kind": str(entity_kind or "artist").strip() or "artist",
                "role_hints": role_hints,
                "aliases": merged_aliases,
                "alias_norms": {value for value in alias_norms if value},
                "has_image": bool(has_image),
                "image_path": str(image_path or "").strip(),
            }
        )

    if not person_rows:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO files_index_meta(key, value, updated_at)
                    VALUES ('artist_person_merge_schema', 'v6', NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """
                )
        except Exception:
            logging.debug("Artist person merge schema stamp failed", exc_info=True)
        return

    rows_by_id = {int(row["id"]): row for row in person_rows}
    parent = {int(row["id"]): int(row["id"]) for row in person_rows}

    def _find(value: int) -> int:
        root = int(parent.get(value, value))
        while root != int(parent.get(root, root)):
            root = int(parent.get(root, root))
        while value != root:
            nxt = int(parent.get(value, value))
            parent[value] = root
            value = nxt
        return root

    def _union(left: int, right: int) -> None:
        lroot = _find(left)
        rroot = _find(right)
        if lroot != rroot:
            parent[rroot] = lroot

    buckets: dict[str, list[int]] = defaultdict(list)
    surname_buckets: dict[str, list[int]] = defaultdict(list)
    for row in person_rows:
        artist_id = int(row["id"])
        mbid = str(row.get("canonical_mbid") or "").strip()
        if mbid:
            buckets[f"mbid:{mbid}"].append(artist_id)
        for alias_norm in sorted(row.get("alias_norms") or []):
            buckets[f"alias:{alias_norm}"].append(artist_id)
        signature_source = str(row.get("canonical_name") or row.get("name") or "").strip()
        sig = _classical_person_alias_signature(signature_source)
        surname = str(sig.get("surname") or "").strip()
        if surname:
            surname_buckets[surname].append(artist_id)

    for ids in buckets.values():
        if len(ids) < 2:
            continue
        anchor = int(ids[0])
        for candidate in ids[1:]:
            _union(anchor, int(candidate))

    for ids in surname_buckets.values():
        unique_ids = sorted({int(value) for value in ids if int(value or 0) > 0})
        if len(unique_ids) < 2:
            continue
        for idx, left_id in enumerate(unique_ids):
            left_row = rows_by_id.get(left_id) or {}
            left_names = [
                str(left_row.get("canonical_name") or left_row.get("name") or "").strip(),
                *[str(value or "").strip() for value in (left_row.get("aliases") or []) if str(value or "").strip()],
            ]
            for right_id in unique_ids[idx + 1 :]:
                right_row = rows_by_id.get(right_id) or {}
                right_names = [
                    str(right_row.get("canonical_name") or right_row.get("name") or "").strip(),
                    *[str(value or "").strip() for value in (right_row.get("aliases") or []) if str(value or "").strip()],
                ]
                if any(
                    _classical_person_names_equivalent(left_name, right_name)
                    for left_name in left_names
                    for right_name in right_names
                    if left_name and right_name
                ):
                    _union(left_id, right_id)

    clusters: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in person_rows:
        clusters[_find(int(row["id"]))].append(row)

    merged_groups = [group for group in clusters.values() if len(group) > 1]
    if not merged_groups:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO files_index_meta(key, value, updated_at)
                    VALUES ('artist_person_merge_schema', 'v6', NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """
                )
        except Exception:
            logging.debug("Artist person merge schema stamp failed", exc_info=True)
        return

    touched_norms: set[str] = set()
    changed = False
    with conn.transaction():
        for group in merged_groups:
            ranked = sorted(
                group,
                key=lambda row: (
                    1 if str(row.get("canonical_mbid") or "").strip() else 0,
                    _identity_display_quality_score(str(row.get("canonical_name") or row.get("name") or "")),
                    1 if bool(row.get("has_image")) and str(row.get("image_path") or "").strip() else 0,
                    -int(row.get("id") or 0),
                ),
                reverse=True,
            )
            winner = dict(ranked[0])
            loser_ids = [int(row["id"]) for row in ranked[1:] if int(row["id"]) != int(winner["id"])]
            if not loser_ids:
                continue
            changed = True
            all_aliases: list[str] = []
            all_entity_kinds: list[str] = []
            all_roles: list[str] = []
            chosen_name = str(winner.get("canonical_name") or winner.get("name") or "").strip()
            chosen_mbid = str(winner.get("canonical_mbid") or "").strip()
            promoted_image_path = str(winner.get("image_path") or "").strip()
            promoted_has_image = bool(winner.get("has_image")) and bool(promoted_image_path)
            for row in ranked:
                all_entity_kinds.append(str(row.get("entity_kind") or "").strip())
                all_roles.extend([str(value or "").strip() for value in (row.get("role_hints") or []) if str(value or "").strip()])
                all_aliases.extend([str(value or "").strip() for value in (row.get("aliases") or []) if str(value or "").strip()])
                row_name = str(row.get("canonical_name") or row.get("name") or "").strip()
                chosen_name = _choose_preferred_person_identity_name(chosen_name, row_name)
                if not chosen_mbid:
                    chosen_mbid = str(row.get("canonical_mbid") or "").strip()
                row_image_path = str(row.get("image_path") or "").strip()
                if (not promoted_has_image) and row_image_path:
                    promoted_image_path = row_image_path
                    promoted_has_image = bool(row.get("has_image")) or _existing_file_path(row_image_path)
            merged_aliases = _files_merge_artist_alias_values(all_aliases, [chosen_name])
            merged_aliases = _collapse_classical_person_aliases(merged_aliases)
            chosen_name = _select_classical_person_display_name(
                current_name=str(winner.get("name") or "").strip(),
                primary_name=chosen_name,
                aliases=merged_aliases,
            ) or chosen_name
            merged_roles = sorted({value.lower() for value in all_roles if value}, key=lambda role: (_FILES_BROWSE_ROLE_PRIORITY.get(role, 99), role))
            merged_entity_kind = _files_best_person_entity_kind(all_entity_kinds, merged_roles)
            winner_norm = str(winner.get("name_norm") or "").strip()
            touched_norms.add(winner_norm)
            for row in ranked[1:]:
                touched_norms.add(str(row.get("name_norm") or "").strip())
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE files_artists
                    SET name = %s,
                        canonical_name = %s,
                        canonical_name_norm = %s,
                        canonical_mbid = COALESCE(NULLIF(%s, ''), canonical_mbid),
                        entity_kind = %s,
                        roles_json = %s,
                        aliases_json = %s,
                        has_image = %s,
                        image_path = CASE
                            WHEN %s <> '' THEN %s
                            ELSE image_path
                        END,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        chosen_name,
                        chosen_name,
                        _norm_artist_key(chosen_name),
                        chosen_mbid,
                        merged_entity_kind,
                        json.dumps(merged_roles, ensure_ascii=False),
                        json.dumps(merged_aliases, ensure_ascii=False),
                        bool(promoted_has_image),
                        promoted_image_path,
                        promoted_image_path,
                        int(winner["id"]),
                    ),
                )
                cur.execute("UPDATE files_albums SET artist_id = %s, updated_at = NOW() WHERE artist_id = ANY(%s)", (int(winner["id"]), loser_ids))
                _files_merge_artist_album_links_to_winner(cur, winner_id=int(winner["id"]), loser_ids=loser_ids)
                cur.execute("DELETE FROM files_artist_aliases WHERE artist_id = ANY(%s)", (loser_ids,))
                cur.execute("DELETE FROM files_artists WHERE id = ANY(%s)", (loser_ids,))

    if changed:
        try:
            _files_sync_artist_aliases(conn, artist_norms=[norm for norm in touched_norms if norm])
            touched_payloads: dict[str, dict[str, Any]] = {}
            for row in rows_by_id.values():
                norm = str(row.get("name_norm") or "").strip()
                if not norm or norm not in touched_norms or norm in touched_payloads:
                    continue
                touched_payloads[norm] = {
                    "name": str(row.get("canonical_name") or row.get("name") or "").strip(),
                    "aliases_json": list(row.get("aliases") or []),
                }
            artist_rows = _files_refresh_artist_media_map_from_db(
                touched_payloads
            )
            _files_promote_artist_alias_cache(conn, artist_rows)
        except Exception:
            logging.debug("Artist person merge alias/media promotion failed", exc_info=True)
        _files_cache_invalidate_all()
        logging.info("Merged %d duplicate classical person artist bucket(s).", len(merged_groups))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files_index_meta(key, value, updated_at)
                VALUES ('artist_person_merge_schema', 'v6', NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """
            )
    except Exception:
        logging.debug("Artist person merge schema stamp failed", exc_info=True)
