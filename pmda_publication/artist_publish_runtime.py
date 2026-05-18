"""Runtime-backed artist publication row builder.

This module contains the heavy per-artist publication payload builder extracted
from ``pmda.py``. It still binds to the live runtime for tag parsing, cover
selection, incomplete assessment, and row upserts while publication is split into
smaller services.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one artist publication call."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        "_publish_files_library_artist_from_items",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def publish_files_library_artist_from_items_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> int:
    _bind_runtime(runtime)
    return _publish_files_library_artist_from_items(*args, **kwargs)


def _publish_files_library_artist_from_items(
    artist_name: str,
    items: list[dict],
    *,
    scan_id: int | None = None,
    results_by_album_id: dict[int, dict] | None = None,
) -> int:
    """
    Publish artist albums to files_library_published_albums after full per-artist flow.
    These rows are used as the source for progressive Files index rebuilds.
    """
    if not items:
        return 0
    candidates: list[dict] = []
    now = time.time()
    results_by_album_id = results_by_album_id or {}
    fast_publish_only = all(
        bool(item.get("_publication_reconcile_fast_publish"))
        for item in items
        if isinstance(item, dict)
    )
    pg_conn = None
    if not fast_publish_only:
        try:
            pg_conn = _files_pg_connect()
        except Exception:
            pg_conn = None
    try:
        for item in items:
            folder_raw = (item.get("folder") or "").strip()
            if not folder_raw:
                continue
            fast_publish = bool(item.get("_publication_reconcile_fast_publish"))
            try:
                folder = path_for_fs_access(Path(folder_raw))
                if not fast_publish and (not folder.exists() or not folder.is_dir()):
                    logging.info(
                        "Files publication skipped non-existent/moved album folder artist=%s folder=%s",
                        artist_name,
                        folder_raw,
                    )
                    continue
                track_entries = _files_build_track_entries_from_item(item, folder)
                if not track_entries:
                    logging.warning(
                        "Files publication skipped album with no track entries artist=%s folder=%s",
                        artist_name,
                        folder_raw,
                    )
                    continue
                first_audio = None
                try:
                    first_audio = path_for_fs_access(Path(track_entries[0]["file_path"]))
                except Exception:
                    first_audio = None
                tags = dict(item.get("meta") or {})
                if (not fast_publish) and first_audio and first_audio.exists():
                    live_tags = extract_tags(first_audio) or {}
                    if live_tags:
                        tags.update(live_tags)
                resolved_artist_from_item, resolved_title_from_item = _resolve_edition_display_identity(
                    item,
                    default_artist=str(artist_name or ""),
                    default_title=str(item.get("album_title") or item.get("title_raw") or folder.name or ""),
                    folder_name=folder.name,
                )
                title_fallback = (
                    resolved_title_from_item
                    or (item.get("album_title") or "").strip()
                    or (item.get("title_raw") or "").strip()
                    or folder.name.replace("_", " ").strip()
                    or "Unknown Album"
                )
                artist_fallback = resolved_artist_from_item or (item.get("artist") or "").strip() or (artist_name or "").strip() or "Unknown Artist"
                # The scan payload already carries the authoritative resolved display identity.
                # Publication must not regress back to stale embedded tags for adversarial
                # corpora such as:
                #   - Slowdive / Souvlaki files tagged as album "Takk..."
                #   - Sigur Rós / Von files tagged as album "Takk..."
                # Live tags are still used for year/genre/label/etc., but album/artist display
                # identity should stay on the scan-time resolution unless it is missing/generic.
                artist_resolved = str(resolved_artist_from_item or artist_fallback or "").strip()
                album_resolved = _sanitize_album_title_display(
                    str(resolved_title_from_item or title_fallback or "").strip()
                )
                if _identity_text_is_generic(artist_resolved):
                    artist_resolved = str(
                        _pick_album_artist_from_tag_dicts([tags], default=artist_fallback) or artist_fallback or "Unknown Artist"
                    ).strip() or "Unknown Artist"
                if _identity_text_is_generic(album_resolved):
                    album_resolved = _sanitize_album_title_display(
                        str(_pick_album_title_from_tag_dicts([tags], fallback=title_fallback) or title_fallback or "Unknown Album").strip()
                    )
                artist_resolved = str(artist_resolved or artist_fallback or "Unknown Artist").strip() or "Unknown Artist"
                album_resolved = _sanitize_album_title_display(
                    str(album_resolved or title_fallback or "Unknown Album").strip() or "Unknown Album"
                )
                label_resolved = _pick_album_label_from_tag_dicts([tags])
                artist_norm = _norm_artist_key(artist_resolved) or "unknown artist"
                title_norm = norm_album_for_dedup(album_resolved, normalize_parenthetical=True) or "unknown album"
                date_text = (tags.get("date") or tags.get("year") or "").strip()
                year = _parse_int_loose((date_text[:4] if date_text else tags.get("year")), 0) or None
                raw_genres = _split_genre_values(tags.get("genre") or "")
                inferred_genre = _infer_genre_from_bandcamp_tags(raw_genres) if raw_genres else None
                genre = inferred_genre if inferred_genre else ("; ".join(raw_genres[:6]) if raw_genres else "")
                fmt_counts: dict[str, int] = defaultdict(int)
                total_duration_sec = 0
                for tr in track_entries:
                    fmt_counts[(tr.get("format") or "UNKNOWN").upper()] += 1
                    total_duration_sec += int(tr.get("duration_sec") or 0)
                dominant_format = max(fmt_counts.items(), key=lambda x: x[1])[0] if fmt_counts else "UNKNOWN"
                if fast_publish:
                    artist_image_path = Path(str(item.get("artist_image_path") or "")) if str(item.get("artist_image_path") or "").strip() else None
                    has_artist_image = bool(artist_image_path)
                else:
                    artist_image_path = _files_effective_artist_image_path(
                        folder,
                        artist_resolved,
                        artist_norm,
                        conn=pg_conn,
                    )
                    has_artist_image = bool(artist_image_path and artist_image_path.is_file())
                indices = [int(t.get("track_num") or 0) for t in track_entries if int(t.get("track_num") or 0) > 0]
                actual_track_count = len(track_entries)
                initial_broken = bool(item.get("is_broken"))
                expected_track_count = _parse_int_loose(item.get("expected_track_count"), 0) or None
                missing_indices = list(item.get("missing_indices") or [])
                mb_hint = None
                if expected_track_count and expected_track_count > 0:
                    mb_hint = {"track_count": int(expected_track_count), "source": "publication_candidate"}
                if initial_broken and expected_track_count and not missing_indices:
                    missing_indices = _edition_missing_indices_exact(item, int(expected_track_count or 0), actual_track_count)
                if (not initial_broken) and indices:
                    max_idx = max(indices)
                    coverage = (actual_track_count / max_idx) if max_idx else 1.0
                    # Skip broken detection when track numbering is obviously corrupt.
                    if max_idx > max(120, actual_track_count * 3) and coverage < 0.5:
                        initial_broken = False
                        expected_track_count = None
                        missing_indices = []
                    else:
                        initial_broken, _actual_count_from_indices, gaps = _detect_gaps_in_indices(indices)
                        if initial_broken and _classical_gap_anomaly_should_be_ignored(
                            tags,
                            actual_count=actual_track_count,
                            max_idx=max_idx,
                            gaps=gaps,
                        ):
                            initial_broken = False
                            gaps = []
                        if initial_broken:
                            expected_track_count = max_idx
                            for start_i, end_i in gaps:
                                if (end_i - start_i) > 2000:
                                    continue
                                missing_indices.extend(list(range(start_i + 1, end_i)))
                                if len(missing_indices) > 5000:
                                    missing_indices = missing_indices[:5000]
                                    break
                album_id = _parse_int_loose(item.get("album_id"), 0)
                result = results_by_album_id.get(album_id, {})
                mb_release_group_id = (
                    (item.get("musicbrainz_id") or "").strip()
                    or _extract_musicbrainz_id_from_meta(tags)
                    or ""
                )
                mb_release_id = (
                    str(result.get("musicbrainz_release_id") or "").strip()
                    or str(item.get("musicbrainz_release_id") or "").strip()
                    or _extract_musicbrainz_release_id_from_meta(tags)
                    or ""
                )
                discogs_release_id = (
                    result.get("discogs_release_id")
                    or item.get("discogs_release_id")
                    or ""
                ).strip()
                lastfm_album_mbid = (
                    result.get("lastfm_album_mbid")
                    or item.get("lastfm_album_mbid")
                    or ""
                ).strip()
                bandcamp_album_url = (
                    result.get("bandcamp_album_url")
                    or item.get("bandcamp_album_url")
                    or ""
                ).strip()
                strict_match_verified = bool(
                    result.get("strict_match_verified")
                    if ("strict_match_verified" in result)
                    else item.get("strict_match_verified")
                )
                strict_match_provider = _normalize_identity_provider(
                    str(
                        result.get("strict_match_provider")
                        or item.get("strict_match_provider")
                        or ""
                    )
                )
                strict_reject_reason = str(
                    result.get("strict_reject_reason")
                    or item.get("strict_reject_reason")
                    or ""
                ).strip()
                try:
                    strict_tracklist_score = float(
                        result.get("strict_tracklist_score")
                        if ("strict_tracklist_score" in result)
                        else item.get("strict_tracklist_score")
                    )
                except Exception:
                    strict_tracklist_score = 0.0
                missing_required = _check_required_tags(
                    tags,
                    REQUIRED_TAGS,
                    edition={"tracks": [{"title": t.get("title"), "index": t.get("track_num")} for t in track_entries]},
                )
                pmda_provider = (tags.get(PMDA_MATCH_PROVIDER_TAG) or "").strip()
                primary_metadata_source = (
                    (
                        strict_match_provider
                        or result.get("provider_used")
                        or result.get("pmda_match_provider")
                        or item.get("primary_metadata_source")
                        or item.get("metadata_source")
                        or pmda_provider
                        or ""
                    ).strip()
                )
                assessment = _build_incomplete_assessment(
                    edition=item,
                    tags=tags,
                    mb_hint=mb_hint,
                    is_broken_detected=bool(initial_broken),
                    expected_track_count=_parse_int_loose(expected_track_count, 0),
                    actual_track_count=int(actual_track_count or 0),
                    missing_indices=list(missing_indices or []),
                    strict_reject_reason=str(strict_reject_reason or ""),
                )
                is_broken = bool((assessment or {}).get("mark_broken"))
                expected_track_count = _parse_int_loose((assessment or {}).get("expected_track_count"), 0) or None
                missing_indices = list((assessment or {}).get("missing_indices") or [])
                current_cover_provider = _cover_provider_from_primary_tags_blob(item.get("primary_tags_json"))
                cover_path_resolved, has_cover, cover_provider = _authoritative_publication_cover(
                    folder=folder,
                    item=item,
                    result=result,
                    tags=tags,
                    artist_resolved=artist_resolved,
                    album_resolved=album_resolved,
                    strict_match_verified=bool(strict_match_verified),
                    strict_match_provider=str(strict_match_provider or ""),
                    metadata_source=str(primary_metadata_source or ""),
                    musicbrainz_release_group_id=mb_release_group_id,
                    musicbrainz_release_id=mb_release_id,
                    discogs_release_id=discogs_release_id,
                    lastfm_album_mbid=lastfm_album_mbid,
                    bandcamp_album_url=bandcamp_album_url,
                    current_cover_path=str(item.get("cover_path") or "").strip(),
                    current_cover_provider=current_cover_provider,
                )
                primary_tags_authoritative = _authoritative_primary_tags_for_publication(
                    tags=tags,
                    artist_resolved=artist_resolved,
                    album_resolved=album_resolved,
                    year=year,
                    genre=genre or "",
                    label=(label_resolved or "").strip(),
                    metadata_source=str(primary_metadata_source or ""),
                    musicbrainz_release_group_id=mb_release_group_id,
                    musicbrainz_release_id=mb_release_id,
                    discogs_release_id=discogs_release_id,
                    lastfm_album_mbid=lastfm_album_mbid,
                    bandcamp_album_url=bandcamp_album_url,
                    strict_match_verified=bool(strict_match_verified),
                    cover_provider=cover_provider,
                )
                source_id = int(_source_id_for_path(folder) or 0)
                row_payload = {
                        "folder_path": _album_folder_cache_key(folder),
                        "scan_id": scan_id,
                        "source_id": source_id if source_id > 0 else None,
                        "artist_name": artist_resolved,
                        "artist_norm": artist_norm,
                        "album_title": album_resolved,
                        "title_norm": title_norm,
                        "year": year,
                        "date_text": date_text[:32] if date_text else "",
                        "genre": genre or "",
                        "label": (label_resolved or "").strip(),
                        "tags_json": json.dumps(raw_genres[:20]),
                        "format": dominant_format,
                        "is_lossless": dominant_format in _LOSSLESS_FORMATS,
                        "has_cover": has_cover,
                        "cover_path": cover_path_resolved,
                        "has_artist_image": has_artist_image,
                        "artist_image_path": str(artist_image_path) if artist_image_path else "",
                        "mb_identified": bool(mb_release_group_id or mb_release_id),
                        "strict_match_verified": bool(strict_match_verified),
                        "strict_match_provider": strict_match_provider,
                        "strict_reject_reason": strict_reject_reason,
                        "strict_tracklist_score": strict_tracklist_score,
                        "musicbrainz_release_group_id": mb_release_group_id,
                        "musicbrainz_release_id": mb_release_id,
                        "discogs_release_id": discogs_release_id,
                        "lastfm_album_mbid": lastfm_album_mbid,
                        "bandcamp_album_url": bandcamp_album_url,
                        "primary_metadata_source": str(primary_metadata_source or ""),
                        "track_count": actual_track_count,
                        "total_duration_sec": total_duration_sec,
                        "is_broken": bool(is_broken),
                        "expected_track_count": expected_track_count,
                        "actual_track_count": actual_track_count,
                        "missing_indices_json": json.dumps(missing_indices),
                        "missing_required_tags_json": json.dumps(missing_required),
                        "incomplete_classification": str((assessment or {}).get("verdict") or ""),
                        "incomplete_confidence": float((assessment or {}).get("confidence") or 0.0),
                        "incomplete_quarantine_eligible": 1 if bool((assessment or {}).get("quarantine_eligible")) else 0,
                        "incomplete_evidence_json": json.dumps(assessment or {}, default=str),
                        "primary_tags_json": json.dumps(primary_tags_authoritative, default=str),
                        "tracks_json": json.dumps(track_entries),
                        "fingerprint": (item.get("fingerprint") or "").strip(),
                        "updated_at": now,
                    }
                candidates.append({"item": dict(item or {}), "row": row_payload})
            except Exception as e:
                logging.warning(
                    "Files publication skipped album artist=%s album=%s folder=%s: %s",
                    artist_name,
                    str(item.get('album_title') or item.get('title_raw') or '').strip() or 'Unknown Album',
                    folder_raw,
                    e,
                    exc_info=True,
                )
                continue
    finally:
        try:
            if pg_conn is not None:
                pg_conn.close()
        except Exception:
            pass
    rows, hidden_folder_paths = _collapse_files_publication_candidates(artist_name, candidates)
    if hidden_folder_paths:
        _delete_files_library_published_rows(hidden_folder_paths)
    _apply_genre_defaults_to_albums_payload(rows)
    inserted = _upsert_files_library_published_rows(rows)
    if inserted:
        logging.debug("Published %d album(s) for artist '%s' to files_library_published_albums", inserted, artist_name)
    return inserted
