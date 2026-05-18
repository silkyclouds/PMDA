"""Runtime-owned publication row helpers for Files library publishing.

This module contains the heavy publication row construction, published-row
upsert, candidate collapse, scan-edition conversion, and per-scan publication
rebuild helpers extracted from ``pmda.py``. It binds the live PMDA runtime at
the call boundary so the existing scan/publication pipeline keeps the same
public behavior while the monolith is reduced.
"""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    '_files_track_value',
    '_files_build_track_entries_from_item',
    '_upsert_files_library_published_rows',
    '_filter_existing_files_album_items',
    '_normalize_bandcamp_album_ref',
    '_strict_album_identity_key',
    '_strict_album_identity_key_for_edition',
    '_files_publication_candidate_score',
    '_collapse_files_publication_candidates',
    '_delete_files_library_published_rows',
    '_files_publication_rewrite_path_prefix',
    '_files_publication_rewrite_tracks_json',
    '_files_publication_remap_published_row',
    '_files_publication_load_published_rows_by_folder',
    '_files_live_publish_batches',
    '_publish_files_library_artist_live_batches',
    '_rebuild_files_publication_for_scan',
    '_files_publication_scan_move_maps',
    '_files_publication_candidate_existing_path',
    '_scan_edition_row_to_publication_item',
}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            continue
        if name == "_bind_runtime" or (name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES):
            continue
        globals()[name] = value


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("Publication row runtime is not bound")
    return _RUNTIME

def files_track_value_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_track_value(*args, **kwargs)

def files_build_track_entries_from_item_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_build_track_entries_from_item(*args, **kwargs)

def upsert_files_library_published_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _upsert_files_library_published_rows(*args, **kwargs)

def filter_existing_files_album_items_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _filter_existing_files_album_items(*args, **kwargs)

def normalize_bandcamp_album_ref_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_bandcamp_album_ref(*args, **kwargs)

def strict_album_identity_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_album_identity_key(*args, **kwargs)

def strict_album_identity_key_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_album_identity_key_for_edition(*args, **kwargs)

def files_publication_candidate_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_candidate_score(*args, **kwargs)

def collapse_files_publication_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _collapse_files_publication_candidates(*args, **kwargs)

def delete_files_library_published_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _delete_files_library_published_rows(*args, **kwargs)

def files_publication_rewrite_path_prefix_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_rewrite_path_prefix(*args, **kwargs)

def files_publication_rewrite_tracks_json_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_rewrite_tracks_json(*args, **kwargs)

def files_publication_remap_published_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_remap_published_row(*args, **kwargs)

def files_publication_load_published_rows_by_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_load_published_rows_by_folder(*args, **kwargs)

def files_live_publish_batches_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_live_publish_batches(*args, **kwargs)

def publish_files_library_artist_live_batches_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _publish_files_library_artist_live_batches(*args, **kwargs)

def rebuild_files_publication_for_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _rebuild_files_publication_for_scan(*args, **kwargs)

def files_publication_scan_move_maps_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_scan_move_maps(*args, **kwargs)

def files_publication_candidate_existing_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _files_publication_candidate_existing_path(*args, **kwargs)

def scan_edition_row_to_publication_item_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_edition_row_to_publication_item(*args, **kwargs)


def _files_track_value(track_obj, attr: str, fallback=None):
    if track_obj is None:
        return fallback
    if isinstance(track_obj, dict):
        if attr in track_obj:
            return track_obj.get(attr)
        if attr == "idx":
            return (
                track_obj.get("idx")
                or track_obj.get("index")
                or track_obj.get("track_num")
                or track_obj.get("track")
            )
        if attr == "disc":
            return track_obj.get("disc") or track_obj.get("disc_num")
        if attr == "dur":
            return (
                track_obj.get("dur")
                or track_obj.get("duration")
                or track_obj.get("duration_ms")
                or track_obj.get("duration_sec")
            )
        return fallback
    return getattr(track_obj, attr, fallback)


def _files_build_track_entries_from_item(item: dict, folder: Path) -> list[dict]:
    reconcile_fast = bool(item.get("_publication_reconcile_fast_publish"))
    fast_publish = bool(reconcile_fast and item.get("ordered_paths"))
    if fast_publish:
        ordered_paths = [
            Path(str(raw))
            for raw in (item.get("ordered_paths") or [])
            if str(raw or "").strip() and AUDIO_RE.search(Path(str(raw)).name)
        ]
    else:
        ordered_paths = _files_collect_ordered_audio_paths(folder, item.get("ordered_paths") or [])
    fallback_tracks = list(item.get("tracks") or [])
    meta_tags = dict(item.get("meta") or {})
    br = _parse_int_loose(item.get("br"), 0)
    sr = _parse_int_loose(item.get("sr"), 0)
    bd = _parse_int_loose(item.get("bd"), 0)
    artist_hint = str(item.get("artist") or meta_tags.get("artist") or "").strip()
    album_hint = str(item.get("title_raw") or item.get("album_title") or meta_tags.get("album") or folder.name or "").strip()
    out: list[dict] = []
    for idx, p in enumerate(ordered_paths):
        track_obj = fallback_tracks[idx] if idx < len(fallback_tracks) else None
        raw_title = (_files_track_value(track_obj, "title", "") or "").strip() or p.stem or f"Track {idx + 1}"
        disc_num = _parse_int_loose(_files_track_value(track_obj, "disc", 1), 1) or 1
        track_num = _parse_int_loose(_files_track_value(track_obj, "idx", idx + 1), idx + 1) or (idx + 1)
        display = _track_display_fields_from_sources(
            raw_title=raw_title,
            file_path=str(p),
            fallback_disc=disc_num,
            fallback_track=track_num,
            album_hint=album_hint,
            artist_hint=artist_hint,
        )
        title = str(display.get("display_title") or raw_title or f"Track {idx + 1}").strip()
        disc_num = int(display.get("display_disc_num") or disc_num or 1)
        track_num = int(display.get("display_track_num") or track_num or (idx + 1))
        raw_dur = _files_track_value(track_obj, "dur", 0)
        dur_sec = 0
        try:
            dur_num = float(raw_dur or 0)
            # Track helper uses milliseconds internally; accept seconds when obviously small.
            dur_sec = int(dur_num / 1000.0) if dur_num > 5000 else int(dur_num)
        except Exception:
            dur_sec = int(max(0.0, _parse_duration_seconds_loose(raw_dur, 0.0)))
        if dur_sec <= 0 and not bool(item.get("_publication_reconcile_skip_ffprobe")):
            try:
                dur_sec = int(max(0, _run_ffprobe_duration_sec(str(p)) or 0))
            except Exception:
                dur_sec = 0
        fmt = (p.suffix.lower().lstrip(".") or "UNKNOWN").upper()
        if reconcile_fast:
            file_size = 0
        else:
            try:
                file_size = int(p.stat().st_size)
            except OSError:
                file_size = 0
        out.append(
            {
                "file_path": str(p),
                "title": title,
                "disc_num": disc_num,
                "disc_label": str(display.get("display_disc_label") or "").strip(),
                "track_num": track_num,
                "duration_sec": max(0, dur_sec),
                "format": fmt,
                "bitrate": br,
                "sample_rate": sr,
                "bit_depth": bd,
                "file_size_bytes": file_size,
            }
        )
    metadata_source = _normalize_identity_provider(
        str(
            item.get("strict_match_provider")
            or item.get("primary_metadata_source")
            or item.get("metadata_source")
            or (item.get("meta") or {}).get(PMDA_MATCH_PROVIDER_TAG)
            or ""
        )
    )
    edition_payload = dict(item or {})
    mb_release_id = _extract_musicbrainz_release_id_from_meta(meta_tags)
    if mb_release_id:
        edition_payload.setdefault("musicbrainz_release_id", mb_release_id)
        edition_payload.setdefault("musicbrainz_albumid", mb_release_id)
    mb_release_group_id = (
        str(item.get("musicbrainz_id") or item.get("musicbrainz_release_group_id") or "").strip()
        or _extract_musicbrainz_id_from_meta(meta_tags)
    )
    if mb_release_group_id:
        edition_payload.setdefault("musicbrainz_id", mb_release_group_id)
        edition_payload.setdefault("musicbrainz_release_group_id", mb_release_group_id)
    provider_track_titles = []
    if not reconcile_fast:
        provider_track_titles = _provider_track_titles_cached(
            artist_name=artist_hint,
            album_title=album_hint,
            metadata_source=metadata_source,
            musicbrainz_release_group_id=mb_release_group_id,
            discogs_release_id=str(item.get("discogs_release_id") or "").strip(),
            lastfm_album_mbid=str(item.get("lastfm_album_mbid") or "").strip(),
            bandcamp_album_url=str(item.get("bandcamp_album_url") or "").strip(),
            edition_payload=edition_payload,
        )
    out.sort(key=lambda t: (int(t.get("disc_num") or 1), int(t.get("track_num") or 0), str(t.get("file_path") or "")))
    if provider_track_titles and len(provider_track_titles) == len(out):
        for i, title in enumerate(provider_track_titles):
            cleaned = str(title or "").strip()
            if not cleaned:
                continue
            out[i]["title"] = cleaned
    return out


def _upsert_files_library_published_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    filtered = [r for r in rows if (r.get("folder_path") or "").strip()]
    if not filtered:
        return 0
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
        cur = con.cursor()
        cur.executemany(
            """
            INSERT INTO files_library_published_albums (
                folder_path, scan_id, artist_name, artist_norm, album_title, title_norm,
                year, date_text, genre, label, tags_json, format, is_lossless,
                has_cover, cover_path, has_artist_image, artist_image_path,
                mb_identified, strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score,
                musicbrainz_release_group_id, musicbrainz_release_id, discogs_release_id, lastfm_album_mbid,
                bandcamp_album_url, primary_metadata_source, track_count, total_duration_sec,
                is_broken, expected_track_count, actual_track_count, missing_indices_json,
                missing_required_tags_json, primary_tags_json, tracks_json, fingerprint, source_id, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?
            )
            ON CONFLICT(folder_path) DO UPDATE SET
                scan_id=excluded.scan_id,
                artist_name=excluded.artist_name,
                artist_norm=excluded.artist_norm,
                album_title=excluded.album_title,
                title_norm=excluded.title_norm,
                year=excluded.year,
                date_text=excluded.date_text,
                genre=excluded.genre,
                label=excluded.label,
                tags_json=excluded.tags_json,
                format=excluded.format,
                is_lossless=excluded.is_lossless,
                has_cover=excluded.has_cover,
                cover_path=excluded.cover_path,
                has_artist_image=excluded.has_artist_image,
                artist_image_path=excluded.artist_image_path,
                mb_identified=excluded.mb_identified,
                strict_match_verified=excluded.strict_match_verified,
                strict_match_provider=excluded.strict_match_provider,
                strict_reject_reason=excluded.strict_reject_reason,
                strict_tracklist_score=excluded.strict_tracklist_score,
                musicbrainz_release_group_id=excluded.musicbrainz_release_group_id,
                musicbrainz_release_id=excluded.musicbrainz_release_id,
                discogs_release_id=excluded.discogs_release_id,
                lastfm_album_mbid=excluded.lastfm_album_mbid,
                bandcamp_album_url=excluded.bandcamp_album_url,
                primary_metadata_source=excluded.primary_metadata_source,
                track_count=excluded.track_count,
                total_duration_sec=excluded.total_duration_sec,
                is_broken=excluded.is_broken,
                expected_track_count=excluded.expected_track_count,
                actual_track_count=excluded.actual_track_count,
                missing_indices_json=excluded.missing_indices_json,
                missing_required_tags_json=excluded.missing_required_tags_json,
                primary_tags_json=excluded.primary_tags_json,
                tracks_json=excluded.tracks_json,
                fingerprint=excluded.fingerprint,
                source_id=excluded.source_id,
                updated_at=excluded.updated_at
            """,
            [
                (
                    r.get("folder_path") or "",
                    r.get("scan_id"),
                    r.get("artist_name") or "Unknown Artist",
                    r.get("artist_norm") or "unknown artist",
                    r.get("album_title") or "Unknown Album",
                    r.get("title_norm") or "unknown album",
                    r.get("year"),
                    r.get("date_text") or "",
                    r.get("genre") or "",
                    r.get("label") or "",
                    r.get("tags_json") or "[]",
                    r.get("format") or "",
                    1 if r.get("is_lossless") else 0,
                    1 if r.get("has_cover") else 0,
                    r.get("cover_path") or "",
                    1 if r.get("has_artist_image") else 0,
                    r.get("artist_image_path") or "",
                    1 if r.get("mb_identified") else 0,
                    1 if r.get("strict_match_verified") else 0,
                    _normalize_identity_provider(str(r.get("strict_match_provider") or "")),
                    str(r.get("strict_reject_reason") or "").strip(),
                    float(r.get("strict_tracklist_score") or 0.0),
                    r.get("musicbrainz_release_group_id") or "",
                    r.get("musicbrainz_release_id") or "",
                    r.get("discogs_release_id") or "",
                    r.get("lastfm_album_mbid") or "",
                    r.get("bandcamp_album_url") or "",
                    r.get("primary_metadata_source") or "",
                    int(r.get("track_count") or 0),
                    int(r.get("total_duration_sec") or 0),
                    1 if r.get("is_broken") else 0,
                    r.get("expected_track_count"),
                    int(r.get("actual_track_count") or 0),
                    r.get("missing_indices_json") or "[]",
                    r.get("missing_required_tags_json") or "[]",
                    r.get("primary_tags_json") or "{}",
                    r.get("tracks_json") or "[]",
                    r.get("fingerprint") or "",
                    (_parse_int_loose(r.get("source_id"), 0) or None),
                    float(r.get("updated_at") or time.time()),
                )
                for r in filtered
            ],
        )
        con.commit()
        con.close()
        return len(filtered)
    except Exception:
        logging.debug("Failed to upsert files_library_published_albums rows", exc_info=True)
        return 0


def _filter_existing_files_album_items(
    items: list[dict] | None,
    *,
    context: str = "",
    artist_name: str = "",
) -> tuple[list[dict], int]:
    filtered: list[dict] = []
    skipped = 0
    for item in items or []:
        folder_raw = str((item or {}).get("folder") or "").strip()
        if not folder_raw:
            skipped += 1
            continue
        try:
            folder = path_for_fs_access(Path(folder_raw))
        except Exception:
            skipped += 1
            continue
        if not folder.exists() or not folder.is_dir():
            skipped += 1
            continue
        filtered.append(item)
    if skipped:
        logging.info(
            "Files album items filtered missing folders%s%s: kept=%d skipped=%d",
            f" context={context}" if context else "",
            f" artist={artist_name}" if artist_name else "",
            len(filtered),
            skipped,
        )
    return filtered, skipped


def _normalize_bandcamp_album_ref(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        path = re.sub(r"/+", "/", str(parsed.path or "/")).rstrip("/")
        return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", "", ""))
    except Exception:
        return raw.rstrip("/").lower()


def _strict_album_identity_key(
    *,
    artist_name: str,
    album_title: str,
    strict_match_verified: bool,
    musicbrainz_release_group_id: str = "",
    musicbrainz_release_id: str = "",
    discogs_release_id: str = "",
    lastfm_album_mbid: str = "",
    bandcamp_album_url: str = "",
) -> str:
    if not bool(strict_match_verified):
        return ""
    artist_norm = _norm_artist_key(str(artist_name or "").strip())
    title_norm = norm_album_for_dedup(str(album_title or "").strip(), normalize_parenthetical=True)
    # Publication and duplicate resolution need one canonical "same album" key even when
    # strict matches were proven through different providers. If we prioritize provider ids
    # first, the same album can survive multiple times in browse just because one edition
    # strict-matched via Discogs and another via Last.fm. Canonical artist/title must win.
    if artist_norm and title_norm:
        return f"strict-title:{artist_norm}:{title_norm}"
    if str(musicbrainz_release_group_id or "").strip():
        return f"mb-rg:{str(musicbrainz_release_group_id or '').strip()}"
    if str(musicbrainz_release_id or "").strip():
        return f"mb-rel:{str(musicbrainz_release_id or '').strip()}"
    if str(discogs_release_id or "").strip():
        return f"discogs:{str(discogs_release_id or '').strip()}"
    if str(lastfm_album_mbid or "").strip():
        return f"lastfm:{str(lastfm_album_mbid or '').strip()}"
    bandcamp_ref = _normalize_bandcamp_album_ref(str(bandcamp_album_url or ""))
    if bandcamp_ref:
        return f"bandcamp:{bandcamp_ref}"
    return ""


def _strict_album_identity_key_for_edition(
    edition: dict | None,
    *,
    default_artist: str = "",
    default_title: str = "",
) -> str:
    e = edition if isinstance(edition, dict) else {}
    folder_name = ""
    try:
        folder_name = Path(str(e.get("folder") or "")).name
    except Exception:
        folder_name = ""
    artist_name, album_title = _resolve_edition_display_identity(
        e,
        default_artist=str(default_artist or "").strip(),
        default_title=str(default_title or "").strip(),
        folder_name=folder_name,
    )
    return _strict_album_identity_key(
        artist_name=artist_name,
        album_title=album_title,
        strict_match_verified=bool(e.get("strict_match_verified")),
        musicbrainz_release_group_id=_dupe_get_mb_release_group_id(e),
        musicbrainz_release_id=_dupe_get_mb_release_id(e),
        discogs_release_id=_dupe_get_discogs_id(e),
        lastfm_album_mbid=_dupe_get_lastfm_mbid(e),
        bandcamp_album_url=_dupe_get_bandcamp_url(e),
    )


def _files_publication_candidate_score(candidate: dict) -> tuple:
    item = candidate.get("item") if isinstance(candidate.get("item"), dict) else {}
    row = candidate.get("row") if isinstance(candidate.get("row"), dict) else {}
    folder_name = ""
    try:
        folder_name = Path(str(row.get("folder_path") or item.get("folder") or "")).name.lower()
    except Exception:
        folder_name = str(row.get("folder_path") or item.get("folder") or "").lower()
    noisy_tokens = ("no tags", "no cover", "gaps", "incomplete", "broken", "[dupe]", " dupe", "(dupe)")
    variant_clean = 1 if not any(tok in folder_name for tok in noisy_tokens) else 0
    missing_required = list(item.get("pre_missing_required_tags") or [])
    format_value = str(row.get("format") or "").strip().lower()
    return (
        1 if bool(row.get("strict_match_verified")) else 0,
        1 if bool(row.get("has_cover")) else 0,
        1 if bool(item.get("pre_has_cover")) else 0,
        1 if bool(item.get("pre_has_artist_image")) else 0,
        variant_clean,
        -int(len(missing_required)),
        int(score_format(format_value) or 0),
        int(item.get("bd") or 0),
        int(item.get("sr") or 0),
        int(item.get("br") or 0),
        float(row.get("strict_tracklist_score") or 0.0),
        int(row.get("track_count") or 0),
        int(row.get("total_duration_sec") or 0),
        -len(str(row.get("folder_path") or "")),
    )


def _collapse_files_publication_candidates(
    artist_name: str,
    candidates: list[dict],
) -> tuple[list[dict], set[str]]:
    if not candidates:
        return [], set()

    def _family_key(row: dict | None) -> str:
        r = row if isinstance(row, dict) else {}
        artist_norm = _norm_artist_key(str(r.get("artist_name") or "").strip())
        title_norm = norm_album_for_dedup(
            str(r.get("album_title") or "").strip(),
            normalize_parenthetical=True,
        )
        if not artist_norm or not title_norm:
            return ""
        return f"{artist_norm}::{title_norm}"

    grouped: dict[str, list[dict]] = defaultdict(list)
    passthrough: list[dict] = []
    hidden_folder_paths: set[str] = set()
    strict_family_keys: set[str] = set()
    for candidate in candidates:
        row = candidate.get("row") if isinstance(candidate.get("row"), dict) else {}
        key = _strict_album_identity_key(
            artist_name=str(row.get("artist_name") or ""),
            album_title=str(row.get("album_title") or ""),
            strict_match_verified=bool(row.get("strict_match_verified")),
            musicbrainz_release_group_id=str(row.get("musicbrainz_release_group_id") or ""),
            musicbrainz_release_id=str(row.get("musicbrainz_release_id") or ""),
            discogs_release_id=str(row.get("discogs_release_id") or ""),
            lastfm_album_mbid=str(row.get("lastfm_album_mbid") or ""),
            bandcamp_album_url=str(row.get("bandcamp_album_url") or ""),
        )
        if key:
            grouped[key].append(candidate)
            family_key = _family_key(row)
            if family_key:
                strict_family_keys.add(family_key)
        else:
            passthrough.append(candidate)
    out_candidates: list[dict] = []
    for candidate in passthrough:
        row = candidate.get("row") if isinstance(candidate.get("row"), dict) else {}
        family_key = _family_key(row)
        folder_path = str(row.get("folder_path") or "").strip()
        if family_key and family_key in strict_family_keys:
            if folder_path:
                hidden_folder_paths.add(folder_path)
            logging.info(
                "[Scan Pipeline] publication shadow-hide artist=%s family=%s folder=%s strict_reject=%s",
                artist_name,
                family_key,
                folder_path,
                str(row.get("strict_reject_reason") or "").strip(),
            )
            continue
        out_candidates.append(candidate)
    for key, group in grouped.items():
        if len(group) == 1:
            out_candidates.append(group[0])
            continue
        winner = max(group, key=_files_publication_candidate_score)
        out_candidates.append(winner)
        loser_paths = sorted(
            {
                str((cand.get("row") or {}).get("folder_path") or "").strip()
                for cand in group
                if cand is not winner and str((cand.get("row") or {}).get("folder_path") or "").strip()
            }
        )
        hidden_folder_paths.update(loser_paths)
        logging.info(
            "[Scan Pipeline] publication collapse artist=%s key=%s winner=%s losers=%s",
            artist_name,
            key,
            str((winner.get("row") or {}).get("folder_path") or "").strip(),
            loser_paths,
        )
    rows = [dict(candidate.get("row") or {}) for candidate in out_candidates if isinstance(candidate.get("row"), dict)]
    return rows, hidden_folder_paths


def _delete_files_library_published_rows(folder_paths: list[str] | set[str] | tuple[str, ...]) -> int:
    paths = sorted({str(value or "").strip() for value in (folder_paths or []) if str(value or "").strip()})
    if not paths:
        return 0
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=30)
        cur = con.cursor()
        placeholders = ",".join(["?"] * len(paths))
        cur.execute(
            f"DELETE FROM files_library_published_albums WHERE folder_path IN ({placeholders})",
            paths,
        )
        deleted = int(cur.rowcount or 0)
        con.commit()
        con.close()
        return deleted
    except Exception:
        logging.debug("Failed to delete stale files_library_published_albums rows", exc_info=True)
        return 0


def _files_publication_rewrite_path_prefix(value: str, old_prefix: str, new_prefix: str) -> str:
    text = str(value or "").strip()
    old = str(old_prefix or "").rstrip("/")
    new = str(new_prefix or "").rstrip("/")
    if not text or not old or not new or old == new:
        return text
    if text == old:
        return new
    if text.startswith(old + "/"):
        return new + text[len(old):]
    return text


def _files_publication_rewrite_tracks_json(tracks_json: str, old_prefix: str, new_prefix: str) -> str:
    try:
        tracks = json.loads(tracks_json or "[]")
    except Exception:
        tracks = []
    if not isinstance(tracks, list):
        return tracks_json or "[]"
    rewritten: list[dict] = []
    for track in tracks:
        if not isinstance(track, dict):
            continue
        row = dict(track)
        if row.get("file_path"):
            row["file_path"] = _files_publication_rewrite_path_prefix(
                str(row.get("file_path") or ""),
                old_prefix,
                new_prefix,
            )
        rewritten.append(row)
    return json.dumps(rewritten, ensure_ascii=False)


def _files_publication_remap_published_row(
    row: dict[str, Any],
    *,
    original_key: str,
    folder_key: str,
    scan_id: int | None = None,
) -> dict[str, Any] | None:
    if not row or not original_key or not folder_key or original_key == folder_key:
        return None
    out = dict(row)
    out["folder_path"] = folder_key
    if scan_id and int(scan_id) > 0:
        out["scan_id"] = int(scan_id)
    out["cover_path"] = _files_publication_rewrite_path_prefix(
        str(out.get("cover_path") or ""),
        original_key,
        folder_key,
    )
    out["artist_image_path"] = _files_publication_rewrite_path_prefix(
        str(out.get("artist_image_path") or ""),
        original_key,
        folder_key,
    )
    out["tracks_json"] = _files_publication_rewrite_tracks_json(
        str(out.get("tracks_json") or "[]"),
        original_key,
        folder_key,
    )
    out["updated_at"] = time.time()
    return out


def _files_publication_load_published_rows_by_folder(cur) -> dict[str, dict[str, Any]]:
    try:
        cur.execute("SELECT * FROM files_library_published_albums")
        return {
            str(row["folder_path"] or "").strip(): dict(row)
            for row in cur.fetchall()
            if str(row["folder_path"] or "").strip()
        }
    except Exception:
        logging.debug("Failed to load published rows for publication reconciliation", exc_info=True)
        return {}


def _files_live_publish_batches(
    items: list[dict],
    *,
    chunk_size: int | None = None,
    min_batching_items: int | None = None,
) -> list[list[dict]]:
    normalized = [dict(item) for item in (items or []) if isinstance(item, dict)]
    if not normalized:
        return []
    size = max(1, int(chunk_size or _FILES_SCAN_LIVE_PUBLISH_BATCH_SIZE))
    minimum = max(size + 1, int(min_batching_items or _FILES_SCAN_LIVE_PUBLISH_MIN_ITEMS))
    if len(normalized) < minimum or len(normalized) <= size:
        return [normalized]
    return [normalized[idx: idx + size] for idx in range(0, len(normalized), size)]


def _publish_files_library_artist_live_batches(
    artist_name: str,
    items: list[dict],
    *,
    scan_id: int | None = None,
    results_by_album_id: dict[int, dict] | None = None,
    on_batch=None,
) -> dict[str, Any]:
    batches = _files_live_publish_batches(items)
    if not batches:
        return {"published": 0, "batches": 0, "chunk_size": 0}
    total_batches = len(batches)
    chunk_size = max((len(batch) for batch in batches), default=0)
    total_published = 0
    if total_batches > 1:
        logging.info(
            "[Scan Pipeline] live publication chunked artist=%s albums=%d batches=%d chunk_size=%d",
            artist_name,
            len(items or []),
            total_batches,
            chunk_size,
        )
    for batch_index, batch_items in enumerate(batches, start=1):
        inserted = int(
            _publish_files_library_artist_from_items(
                artist_name,
                batch_items,
                scan_id=scan_id,
                results_by_album_id=results_by_album_id,
            )
            or 0
        )
        total_published += inserted
        if callable(on_batch):
            try:
                on_batch(
                    inserted=inserted,
                    batch_index=batch_index,
                    total_batches=total_batches,
                    batch_size=len(batch_items),
                )
            except Exception:
                logging.debug("Files live publication on_batch callback failed for %s", artist_name, exc_info=True)
    return {
        "published": int(total_published),
        "batches": int(total_batches),
        "chunk_size": int(chunk_size),
    }


def _rebuild_files_publication_for_scan(scan_id: int | None) -> dict[str, int]:
    """
    Rebuild published Files rows from the current on-disk state for one scan.

    This runs after the scan pipeline modified tags, covers, artist images, or moved
    albums to dupes/incomplete. The published cache must reflect the final filesystem
    state, otherwise the UI keeps showing stale albums and stale artist/image coverage.
    """
    sid = _parse_int_loose(scan_id, 0)
    if sid <= 0 or _get_library_mode() != "files":
        return {"scan_id": int(sid or 0), "deleted": 0, "inserted": 0}

    targets = _scan_collect_profile_enrich_targets(sid)
    raw_target_count = len(targets)
    targets, filtered_missing_targets = _filter_existing_files_album_items(
        targets,
        context=f"scan_publication_rebuild:{sid}",
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in targets:
        if not isinstance(item, dict):
            continue
        artist_name = str(item.get("artist") or "").strip() or "Unknown Artist"
        grouped[artist_name].append(dict(item))

    deleted = 0
    con = None
    try:
        con = _state_connect(timeout=30)
        cur = con.cursor()
        cur.execute("DELETE FROM files_library_published_albums WHERE scan_id = ?", (sid,))
        deleted = int(cur.rowcount or 0)
        con.commit()
    except Exception:
        if con is not None:
            try:
                con.rollback()
            except Exception:
                pass
        logging.debug("Failed to clear published rows before scan publication rebuild for scan_id=%s", sid, exc_info=True)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass

    inserted = 0
    for artist_name, items in grouped.items():
        try:
            inserted += int(
                _publish_files_library_artist_from_items(
                    artist_name,
                    items,
                    scan_id=sid,
                    results_by_album_id={},
                )
                or 0
            )
        except Exception:
            logging.debug(
                "Failed to rebuild published rows for artist=%s scan_id=%s",
                artist_name,
                sid,
                exc_info=True,
            )

    surviving_targets = 0
    for item in targets:
        try:
            folder_raw = str((item or {}).get("folder") or "").strip()
            if not folder_raw:
                continue
            folder_live = path_for_fs_access(Path(folder_raw))
            if folder_live.exists() and folder_live.is_dir():
                surviving_targets += 1
        except Exception:
            continue

    logging.info(
        "Files publication rebuild for scan_id=%s: deleted=%d inserted=%d target_albums=%d surviving_targets=%d filtered_missing=%d",
        sid,
        deleted,
        inserted,
        raw_target_count,
        surviving_targets,
        filtered_missing_targets,
    )
    if inserted < surviving_targets:
        logging.warning(
            "Files publication rebuild incomplete for scan_id=%s: published=%d surviving_targets=%d raw_targets=%d",
            sid,
            inserted,
            surviving_targets,
            raw_target_count,
        )
    return {"scan_id": sid, "deleted": int(deleted), "inserted": int(inserted)}


def _files_publication_scan_move_maps(cur: sqlite3.Cursor) -> dict[str, Any]:
    """Return scan-move path maps used to repair publication rows after exports/quarantines."""
    out: dict[str, Any] = {
        "matched_by_key": {},
        "matched_by_source": {},
        "suppressed_keys": set(),
        "suppressed_sources": set(),
    }
    if not _sqlite_table_exists(cur, "scan_moves"):
        return out
    try:
        cur.execute("PRAGMA table_info(scan_moves)")
        cols = {str(r[1] or "").strip() for r in cur.fetchall() if len(r) > 1}
        source_expr = "COALESCE(source_path, original_path, '')" if "source_path" in cols else "COALESCE(original_path, '')"
        dest_expr = "COALESCE(destination_path, moved_to_path, '')" if "destination_path" in cols else "COALESCE(moved_to_path, '')"
        reason_expr = "COALESCE(move_reason, '')" if "move_reason" in cols else "''"
        restored_expr = "COALESCE(restored, 0)" if "restored" in cols else "0"
        cur.execute(
            f"""
            SELECT
                COALESCE(scan_id, 0),
                COALESCE(album_id, 0),
                {source_expr} AS source_path,
                {dest_expr} AS destination_path,
                {reason_expr} AS move_reason,
                {restored_expr} AS restored
            FROM scan_moves
            WHERE {restored_expr} = 0
            """
        )
        for scan_id, album_id, source_path, destination_path, move_reason, _restored in cur.fetchall():
            sid = int(_parse_int_loose(scan_id, 0) or 0)
            aid = int(_parse_int_loose(album_id, 0) or 0)
            source_key = _album_folder_cache_key(Path(str(source_path or "").strip())) if str(source_path or "").strip() else ""
            dest_raw = str(destination_path or "").strip()
            reason = str(move_reason or "").strip().lower()
            if not source_key and not aid:
                continue
            key = (sid, aid)
            if reason == "matched_export" and dest_raw:
                if sid > 0 and aid > 0:
                    out["matched_by_key"][key] = dest_raw
                if source_key:
                    out["matched_by_source"][source_key] = dest_raw
                continue
            if reason in {"dedupe", "incomplete", "incomplete_quarantine", "matched_export_conflict"}:
                if sid > 0 and aid > 0:
                    out["suppressed_keys"].add(key)
                if source_key:
                    out["suppressed_sources"].add(source_key)
    except Exception:
        logging.debug("Failed to load scan move maps for Files publication reconciliation", exc_info=True)
    return out


def _files_publication_candidate_existing_path(path_raw: str) -> tuple[str, Path] | None:
    candidate = str(path_raw or "").strip()
    if not candidate:
        return None
    try:
        live = path_for_fs_access(Path(candidate))
        if live.exists() and live.is_dir():
            return _album_folder_cache_key(Path(candidate)), live
    except Exception:
        return None
    return None


def _scan_edition_row_to_publication_item(
    row: sqlite3.Row,
    *,
    cache_payload: dict[str, Any] | None = None,
    folder_key: str,
    live_folder: Path,
) -> dict[str, Any] | None:
    artist_name = str(row["artist"] or "").strip()
    album_title = _sanitize_album_title_display(str(row["title_raw"] or "").strip())
    if not artist_name or not album_title:
        return None
    meta_raw = row["meta_json"] if "meta_json" in row.keys() else ""
    meta = _json_loads_safe(meta_raw, {})
    if not isinstance(meta, dict):
        meta = {}
    cache_payload = dict(cache_payload or {})
    missing_indices = _json_loads_safe(row["missing_indices"] if "missing_indices" in row.keys() else "", [])
    if not isinstance(missing_indices, list):
        missing_indices = []
    missing_required = _json_loads_safe(row["missing_required_tags"] if "missing_required_tags" in row.keys() else "", [])
    if not isinstance(missing_required, list):
        missing_required = []
    strict_provider = _normalize_identity_provider(str(row["strict_match_provider"] or cache_payload.get("strict_match_provider") or ""))
    metadata_source = _normalize_identity_provider(
        str(row["metadata_source"] or cache_payload.get("metadata_source") or strict_provider or "")
    )
    musicbrainz_id = str(row["musicbrainz_id"] or cache_payload.get("musicbrainz_id") or "").strip()
    musicbrainz_release_id = str(row["musicbrainz_release_id"] or cache_payload.get("musicbrainz_release_id") or "").strip()
    discogs_release_id = str(row["discogs_release_id"] or cache_payload.get("discogs_release_id") or "").strip()
    lastfm_album_mbid = str(row["lastfm_album_mbid"] or cache_payload.get("lastfm_album_mbid") or "").strip()
    bandcamp_album_url = str(row["bandcamp_album_url"] or cache_payload.get("bandcamp_album_url") or "").strip()
    if musicbrainz_id:
        meta.setdefault("musicbrainz_releasegroupid", musicbrainz_id)
        meta.setdefault("musicbrainz_release_group_id", musicbrainz_id)
    if musicbrainz_release_id:
        meta.setdefault("musicbrainz_releaseid", musicbrainz_release_id)
        meta.setdefault("musicbrainz_albumid", musicbrainz_release_id)
    if discogs_release_id:
        meta.setdefault("discogs_release_id", discogs_release_id)
    if lastfm_album_mbid:
        meta.setdefault("lastfm_album_mbid", lastfm_album_mbid)
    if bandcamp_album_url:
        meta.setdefault("bandcamp_album_url", bandcamp_album_url)
    if metadata_source:
        meta.setdefault("primary_metadata_source", metadata_source)
    if strict_provider:
        meta.setdefault(PMDA_MATCH_PROVIDER_TAG, strict_provider)
        meta.setdefault("pmda_matched", "true")

    ordered_paths = [str(p) for p in (cache_payload.get("ordered_paths") or []) if str(p or "").strip()]

    return {
        "scan_id": int(_parse_int_loose(row["scan_id"], 0) or 0),
        "artist": artist_name,
        "artist_name": artist_name,
        "album_id": int(_parse_int_loose(row["album_id"], 0) or 0),
        "album_title": album_title,
        "title_raw": album_title,
        "folder": folder_key,
        "meta": meta,
        "fmt_text": str(row["fmt_text"] or "").strip(),
        "format": str(row["fmt_text"] or "").strip(),
        "br": int(_parse_int_loose(row["br"], 0) or 0),
        "sr": int(_parse_int_loose(row["sr"], 0) or 0),
        "bd": int(_parse_int_loose(row["bd"], 0) or 0),
        "ordered_paths": ordered_paths,
        "musicbrainz_id": musicbrainz_id,
        "musicbrainz_release_id": musicbrainz_release_id,
        "discogs_release_id": discogs_release_id,
        "lastfm_album_mbid": lastfm_album_mbid,
        "bandcamp_album_url": bandcamp_album_url,
        "metadata_source": metadata_source,
        "primary_metadata_source": metadata_source,
        "strict_match_verified": bool(row["strict_match_verified"] or cache_payload.get("strict_match_verified")),
        "strict_match_provider": strict_provider,
        "strict_reject_reason": str(row["strict_reject_reason"] or cache_payload.get("strict_reject_reason") or "").strip(),
        "strict_tracklist_score": float(row["strict_tracklist_score"] or cache_payload.get("strict_tracklist_score") or 0.0),
        "is_broken": bool(row["is_broken"]),
        "expected_track_count": int(_parse_int_loose(row["expected_track_count"], 0) or 0) or None,
        "actual_track_count": int(_parse_int_loose(row["actual_track_count"], 0) or 0) or None,
        "missing_indices": missing_indices,
        "pre_has_cover": bool(row["has_cover"] or cache_payload.get("has_cover")),
        "pre_has_artist_image": bool(cache_payload.get("has_artist_image")),
        "pre_missing_required_tags": missing_required or list(cache_payload.get("missing_required_tags") or []),
        "fingerprint": str(cache_payload.get("fingerprint") or "").strip(),
        "_publication_reconcile_skip_ffprobe": True,
        "_publication_reconcile_fast_publish": True,
    }
