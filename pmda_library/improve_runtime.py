"""Runtime-backed library improvement operations.

This module contains the effectful album/folder improvement implementations
extracted from ``pmda.py``. It binds the live PMDA runtime at the boundary while
provider, tag, and filesystem dependencies are progressively split into services.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind PMDA runtime globals for one improvement operation."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {"_improve_single_album", "_improve_folder_by_path"}
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _runtime_module() -> Any:
    """Return the bound PMDA runtime module when available."""
    return _RUNTIME if _RUNTIME is not None else sys.modules[__name__]


def improve_single_album_for_runtime(
    runtime: Any,
    album_id: int,
    db_conn: Any,
    known_release_group_id: Optional[str] = None,
) -> dict:
    """Improve one library album using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _improve_single_album_impl(album_id, db_conn, known_release_group_id=known_release_group_id)


def improve_folder_by_path_for_runtime(runtime: Any, folder_path: Path) -> dict:
    """Improve one filesystem folder using the live PMDA runtime."""
    _bind_runtime(runtime)
    return _improve_folder_by_path_impl(folder_path)


def _improve_single_album_impl(album_id: int, db_conn: Any, known_release_group_id: Optional[str] = None) -> dict:
    """Compatibility stub for the removed Plex-source album improvement path."""
    return {
        "steps": ["Plex source album improvement is disabled"],
        "summary": "Plex source album improvement is disabled. Use files-mode folder improvement instead.",
        "tags_updated": False,
        "cover_saved": False,
        "provider_used": None,
        "mutation_blocked": True,
        "mutation_blocked_reason": "plex_source_disabled",
    }


def _improve_folder_by_path_impl(folder_path: Path) -> dict:
    """
    Improve one album by folder path (no Plex). Infers artist/album from tags or filename,
    runs MusicBrainz search + tag/cover pipeline, detects dupes within folder.
    Returns same shape as _improve_single_album plus dupes_in_folder and files_updated.
    """
    steps: List[str] = []
    tags_updated = False
    cover_saved = False
    provider_used: Optional[str] = None
    files_updated = 0
    dupes_in_folder: List[dict] = []
    discogs_release_id = ""
    lastfm_album_mbid = ""
    bandcamp_album_url = ""

    if not folder_path.is_dir():
        return {"steps": ["Not a directory"], "summary": "Invalid path.", "tags_updated": False, "cover_saved": False, "provider_used": None, "dupes_in_folder": [], "files_updated": 0}

    audio_files = sorted([p for p in folder_path.rglob("*") if AUDIO_RE.search(p.name)])
    if not audio_files:
        return {"steps": ["No audio files"], "summary": "No audio files found.", "tags_updated": False, "cover_saved": False, "provider_used": None, "dupes_in_folder": [], "files_updated": 0}

    artist_name, album_title_str = _infer_artist_album_from_folder(folder_path, audio_files)

    # Dupe detection: group by track index
    by_track: Dict[int, List[Path]] = defaultdict(list)
    for p in audio_files:
        idx = _track_index_from_file(p)
        if idx is not None:
            by_track[idx].append(p)
    for idx, paths in by_track.items():
        if len(paths) > 1:
            dupes_in_folder.append({"track": idx, "paths": [str(p) for p in paths]})
    if dupes_in_folder:
        steps.append(f"Duplicate track positions detected: {len(dupes_in_folder)} group(s)")

    if BACKUP_BEFORE_FIX:
        backup_dst = _backup_album_folder_before_fix(folder_path, artist_name, album_title_str)
        if backup_dst:
            steps.append(f"Backed up to {backup_dst}")
        else:
            steps.append("Backup skipped: copy failed")

    first_audio = audio_files[0]
    current_tags = dict(extract_tags(first_audio) or {})
    musicbrainz_release_id = str(
        current_tags.get("musicbrainz_releaseid")
        or current_tags.get("musicbrainz_albumid")
        or ""
    ).strip()
    release_group_mbid = str(current_tags.get("musicbrainz_releasegroupid") or "").strip()
    release_mbid = release_group_mbid or musicbrainz_release_id
    # Compute which required tags are currently missing on the first audio file.
    # This lets us decide when to call Last.fm/Bandcamp even if MusicBrainz already provided basic tags.
    try:
        missing_required = _check_required_tags(current_tags, REQUIRED_TAGS, edition=None)
    except Exception:
        missing_required = []

    # Global strict commit gate (100% match required): artist + album + provider id +
    # exact track count + exact positional track titles.
    strict_tracks: list[dict] = []
    for order, audio_path in enumerate(audio_files, start=1):
        try:
            meta_i = dict(extract_tags(audio_path) or {})
        except Exception:
            meta_i = {}
        title_i = (
            str(
                meta_i.get("title")
                or meta_i.get("tit2")
                or meta_i.get("track_title")
                or ""
            ).strip()
            or audio_path.stem
        )
        idx_i = (
            _parse_int_loose(
                meta_i.get("track")
                or meta_i.get("tracknumber")
                or meta_i.get("trck"),
                order,
            )
            or order
        )
        disc_i = (
            _parse_int_loose(
                meta_i.get("disc")
                or meta_i.get("discnumber")
                or meta_i.get("disc_num")
                or meta_i.get("tpas"),
                1,
            )
            or 1
        )
        strict_tracks.append(
            {
                "title": title_i,
                "idx": idx_i,
                "disc": disc_i,
            }
        )
    strict_edition = {
        "tracks": strict_tracks,
        "meta": current_tags,
        "musicbrainz_id": str(release_mbid or current_tags.get("musicbrainz_releasegroupid") or current_tags.get("musicbrainz_releaseid") or "").strip(),
        "discogs_release_id": str(current_tags.get("discogs_release_id") or "").strip(),
        "bandcamp_album_url": str(current_tags.get("bandcamp_album_url") or "").strip(),
        "lastfm_album_mbid": str(current_tags.get("lastfm_album_mbid") or "").strip(),
    }
    strict_verdict = _strict_validate_edition_match(
        artist_name=artist_name,
        album_title=album_title_str,
        edition=strict_edition,
    )
    strict_ok, strict_reason = _strict_mutation_allowed(strict_verdict)
    if not strict_ok:
        reason = strict_reason or "strict_match_missing"
        steps.append(f"Mutation blocked: {reason}")
        return {
            "steps": steps,
            "summary": f"No mutation applied (strict gate): {reason}.",
            "tags_updated": False,
            "cover_saved": False,
            "provider_used": None,
            "dupes_in_folder": dupes_in_folder,
            "files_updated": 0,
            "pmda_matched": False,
            "pmda_cover": False,
            "pmda_artist_image": False,
            "pmda_complete": False,
            "pmda_match_provider": None,
            "pmda_cover_provider": None,
            "pmda_artist_provider": None,
            "discogs_release_id": "",
            "lastfm_album_mbid": "",
            "bandcamp_album_url": "",
            "strict_match_verified": False,
            "strict_match_provider": "",
            "strict_reject_reason": reason,
            "strict_tracklist_score": 0.0,
            "mutation_blocked": True,
            "mutation_blocked_reason": reason,
        }
    strict_provider = _normalize_identity_provider(str(strict_verdict.get("strict_match_provider") or ""))
    exact_cover_provider_lock = strict_provider if strict_provider in {"musicbrainz", "discogs", "bandcamp", "lastfm"} else ""
    strict_provider_label = {
        "musicbrainz": "MusicBrainz",
        "discogs": "Discogs",
        "bandcamp": "Bandcamp",
        "lastfm": "Last.fm",
    }.get(strict_provider, strict_provider or "provider")
    steps.append(f"Strict match verified (100%): {strict_provider_label}")
    strict_provider_id = str(strict_verdict.get("provider_id") or "").strip()
    if strict_provider == "musicbrainz" and strict_provider_id:
        resolved_strict_rg = resolve_mbid_to_release_group(strict_provider_id, "strict_match_provider")
        if resolved_strict_rg and resolved_strict_rg != strict_provider_id:
            musicbrainz_release_id = strict_provider_id
            release_group_mbid = str(resolved_strict_rg or "").strip()
        elif not release_group_mbid:
            release_group_mbid = strict_provider_id
        release_mbid = release_group_mbid or musicbrainz_release_id or strict_provider_id
    elif strict_provider == "discogs" and strict_provider_id:
        discogs_release_id = strict_provider_id
    elif strict_provider == "bandcamp" and strict_provider_id:
        bandcamp_album_url = strict_provider_id
    elif strict_provider == "lastfm" and strict_provider_id:
        lastfm_album_mbid = strict_provider_id

    if not release_mbid and USE_MUSICBRAINZ:
        album_norm = norm_album(album_title_str)
        tracks = set()
        try:
            for p in audio_files[:20]:
                meta = extract_tags(p)
                t = (meta.get("title") or meta.get("tit2") or "").strip()
                if t:
                    tracks.add(t)
        except Exception:
            pass
        rg_info, _ = search_mb_release_group_by_metadata(
            artist_name,
            album_norm,
            tracks,
            title_raw=album_title_str,
            album_folder=folder_path,
            local_tags=current_tags if isinstance(current_tags, dict) else {},
            local_paths=[str(p) for p in audio_files[:80]],
            scan_inline=False,
        )
        if rg_info and isinstance(rg_info.get("id"), str):
            release_group_mbid = str(rg_info["id"] or "").strip()
            release_mbid = release_group_mbid
            steps.append("Found MusicBrainz release group via search")
    if release_mbid and not steps:
        steps.append("Using existing MusicBrainz ID")

    try:
        from mutagen import File as MutagenFile
        HAS_MUTAGEN = True
    except ImportError:
        HAS_MUTAGEN = False

    if not HAS_MUTAGEN:
        return {"steps": steps + ["Mutagen not installed"], "summary": "Cannot update tags: mutagen not installed.", "tags_updated": False, "cover_saved": False, "provider_used": None, "dupes_in_folder": dupes_in_folder, "files_updated": 0}

    mb_release_info = None
    pmda_match_provider: Optional[str] = None
    pmda_cover_provider: Optional[str] = None
    pmda_artist_provider: Optional[str] = None
    if release_mbid:
        release_group_id = release_mbid
        tag_src = "musicbrainz_releasegroupid" if current_tags.get("musicbrainz_releasegroupid") else ("musicbrainz_releaseid" if current_tags.get("musicbrainz_releaseid") else "")
        resolved = resolve_mbid_to_release_group(release_mbid, tag_src)
        if resolved:
            if release_mbid and release_mbid != resolved and not musicbrainz_release_id:
                musicbrainz_release_id = str(release_mbid or "").strip()
            release_group_id = resolved
            release_group_mbid = str(resolved or "").strip()
        missing_rg_cache = _mb_missing_release_group_ids_cache()
        if release_group_id in missing_rg_cache:
            steps.append(f"MusicBrainz release group unavailable: {release_group_id}")
        else:
            try:
                result = musicbrainzngs.get_release_group_by_id(release_group_id, includes=["releases", "artist-credits"])
                mb_release_info = result.get("release-group", {})
                if mb_release_info:
                    strict_ok, strict_reason = _strict_identity_match_details(
                        local_artist=artist_name,
                        local_title=album_title_str,
                        candidate_artist=_extract_mb_artist_names(mb_release_info),
                        candidate_title=mb_release_info.get("title") or "",
                    )
                    if not strict_ok:
                        logging.warning(
                            "improve-folder: rejected MBID %s for %s / %s (%s)",
                            release_group_id,
                            artist_name,
                            album_title_str,
                            strict_reason,
                        )
                        steps.append(f"MusicBrainz rejected by strict identity: {strict_reason}")
                        mb_release_info = None
                        release_mbid = None
            except Exception as e:
                err_text = str(e or "").strip()
                if "HTTP Error 404" in err_text:
                    missing_rg_cache.add(release_group_id)
                    logging.info(
                        "improve-folder: release group %s not found; caching miss and skipping future direct fetches",
                        release_group_id,
                    )
                    steps.append(f"MusicBrainz release group missing: {release_group_id}")
                else:
                    logging.warning("improve-folder: failed to fetch release group %s: %s", release_group_id, e)
                    steps.append(f"MusicBrainz lookup failed: {e}")

    artist_mbid = current_tags.get("musicbrainz_albumartistid") or current_tags.get("musicbrainz_artistid")
    if not artist_mbid and USE_MUSICBRAINZ:
        try:
            search_result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
            if search_result.get("artist-list"):
                artist_mbid = search_result["artist-list"][0]["id"]
        except Exception as e:
            logging.warning("improve-folder: artist search failed for '%s': %s", artist_name, e)

    def _apply_fallback_tags_folder(artist_str: str, album_str: str, year_str: str, source: str, genre: str | None = None) -> int:
        if _files_tag_write_mode() == "pmda_id_only":
            return 0
        count = 0
        for audio_file in audio_files:
            try:
                audio = MutagenFile(str(audio_file))
                if audio is None:
                    continue
                _apply_artist_album_tags_to_audio(
                    audio,
                    album_artist=artist_str,
                    track_artist=artist_str,
                    album_title=album_str,
                    year_str=year_str,
                    genre_str=genre,
                )
                audio.save()
                count += 1
            except Exception as e:
                logging.error("improve-folder: fallback tag error %s: %s", audio_file, e)
        return count

    pmda_processed_id = str(uuid.uuid4()) if (mb_release_info and artist_mbid) else None
    for audio_file in audio_files:
        try:
            audio = MutagenFile(str(audio_file))
            if audio is None:
                continue
            if mb_release_info and artist_mbid:
                if pmda_match_provider is None:
                    pmda_match_provider = "musicbrainz"
                if _files_tag_write_mode() == "pmda_id_only":
                    _set_pmda_tag(audio, "PMDA_ID", pmda_processed_id or str(uuid.uuid4()))
                    audio.save()
                    files_updated += 1
                    continue
                mb_title = mb_release_info.get("title", album_title_str)
                date_str = mb_release_info.get("first-release-date", "")
                year = date_str.split("-")[0] if (date_str and "-" in date_str) else (date_str or "")
                _apply_artist_album_tags_to_audio(
                    audio,
                    album_artist=artist_name,
                    track_artist=artist_name,
                    album_title=mb_title,
                    year_str=year,
                )
                try:
                    from mutagen.flac import FLAC  # type: ignore
                    from mutagen.mp4 import MP4  # type: ignore
                except Exception:
                    FLAC = MP4 = None  # type: ignore[assignment]
                if FLAC is not None and isinstance(audio, FLAC):
                    audio["MUSICBRAINZ_ARTISTID"] = artist_mbid
                    audio["MUSICBRAINZ_ALBUMARTISTID"] = artist_mbid
                    if release_mbid:
                        audio["MUSICBRAINZ_RELEASEGROUPID"] = release_mbid
                elif MP4 is not None and isinstance(audio, MP4):
                    audio["----:com.apple.iTunes:MusicBrainz Artist Id"] = [artist_mbid.encode("utf-8")]
                    audio["----:com.apple.iTunes:MusicBrainz Album Artist Id"] = [artist_mbid.encode("utf-8")]
                    if release_mbid:
                        audio["----:com.apple.iTunes:MusicBrainz Release Group Id"] = [release_mbid.encode("utf-8")]
                audio.save()
                files_updated += 1
        except Exception as e:
            logging.error("improve-folder: error updating %s: %s", audio_file, e)

    if files_updated > 0:
        tags_updated = True
        steps.append(f"Updated tags on {files_updated} file(s)")

    has_cover = any(
        f.name.lower().startswith(("cover", "folder", "album", "artwork", "front"))
        and f.suffix.lower() in [".jpg", ".jpeg", ".png", ".webp"]
        for f in folder_path.iterdir() if f.is_file()
    )
    existing_cover_provider = _normalize_identity_provider(str(current_tags.get(PMDA_COVER_PROVIDER_TAG) or ""))
    preserve_local_cover_for_exact_identity = bool(
        exact_cover_provider_lock
        and has_cover
        and existing_cover_provider == exact_cover_provider_lock
    )
    allow_cover_replace = bool(
        has_cover
        and existing_cover_provider in {"", "local", "unknown"}
        and not preserve_local_cover_for_exact_identity
    )
    if (release_group_mbid or musicbrainz_release_id) and (not has_cover or allow_cover_replace):
        try:
            cover_result = _download_cover_art_archive_front(
                release_id=musicbrainz_release_id,
                release_group_id=release_group_mbid or release_mbid,
                timeout_sec=5.0,
            )
            if cover_result:
                content, mime, _cover_url = cover_result
                # With a validated MB release-group id, CAA is considered trusted.
                # Do not gate it behind vision (costly + can reject correct covers).
                use_vision = bool(USE_AI_VISION_BEFORE_COVER_INJECT) and False
                if use_vision:
                    if not _vision_verify_cover_before_inject(content, mime, artist_name, album_title_str, "CAA"):
                        steps.append("Vision: cover rejected (not matching album)")
                    else:
                        steps.append("Vision: cover accepted")
                        if _files_tag_write_mode() != "pmda_id_only":
                            cover_path = folder_path / "cover.jpg"
                            with open(cover_path, "wb") as f:
                                f.write(content)
                            cover_saved = True
                            pmda_cover_provider = "musicbrainz"
                            steps.append("Fetched and saved cover art")
                            _embed_cover_in_audio_files(cover_path, audio_files)
                else:
                    if _files_tag_write_mode() != "pmda_id_only":
                        cover_path = folder_path / "cover.jpg"
                        with open(cover_path, "wb") as f:
                            f.write(content)
                        cover_saved = True
                        pmda_cover_provider = "musicbrainz"
                        steps.append("Fetched and saved cover art")
                        _embed_cover_in_audio_files(cover_path, audio_files)
        except Exception as e:
            logging.warning("improve-folder: cover fetch failed: %s", e)
    if tags_updated or cover_saved:
        provider_used = "musicbrainz"

    has_cover_now = has_cover or cover_saved
    if exact_cover_provider_lock and not cover_saved:
        if has_cover_now:
            steps.append(f"Preserved local cover; skipped cross-provider cover replacement for exact {strict_provider_label} match")
        else:
            steps.append(f"No authoritative cover available from exact {strict_provider_label} match; skipped cross-provider cover replacement")
        need_verified_cover = False
    else:
        need_verified_cover = (not has_cover_now) or allow_cover_replace
    if USE_DISCOGS and (not tags_updated or need_verified_cover):
        try:
            did = str(discogs_release_id or current_tags.get("discogs_release_id") or "").strip()
            discogs_info = _fetch_discogs_release_by_id(did) if did else _fetch_discogs_release(artist_name, album_title_str)
        except DiscogsRateLimited:
            steps.append("Discogs: rate limited (skipped)")
            discogs_info = None
        except Exception as e:
            logging.debug("improve-folder: Discogs fetch failed: %s", e)
            discogs_info = None
        discogs_strict_ok = False
        if discogs_info:
            strict_ok, strict_reason = _strict_identity_match_details(
                local_artist=artist_name,
                local_title=album_title_str,
                candidate_artist=discogs_info.get("artist_name") or "",
                candidate_title=discogs_info.get("title") or "",
            )
            if not strict_ok:
                logging.info(
                    "improve-folder: Discogs candidate rejected for %s / %s (%s)",
                    artist_name,
                    album_title_str,
                    strict_reason,
                )
                steps.append(f"Discogs rejected by strict identity: {strict_reason}")
                discogs_info = None
            else:
                discogs_strict_ok = True
        if discogs_info:
            discogs_release_id = str(discogs_info.get("release_id") or "").strip() or discogs_release_id
            artist_str = discogs_info.get("artist_name") or artist_name
            album_str = discogs_info.get("title") or album_title_str
            year_str = (discogs_info.get("year") or "").strip()
            if not tags_updated and (album_str or artist_str):
                n = _apply_fallback_tags_folder(artist_str, album_str, year_str, "Discogs")
                if n > 0:
                    tags_updated = True
                    files_updated = n
                    steps.append(f"Updated tags from Discogs on {n} file(s)")
            if need_verified_cover and discogs_info.get("cover_url"):
                try:
                    best_cover = _download_best_cover_image("Discogs", discogs_info.get("cover_url"))
                    if best_cover:
                        content, mime, _url_used = best_cover
                        # Use vision only when identity is ambiguous; strict provider identity is considered safe.
                        use_vision = bool(USE_AI_VISION_BEFORE_COVER_INJECT) and (not discogs_strict_ok)
                        if use_vision:
                            if not _vision_verify_cover_before_inject(content, mime, artist_name, album_title_str, "Discogs"):
                                steps.append("Vision: cover rejected (not matching album)")
                            else:
                                steps.append("Vision: cover accepted")
                                if _files_tag_write_mode() != "pmda_id_only":
                                    cover_path = folder_path / "cover.jpg"
                                    with open(cover_path, "wb") as f:
                                        f.write(content)
                                    cover_saved = True
                                    has_cover_now = True
                                    need_verified_cover = False
                                    pmda_cover_provider = "discogs"
                                    try:
                                        _files_watcher_suppress_folder(folder_path, seconds=120.0, reason="cover_write")
                                    except Exception:
                                        pass
                                    _embed_cover_in_audio_files(cover_path, audio_files)
                                    steps.append("Fetched and saved cover art from Discogs")
                        else:
                            if _files_tag_write_mode() != "pmda_id_only":
                                cover_path = folder_path / "cover.jpg"
                                with open(cover_path, "wb") as f:
                                    f.write(content)
                                cover_saved = True
                                has_cover_now = True
                                need_verified_cover = False
                                pmda_cover_provider = "discogs"
                                try:
                                    _files_watcher_suppress_folder(folder_path, seconds=120.0, reason="cover_write")
                                except Exception:
                                    pass
                                _embed_cover_in_audio_files(cover_path, audio_files)
                                steps.append("Fetched and saved cover art from Discogs")
                except Exception as e:
                    logging.warning("improve-folder: Discogs cover fetch failed: %s", e)
            if tags_updated or cover_saved:
                provider_used = "discogs"

    # Always allow Last.fm when some required tags (e.g. genre) are still missing,
    # even if MusicBrainz already provided basic tags.
    lastfm_used_for_tags = False
    if USE_LASTFM and ((not tags_updated or need_verified_cover) or ("genre" in missing_required)):
        lastfm_info = _fetch_lastfm_album_info(artist_name, album_title_str, release_mbid)
        lastfm_strict_ok = False
        if lastfm_info:
            strict_ok, strict_reason = _strict_identity_match_details(
                local_artist=artist_name,
                local_title=album_title_str,
                candidate_artist=lastfm_info.get("artist") or lastfm_info.get("artist_name") or "",
                candidate_title=lastfm_info.get("title") or lastfm_info.get("album") or "",
            )
            if not strict_ok:
                logging.info(
                    "improve-folder: Last.fm candidate rejected for %s / %s (%s)",
                    artist_name,
                    album_title_str,
                    strict_reason,
                )
                steps.append(f"Last.fm rejected by strict identity: {strict_reason}")
                lastfm_info = None
            else:
                lastfm_strict_ok = True
        if lastfm_info:
            lfm_mbid_val = str(lastfm_info.get("mbid") or "").strip()
            if lfm_mbid_val:
                lastfm_album_mbid = lfm_mbid_val
            artist_str = lastfm_info.get("artist") or artist_name
            album_str = lastfm_info.get("title") or album_title_str
            year_str = ""
            # Use primary Last.fm toptag as genre when available
            toptags = lastfm_info.get("toptags") or []
            primary_genre = ""
            if toptags:
                primary = toptags[0]
                primary_genre = (primary if isinstance(primary, str) else str(primary)).strip()
            if (album_str or artist_str) and (not tags_updated or "genre" in missing_required):
                n = _apply_fallback_tags_folder(artist_str, album_str, year_str, "Last.fm", genre=primary_genre or None)
                if n > 0:
                    tags_updated = True
                    files_updated = n
                    steps.append(f"Updated tags from Last.fm on {n} file(s)")
                    lastfm_used_for_tags = True
                    if primary_genre:
                        current_tags["genre"] = primary_genre
            if need_verified_cover and lastfm_info.get("cover_url"):
                try:
                    best_cover = _download_best_cover_image("Last.fm", lastfm_info.get("cover_url"))
                    if best_cover:
                        content, mime, _url_used = best_cover
                        # Use vision only when identity is ambiguous; strict provider identity is considered safe.
                        use_vision = bool(USE_AI_VISION_BEFORE_COVER_INJECT) and (not lastfm_strict_ok)
                        if use_vision:
                            if not _vision_verify_cover_before_inject(content, mime, artist_name, album_title_str, "Last.fm"):
                                steps.append("Vision: cover rejected (not matching album)")
                            else:
                                steps.append("Vision: cover accepted")
                                if _files_tag_write_mode() != "pmda_id_only":
                                    cover_path = folder_path / "cover.jpg"
                                    with open(cover_path, "wb") as f:
                                        f.write(content)
                                    cover_saved = True
                                    has_cover_now = True
                                    need_verified_cover = False
                                    pmda_cover_provider = "lastfm"
                                    try:
                                        _files_watcher_suppress_folder(folder_path, seconds=120.0, reason="cover_write")
                                    except Exception:
                                        pass
                                    _embed_cover_in_audio_files(cover_path, audio_files)
                                    steps.append("Fetched and saved cover art from Last.fm")
                        else:
                            if _files_tag_write_mode() != "pmda_id_only":
                                cover_path = folder_path / "cover.jpg"
                                with open(cover_path, "wb") as f:
                                    f.write(content)
                                cover_saved = True
                                has_cover_now = True
                                need_verified_cover = False
                                pmda_cover_provider = "lastfm"
                                try:
                                    _files_watcher_suppress_folder(folder_path, seconds=120.0, reason="cover_write")
                                except Exception:
                                    pass
                                _embed_cover_in_audio_files(cover_path, audio_files)
                                steps.append("Fetched and saved cover art from Last.fm")
                except Exception as e:
                    logging.warning("improve-folder: Last.fm cover fetch failed: %s", e)
            if tags_updated or cover_saved:
                provider_used = "lastfm"

    genre_missing_now = not str((current_tags or {}).get("genre") or "").strip()
    # Special case: if Last.fm was the identity provider, prefer Bandcamp tags for richer genres
    # when available (Bandcamp often exposes multiple useful genre tags).
    mb_identity_used = bool(mb_release_info and artist_mbid)
    want_bandcamp_genre = bool(lastfm_used_for_tags and not mb_identity_used and ("genre" in missing_required))
    if USE_BANDCAMP and ((not tags_updated or need_verified_cover) or genre_missing_now or want_bandcamp_genre):
        bandcamp_info = _fetch_bandcamp_album_info(
            artist_name,
            album_title_str,
            allow_web_fallback=False,
            album_url_hint=str(bandcamp_album_url or "").strip(),
        )
        bandcamp_strict_ok = False
        if bandcamp_info:
            strict_ok, strict_reason = _strict_identity_match_details(
                local_artist=artist_name,
                local_title=album_title_str,
                candidate_artist=bandcamp_info.get("artist_name") or "",
                candidate_title=bandcamp_info.get("title") or "",
            )
            if not strict_ok:
                logging.info(
                    "improve-folder: Bandcamp candidate rejected for %s / %s (%s)",
                    artist_name,
                    album_title_str,
                    strict_reason,
                )
                steps.append(f"Bandcamp rejected by strict identity: {strict_reason}")
                bandcamp_info = None
            else:
                bandcamp_strict_ok = True
        if bandcamp_info:
            bandcamp_album_url = str(bandcamp_info.get("album_url") or "").strip() or bandcamp_album_url
            artist_str = bandcamp_info.get("artist_name") or artist_name
            album_str = bandcamp_info.get("title") or album_title_str
            year_raw = (bandcamp_info.get("year") or "").strip()
            year_match = re.search(r"\b(\d{4})\b", year_raw) if year_raw else None
            year_str = year_match.group(1) if year_match else year_raw
            bandcamp_tags = bandcamp_info.get("tags") or []
            inferred_genre = _infer_genre_from_bandcamp_tags(bandcamp_tags) if bandcamp_tags else None
            if (album_str or artist_str) and (not tags_updated or "genre" in missing_required or want_bandcamp_genre):
                n = _apply_fallback_tags_folder(artist_str, album_str, year_str, "Bandcamp", genre=inferred_genre)
                if n > 0:
                    tags_updated = True
                    files_updated = n
                    steps.append(f"Updated tags from Bandcamp on {n} file(s)")
                    if inferred_genre:
                        current_tags["genre"] = inferred_genre
            if need_verified_cover and bandcamp_info.get("cover_url"):
                try:
                    best_cover = _download_best_cover_image(
                        "Bandcamp",
                        bandcamp_info.get("cover_url"),
                        cover_candidates=bandcamp_info.get("cover_candidates") or [],
                        headers={"User-Agent": "PMDA/1.0 (metadata fallback)"},
                    )
                    if best_cover:
                        content, mime, _url_used = best_cover
                        # Use vision only when identity is ambiguous; strict provider identity is considered safe.
                        use_vision = bool(USE_AI_VISION_BEFORE_COVER_INJECT) and (not bandcamp_strict_ok)
                        if use_vision:
                            if not _vision_verify_cover_before_inject(content, mime, artist_name, album_title_str, "Bandcamp"):
                                steps.append("Vision: cover rejected (not matching album)")
                            else:
                                steps.append("Vision: cover accepted")
                                if _files_tag_write_mode() != "pmda_id_only":
                                    cover_path = folder_path / "cover.jpg"
                                    with open(cover_path, "wb") as f:
                                        f.write(content)
                                    cover_saved = True
                                    has_cover_now = True
                                    need_verified_cover = False
                                    pmda_cover_provider = "bandcamp"
                                    try:
                                        _files_watcher_suppress_folder(folder_path, seconds=120.0, reason="cover_write")
                                    except Exception:
                                        pass
                                    _embed_cover_in_audio_files(cover_path, audio_files)
                                    steps.append("Fetched and saved cover art from Bandcamp")
                        else:
                            if _files_tag_write_mode() != "pmda_id_only":
                                cover_path = folder_path / "cover.jpg"
                                with open(cover_path, "wb") as f:
                                    f.write(content)
                                cover_saved = True
                                has_cover_now = True
                                need_verified_cover = False
                                pmda_cover_provider = "bandcamp"
                                try:
                                    _files_watcher_suppress_folder(folder_path, seconds=120.0, reason="cover_write")
                                except Exception:
                                    pass
                                _embed_cover_in_audio_files(cover_path, audio_files)
                                steps.append("Fetched and saved cover art from Bandcamp")
                except Exception as e:
                    logging.warning("improve-folder: Bandcamp cover fetch failed: %s", e)
            if tags_updated or cover_saved:
                provider_used = "bandcamp"

    root_dirs = _files_root_dir_strings()
    artist_folder = _files_guess_artist_folder(folder_path, artist_name, root_dirs=root_dirs)
    artist_provider = None
    had_artist_image = bool(_first_artist_image_path(artist_folder)) if artist_folder else False
    external_cached_path: Optional[str] = None
    if tags_updated or cover_saved:
        if artist_folder:
            artist_provider = _fetch_and_save_artist_image(
                artist_name,
                artist_folder,
                artist_mbid,
                identity_fields={
                    "musicbrainz_release_group_id": release_mbid,
                    "discogs_release_id": discogs_release_id,
                    "lastfm_album_mbid": lastfm_album_mbid,
                    "bandcamp_album_url": bandcamp_album_url,
                },
            )
            if artist_provider:
                pmda_artist_provider = artist_provider
                steps.append("Fetched and saved artist image")
        else:
            # Flat libraries (albums directly under FILES_ROOTS) have no reliable per-artist folder.
            # In that case, cache an external image instead of writing artist.jpg into the library root.
            try:
                conn = _files_pg_connect()
            except Exception:
                conn = None
            if conn is not None:
                try:
                    # If an external image is already cached, count it immediately (even if we don't fetch a new one).
                    try:
                        key = _norm_artist_key(artist_name)
                        ext = _files_get_external_artist_images(conn, [key]).get(key) or {}
                        ext_path = str(ext.get("image_path") or "").strip()
                        if ext_path:
                            external_cached_path = ext_path
                    except Exception:
                        pass
                    url = ""
                    prov = ""
                    try:
                        fanart_artist_mbid = _resolve_artist_mbid_for_fanart(
                            artist_name=artist_name,
                            artist_mbid=artist_mbid,
                            musicbrainz_id=release_mbid,
                            discogs_release_id=discogs_release_id,
                            lastfm_album_mbid=lastfm_album_mbid,
                            bandcamp_album_url=bandcamp_album_url,
                        )
                        if fanart_artist_mbid:
                            url = (_fetch_artist_image_fanart(fanart_artist_mbid) or "").strip()
                            if url:
                                prov = "fanart"
                    except Exception:
                        url = ""
                        prov = ""
                    if not url:
                        try:
                            url = (_fetch_artist_image_lastfm(artist_name) or "").strip()
                            if url:
                                prov = "lastfm"
                        except Exception:
                            url = ""
                            prov = ""
                    if not url:
                        try:
                            url = (_fetch_artist_image_audiodb(artist_name) or "").strip()
                            if url:
                                prov = "audiodb"
                        except Exception:
                            url = ""
                            prov = ""
                    if url and prov:
                        with conn.transaction():
                            external_cached_path = _files_cache_external_artist_image(
                                conn,
                                artist_name=artist_name,
                                provider=prov,
                                image_url=url,
                                max_px=640,
                            )
                        if external_cached_path:
                            pmda_artist_provider = prov
                            steps.append("Cached external artist image")
                except Exception:
                    pass
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
    has_artist_image_now = bool(_first_artist_image_path(artist_folder)) if artist_folder else bool(external_cached_path)
    pmda_matched = bool(tags_updated)
    pmda_cover = bool(cover_saved or has_cover_now)
    pmda_artist_image = has_artist_image_now
    pmda_complete = pmda_matched and pmda_cover and pmda_artist_image

    if pmda_matched and not pmda_match_provider:
        pmda_match_provider = provider_used or ("musicbrainz" if release_mbid else None)
    if pmda_cover and not pmda_cover_provider:
        if provider_used in {"musicbrainz", "discogs", "lastfm", "bandcamp"}:
            pmda_cover_provider = provider_used
        elif has_cover_now:
            pmda_cover_provider = "local"
    if pmda_artist_image and not pmda_artist_provider:
        pmda_artist_provider = artist_provider or ("local" if had_artist_image else None)
    pmda_id = (current_tags.get(PMDA_ID_TAG) or "").strip() or None
    if pmda_matched or pmda_cover or pmda_artist_image or pmda_complete:
        _write_pmda_album_tags(
            folder_path,
            audio_files,
            pmda_id=pmda_id,
            match_provider=pmda_match_provider,
            cover_provider=pmda_cover_provider,
            artist_provider=pmda_artist_provider,
            matched=pmda_matched,
            cover=pmda_cover,
            artist_image=pmda_artist_image,
            complete=pmda_complete,
            tag_write_mode=_files_tag_write_mode(),
        )

    summary = f"Updated tags on {files_updated} file(s)." + (" Fetched cover art." if cover_saved else "")
    return {
        "steps": steps,
        "summary": summary,
        "tags_updated": tags_updated,
        "cover_saved": cover_saved,
        "provider_used": provider_used,
        "dupes_in_folder": dupes_in_folder,
        "files_updated": files_updated,
        "pmda_matched": pmda_matched,
        "pmda_cover": pmda_cover,
        "pmda_artist_image": pmda_artist_image,
        "pmda_complete": pmda_complete,
        "pmda_match_provider": pmda_match_provider,
        "pmda_cover_provider": pmda_cover_provider,
        "pmda_artist_provider": pmda_artist_provider,
        "discogs_release_id": discogs_release_id,
        "lastfm_album_mbid": lastfm_album_mbid,
        "bandcamp_album_url": bandcamp_album_url,
        "strict_match_verified": bool(strict_verdict.get("strict_match_verified")),
        "strict_match_provider": strict_provider,
        "strict_reject_reason": str(strict_verdict.get("strict_reject_reason") or "").strip(),
        "strict_tracklist_score": float(strict_verdict.get("strict_tracklist_score") or 0.0),
        "mutation_blocked": False,
        "mutation_blocked_reason": "",
    }

# --- Extracted library improve/rematch API and audit helpers ---
def _match_type_from_flags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_match_type_from_flags`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _match_type_from_flags_impl(*args, **kwargs)

def _record_files_match_audit_album_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_record_files_match_audit_album`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _record_files_match_audit_album_impl(*args, **kwargs)

def _serialize_match_audit_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_serialize_match_audit_row`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _serialize_match_audit_row_impl(*args, **kwargs)

def _track_index_from_file_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_track_index_from_file`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _track_index_from_file_impl(*args, **kwargs)

def _infer_artist_album_from_folder_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_infer_artist_album_from_folder`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _infer_artist_album_from_folder_impl(*args, **kwargs)

def api_library_album_rematch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_library_album_rematch`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_album_rematch_impl(*args, **kwargs)

def api_library_artist_rematch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_library_artist_rematch`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_artist_rematch_impl(*args, **kwargs)

def api_library_improve_album_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_library_improve_album`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_improve_album_impl(*args, **kwargs)

def api_drop_improve_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_drop_improve`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_drop_improve_impl(*args, **kwargs)

def api_library_improve_all_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_library_improve_all_albums`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_improve_all_albums_impl(*args, **kwargs)

def api_library_improve_all_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_library_improve_all`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_improve_all_impl(*args, **kwargs)

def api_library_improve_all_progress_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``api_library_improve_all_progress`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return api_library_improve_all_progress_impl(*args, **kwargs)

def _match_type_from_flags_impl(
    *,
    strict_match_verified: bool,
    has_identity_hint: bool,
) -> str:
    if strict_match_verified:
        return "MATCH"
    if has_identity_hint:
        return "SOFT_MATCH"
    return "NO_MATCH"

def _record_files_match_audit_album_impl(
    *,
    album_id: int,
    folder_path: str = "",
    artist_name: str,
    album_title: str,
    run_kind: str,
    status: str,
    result: dict | None,
    steps: list[str] | None,
) -> bool:
    """Persist one manual rematch diagnostic row for an album."""
    if _get_library_mode() != "files":
        return False
    aid = int(album_id or 0)
    if aid <= 0:
        return False
    folder_path_txt = str(folder_path or "").strip()
    row_result = dict(result or {})
    step_lines = [str(s or "").strip() for s in (steps or []) if str(s or "").strip()]
    provider_used = _normalize_identity_provider(
        str(
            row_result.get("provider_used")
            or row_result.get("pmda_match_provider")
            or row_result.get("strict_match_provider")
            or ""
        )
    )
    strict_verified = bool(row_result.get("strict_match_verified"))
    has_identity_hint = bool(
        strict_verified
        or row_result.get("pmda_matched")
        or provider_used
        or str(row_result.get("discogs_release_id") or "").strip()
        or str(row_result.get("lastfm_album_mbid") or "").strip()
        or str(row_result.get("bandcamp_album_url") or "").strip()
    )
    match_type = _match_type_from_flags(
        strict_match_verified=strict_verified,
        has_identity_hint=has_identity_hint,
    )
    conf = None
    try:
        conf_raw = float(row_result.get("strict_tracklist_score") or 0.0)
        if conf_raw > 0.0:
            conf = max(0.0, min(1.0, conf_raw))
    except Exception:
        conf = None
    if conf is None and strict_verified:
        conf = 1.0
    ai_used = any(("ai" in s.lower()) or ("vision" in s.lower()) for s in step_lines)
    ai_conf = None
    for s in step_lines:
        m = re.search(r"confidence:\s*(\d+)", s, flags=re.IGNORECASE)
        if m:
            try:
                ai_conf = max(0, min(100, int(m.group(1))))
            except Exception:
                ai_conf = None
            if ai_conf is not None:
                break
    details = {
        "steps": step_lines[:120],
        "provider_attempts": _match_attempts_from_steps(step_lines, provider_used),
        "mutation_blocked": bool(row_result.get("mutation_blocked")),
        "mutation_blocked_reason": str(row_result.get("mutation_blocked_reason") or "").strip(),
        "strict": {
            "verified": strict_verified,
            "provider": _normalize_identity_provider(str(row_result.get("strict_match_provider") or "")),
            "reason": str(row_result.get("strict_reject_reason") or "").strip(),
            "tracklist_score": float(row_result.get("strict_tracklist_score") or 0.0),
        },
        "result": {
            "tags_updated": bool(row_result.get("tags_updated")),
            "cover_saved": bool(row_result.get("cover_saved")),
            "pmda_matched": bool(row_result.get("pmda_matched")),
            "pmda_cover": bool(row_result.get("pmda_cover")),
            "pmda_artist_image": bool(row_result.get("pmda_artist_image")),
            "pmda_complete": bool(row_result.get("pmda_complete")),
            "pmda_match_provider": _normalize_identity_provider(str(row_result.get("pmda_match_provider") or "")),
            "pmda_cover_provider": _normalize_identity_provider(str(row_result.get("pmda_cover_provider") or "")),
            "pmda_artist_provider": _normalize_identity_provider(str(row_result.get("pmda_artist_provider") or "")),
        },
    }
    conn = _files_pg_connect()
    if conn is None:
        return False
    try:
        if not folder_path_txt and aid > 0:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(folder_path, '') FROM files_albums WHERE id = %s LIMIT 1", (aid,))
                    folder_row = cur.fetchone()
                folder_path_txt = str((folder_row or [""])[0] or "").strip()
            except Exception:
                folder_path_txt = ""
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO files_match_audit(
                        album_id, folder_path, artist_name, album_title, run_kind, status,
                        match_type, confidence, ai_used, ai_confidence, provider_used,
                        summary, details_json, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        aid,
                        folder_path_txt or None,
                        str(artist_name or "").strip() or None,
                        str(album_title or "").strip() or None,
                        str(run_kind or "").strip() or "manual",
                        str(status or "").strip() or "completed",
                        match_type,
                        conf,
                        bool(ai_used),
                        ai_conf,
                        provider_used or None,
                        str(row_result.get("summary") or "").strip()[:1000] or None,
                        _json_dumps_safe(details),
                    ),
                )
        return True
    except Exception:
        logging.debug("Failed to record files_match_audit row for album_id=%s", aid, exc_info=True)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _serialize_match_audit_row_impl(row: tuple[Any, ...]) -> dict[str, Any]:
    details_raw = row[9] if len(row) > 9 else "{}"
    details = _safe_json_load(details_raw, fallback={})
    if not isinstance(details, dict):
        details = {}
    return {
        "id": int(row[0] or 0),
        "run_kind": str(row[1] or "").strip(),
        "status": str(row[2] or "").strip(),
        "match_type": str(row[3] or "").strip() or None,
        "confidence": float(row[4]) if row[4] is not None else None,
        "ai_used": bool(row[5]),
        "ai_confidence": int(row[6]) if row[6] is not None else None,
        "provider_used": _normalize_identity_provider(str(row[7] or "")) or None,
        "summary": str(row[8] or "").strip() or None,
        "details": details,
        "created_at": int(row[10] or 0),
    }

def _track_index_from_file_impl(audio_path: Path, tags: Optional[dict] = None) -> Optional[int]:
    """Derive track position from tags (TRCK) or filename (e.g. 01, 02). Returns 1-based index or None."""
    if tags is None:
        tags = extract_tags(audio_path)
    trck = (tags.get("trck") or tags.get("track") or "").strip()
    if trck:
        part = trck.split("/")[0].strip()
        try:
            return int(part)
        except ValueError:
            pass
    name = audio_path.stem
    match = re.search(r"\b(0*)(\d{1,3})\b", name)
    if match:
        try:
            return int(match.group(2))
        except ValueError:
            pass
    return None

def _infer_artist_album_from_folder_impl(folder_path: Path, audio_files: List[Path]) -> Tuple[str, str]:
    """Infer artist and album from first file tags, then folder name, then first filename."""
    artist_name = "Unknown"
    album_title_str = folder_path.name or "Unknown Album"
    folder_name_raw = str(folder_path.name or "").strip()

    def _clean_folder_token(token: str) -> str:
        txt = str(token or "").strip().replace("_", " ")
        txt = re.sub(r"\b(no\s*tags?|no\s*cover|gaps?|incomplete|broken|dupe|duplicate|run\s*source)\b", " ", txt, flags=re.IGNORECASE)
        txt = " ".join(txt.split())
        return txt.strip(" -_")

    if "__" in folder_name_raw:
        parts = [_clean_folder_token(p) for p in folder_name_raw.split("__")]
        parts = [p for p in parts if p]
        if len(parts) >= 2:
            guessed_artist = _identity_artist_fallback_candidate(parts[0]) or parts[0]
            guessed_album = parts[1]
            if guessed_artist:
                artist_name = guessed_artist
            if guessed_album:
                album_title_str = guessed_album
    if not audio_files:
        return (artist_name, album_title_str)
    tag_samples: list[dict] = []
    for sample_path in audio_files[: min(len(audio_files), 6)]:
        try:
            sample_tags = extract_tags(sample_path) or {}
        except Exception:
            sample_tags = {}
        if isinstance(sample_tags, dict):
            tag_samples.append(sample_tags)
    first_tags = tag_samples[0] if tag_samples else {}
    release_segment_parent = _folder_has_release_segment_children(folder_path, audio_files)
    sampled_artist = _pick_album_artist_from_tag_dicts(tag_samples, default=artist_name) if tag_samples else artist_name
    sampled_album = _pick_album_title_from_tag_dicts(tag_samples, fallback=album_title_str) if tag_samples else album_title_str
    if _identity_artist_fallback_is_usable(sampled_artist):
        artist_name = sampled_artist
    else:
        repaired_sampled_artist = _identity_artist_fallback_candidate(sampled_artist)
        if repaired_sampled_artist:
            artist_name = repaired_sampled_artist
    album_title_str = str(sampled_album or "").strip() or album_title_str
    filename_hints = _filename_identity_hints(audio_files)
    hinted_artist = str(filename_hints.get("artist") or "").strip()
    hinted_album = str(filename_hints.get("album") or "").strip()
    if _identity_artist_fallback_is_usable(hinted_artist) and not _identity_artist_fallback_is_usable(artist_name):
        artist_name = hinted_artist
    elif hinted_artist and not _identity_artist_fallback_is_usable(artist_name):
        repaired_hinted_artist = _identity_artist_fallback_candidate(hinted_artist)
        if repaired_hinted_artist:
            artist_name = repaired_hinted_artist
    if (
        hinted_album
        and not release_segment_parent
        and (not bool(_normalize_meta_text(first_tags.get("album"))) or album_title_str == folder_path.name)
    ):
        album_title_str = hinted_album
    # Folder-based fallback is more reliable than filename parsing for album identity.
    # For untagged albums, parent folder is almost always the artist.
    if not _identity_artist_fallback_is_usable(artist_name):
        try:
            parent_name = (folder_path.parent.name or "").replace("_", " ").strip()
        except Exception:
            parent_name = ""
        parent_artist = _identity_artist_fallback_candidate(parent_name)
        if parent_artist:
            artist_name = parent_artist
    if not _identity_artist_fallback_is_usable(artist_name) or album_title_str == folder_path.name:
        parts = audio_files[0].stem.split(" - ")
        if len(parts) >= 2:
            if not _identity_artist_fallback_is_usable(artist_name):
                fallback_artist = _identity_artist_fallback_candidate(parts[0])
                if fallback_artist:
                    artist_name = fallback_artist
            if album_title_str != folder_path.name or release_segment_parent:
                album_title_str = album_title_str
            else:
                album_title_str = parts[1].strip()
    album_title_str = _sanitize_album_title_display(album_title_str)
    return (artist_name, album_title_str)

def api_library_album_rematch_impl(album_id: int):
    """Manual rematch for one album (files mode) + persist audit row."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable"}), 503
    album_id = int(album_id or 0)
    if album_id <= 0:
        return jsonify({"error": "Invalid album id"}), 400

    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alb.folder_path,
                    alb.title,
                    art.name,
                    alb.strict_match_verified,
                    COALESCE(alb.strict_match_provider, ''),
                    COALESCE(alb.strict_reject_reason, ''),
                    COALESCE(alb.strict_tracklist_score, 0.0)
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE alb.id = %s
                LIMIT 1
                """,
                (album_id,),
            )
            row = cur.fetchone()
        if not row or not str(row[0] or "").strip():
            return jsonify({"error": "Album not found"}), 404
        folder_path = path_for_fs_access(Path(str(row[0]).strip()))
        album_title = str(row[1] or "").strip() or f"Album {album_id}"
        artist_name = str(row[2] or "").strip() or "Unknown Artist"
        strict_item = {
            "strict_match_verified": bool(row[3]),
            "strict_match_provider": _normalize_identity_provider(str(row[4] or "")),
            "strict_reject_reason": str(row[5] or "").strip(),
            "strict_tracklist_score": float(row[6] or 0.0),
        }
    finally:
        conn.close()

    if not folder_path.exists() or not folder_path.is_dir():
        return jsonify({"error": "Album folder not found on disk"}), 404

    strict_ok, strict_reason = _strict_mutation_allowed(strict_item)
    if not strict_ok:
        reason = strict_reason or "strict_match_missing"
        blocked = {
            "steps": [f"Mutation blocked: {reason}"],
            "summary": f"No mutation applied (strict gate): {reason}.",
            "tags_updated": False,
            "cover_saved": False,
            "provider_used": None,
            "files_updated": 0,
            "pmda_matched": False,
            "pmda_cover": False,
            "pmda_artist_image": False,
            "pmda_complete": False,
            "strict_match_verified": False,
            "strict_match_provider": "",
            "strict_reject_reason": reason,
            "strict_tracklist_score": 0.0,
            "mutation_blocked": True,
            "mutation_blocked_reason": reason,
        }
        _record_files_match_audit_album(
            album_id=album_id,
            artist_name=artist_name,
            album_title=album_title,
            run_kind="manual_album",
            status="blocked",
            result=blocked,
            steps=blocked.get("steps") or [],
        )
        blocked["audit_recorded"] = True
        return jsonify(blocked)

    result = _improve_folder_by_path(folder_path)
    result.setdefault("mutation_blocked", False)
    result.setdefault("mutation_blocked_reason", "")
    result.setdefault("strict_match_verified", bool(strict_item.get("strict_match_verified")))
    result.setdefault("strict_match_provider", strict_item.get("strict_match_provider") or "")
    result.setdefault("strict_reject_reason", strict_item.get("strict_reject_reason") or "")
    result.setdefault("strict_tracklist_score", float(strict_item.get("strict_tracklist_score") or 0.0))

    audit_recorded = _record_files_match_audit_album(
        album_id=album_id,
        artist_name=artist_name,
        album_title=album_title,
        run_kind="manual_album",
        status="completed",
        result=result,
        steps=result.get("steps") or [],
    )
    result["audit_recorded"] = bool(audit_recorded)
    _trigger_files_index_rebuild_async(reason="manual_album_rematch")
    return jsonify(result)

def api_library_artist_rematch_impl(artist_id: int):
    """Manual rematch for one artist (all albums) with audit logging enabled."""
    if _get_library_mode() != "files":
        return jsonify({"error": "Files mode required"}), 400
    with lock:
        if state.get("improve_all") and state["improve_all"].get("running"):
            return jsonify({"error": "Improve-all already running", "started": False}), 409
    ok, err = _ensure_files_index_ready()
    if not ok:
        return jsonify({"error": err or "Files index unavailable", "started": False}), 503
    artist_id = int(artist_id or 0)
    if artist_id <= 0:
        return jsonify({"error": "Invalid artist id", "started": False}), 400
    conn = _files_pg_connect()
    if conn is None:
        return jsonify({"error": "PostgreSQL unavailable", "started": False}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM files_artists WHERE id = %s", (artist_id,))
            arow = cur.fetchone()
            if not arow:
                return jsonify({"error": "Artist not found", "started": False}), 404
            artist_name = str(arow[0] or "").strip() or "Unknown Artist"
            cur.execute(
                """
                SELECT
                    id,
                    title,
                    folder_path,
                    musicbrainz_release_group_id,
                    strict_match_verified,
                    COALESCE(strict_match_provider, ''),
                    COALESCE(strict_reject_reason, ''),
                    COALESCE(strict_tracklist_score, 0.0)
                FROM files_albums
                WHERE id IN (
                    SELECT DISTINCT album_id
                    FROM files_artist_album_links
                    WHERE artist_id = %s
                )
                ORDER BY title ASC
                """,
                (artist_id,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return jsonify({"error": "No albums found for this artist", "started": False}), 404
    items = [
        {
            "artist": artist_name,
            "album_id": int(r[0]),
            "album_title": (r[1] or "").strip() or f"Album {int(r[0])}",
            "folder": (r[2] or "").strip(),
            "musicbrainz_id": (r[3] or "").strip(),
            "strict_match_verified": bool(r[4]),
            "strict_match_provider": _normalize_identity_provider(str(r[5] or "")),
            "strict_reject_reason": str(r[6] or "").strip(),
            "strict_tracklist_score": float(r[7] or 0.0),
            "_audit_enabled": True,
            "_run_kind": "manual_artist",
        }
        for r in rows
    ]
    thread = threading.Thread(
        target=_run_improve_all_albums_global,
        args=(items,),
        daemon=True,
    )
    thread.start()
    return jsonify({"started": True, "total": len(items)})

def api_library_improve_album_impl():
    """Improve a single album: query MusicBrainz for tags, update files, fetch cover if missing. Used by Fix column."""
    if _get_library_mode() == "files":
        data = request.get_json() or {}
        album_id = data.get("album_id")
        if not album_id:
            return jsonify({"error": "Missing album_id"}), 400
        try:
            album_id = int(album_id)
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid album_id"}), 400
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable"}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable"}), 503
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        folder_path,
                        strict_match_verified,
                        COALESCE(strict_match_provider, ''),
                        COALESCE(strict_reject_reason, ''),
                        COALESCE(strict_tracklist_score, 0.0)
                    FROM files_albums
                    WHERE id = %s
                    """,
                    (album_id,),
                )
                row = cur.fetchone()
            if not row or not (row[0] or "").strip():
                return jsonify({"error": "Album not found"}), 404
            folder_path = path_for_fs_access(Path(row[0]))
            strict_item = {
                "strict_match_verified": bool(row[1]),
                "strict_match_provider": _normalize_identity_provider(str(row[2] or "")),
                "strict_reject_reason": str(row[3] or "").strip(),
                "strict_tracklist_score": float(row[4] or 0.0),
            }
        finally:
            conn.close()
        if not folder_path.exists() or not folder_path.is_dir():
            return jsonify({"error": "Album folder not found on disk"}), 404
        strict_ok, strict_reason = _strict_mutation_allowed(strict_item)
        if not strict_ok:
            reason = strict_reason or "strict_match_missing"
            return jsonify(
                {
                    "steps": [f"Mutation blocked: {reason}"],
                    "summary": f"No mutation applied (strict gate): {reason}.",
                    "tags_updated": False,
                    "cover_saved": False,
                    "provider_used": None,
                    "files_updated": 0,
                    "pmda_matched": False,
                    "pmda_cover": False,
                    "pmda_artist_image": False,
                    "pmda_complete": False,
                    "strict_match_verified": False,
                    "strict_match_provider": "",
                    "strict_reject_reason": reason,
                    "strict_tracklist_score": 0.0,
                    "mutation_blocked": True,
                    "mutation_blocked_reason": reason,
                }
            )
        result = _improve_folder_by_path(folder_path)
        result.setdefault("mutation_blocked", False)
        result.setdefault("mutation_blocked_reason", "")
        result.setdefault("strict_match_verified", bool(strict_item.get("strict_match_verified")))
        result.setdefault("strict_match_provider", strict_item.get("strict_match_provider") or "")
        result.setdefault("strict_reject_reason", strict_item.get("strict_reject_reason") or "")
        result.setdefault("strict_tracklist_score", float(strict_item.get("strict_tracklist_score") or 0.0))
        _trigger_files_index_rebuild_async(reason="improve_album")
        return jsonify(result)

    return jsonify({
        "error": "Plex source improvement is disabled. Configure the files library and use Files album improvement endpoints."
    }), 410

def api_drop_improve_impl():
    """
    Accept multipart upload of audio files (one album). Save to temp dir, run improve-by-path,
    return result. Limits: max 50 files, 500 MB total. Temp dir deleted after success.
    """
    if "files" not in request.files and not request.files:
        return jsonify({"error": "No files uploaded"}), 400
    file_list = request.files.getlist("files") if request.files.get("files") else list(request.files.values())
    if not file_list or not any(f and f.filename for f in file_list):
        return jsonify({"error": "No files uploaded"}), 400
    allowed_ext = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".alac", ".ape", ".dsf", ".aif", ".aiff", ".wma", ".m4b", ".mp4"}
    total_size = 0
    files_to_save = []
    for f in file_list:
        if not f or not f.filename:
            continue
        base = os.path.basename(f.filename).strip()
        if not base or ".." in base or base.startswith("."):
            continue
        ext = Path(base).suffix.lower()
        if ext not in allowed_ext:
            continue
        try:
            f.stream.seek(0, 2)
            size = f.stream.tell()
            f.stream.seek(0)
        except Exception:
            size = 0
        total_size += size
        files_to_save.append((f, base))
    if len(files_to_save) > DROP_MAX_FILES:
        return jsonify({"error": f"Too many files (max {DROP_MAX_FILES})"}), 400
    if total_size > DROP_MAX_BYTES:
        return jsonify({"error": f"Total size too large (max {DROP_MAX_BYTES // (1024*1024)} MB)"}), 400
    if not files_to_save:
        return jsonify({"error": "No valid audio files"}), 400

    DROP_ALBUMS_BASE.mkdir(parents=True, exist_ok=True)
    temp_id = str(uuid.uuid4())[:8]
    temp_dir = DROP_ALBUMS_BASE / temp_id
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        for f, base in files_to_save:
            safe_name = re.sub(r'[^\w\s\-\.]', "_", base)[:200]
            dest = temp_dir / safe_name
            f.save(str(dest))
        result = _improve_folder_by_path(temp_dir)
        result.setdefault("mutation_blocked", False)
        result.setdefault("mutation_blocked_reason", "")
        result.setdefault("strict_match_verified", False)
        result.setdefault("strict_match_provider", "")
        result.setdefault("strict_reject_reason", "")
        result.setdefault("strict_tracklist_score", 0.0)
        return jsonify(result)
    except Exception as e:
        logging.exception("drop/improve failed: %s", e)
        return jsonify({"error": str(e), "steps": [], "summary": str(e), "tags_updated": False, "cover_saved": False, "provider_used": None, "dupes_in_folder": [], "files_updated": 0}), 500
    finally:
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            logging.debug("drop/improve cleanup %s: %s", temp_dir, e)

def api_library_improve_all_albums_impl():
    """Start improving all albums for an artist (MusicBrainz tags + cover)."""
    data = request.get_json() or {}
    artist_id = data.get("artist_id")
    if artist_id is None:
        return jsonify({"error": "Missing artist_id"}), 400
    try:
        artist_id = int(artist_id)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid artist_id"}), 400
    with lock:
        if state.get("improve_all") and state["improve_all"].get("running"):
            return jsonify({"error": "Improve-all already running", "started": False}), 409
    if _get_library_mode() == "files":
        ok, err = _ensure_files_index_ready()
        if not ok:
            return jsonify({"error": err or "Files index unavailable", "started": False}), 503
        conn = _files_pg_connect()
        if conn is None:
            return jsonify({"error": "PostgreSQL unavailable", "started": False}), 503
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM files_artists WHERE id = %s", (artist_id,))
                arow = cur.fetchone()
                if not arow:
                    return jsonify({"error": "Artist not found", "started": False}), 404
                artist_name = arow[0] or ""
                cur.execute(
                    """
                    SELECT
                        id,
                        title,
                        folder_path,
                        musicbrainz_release_group_id,
                        strict_match_verified,
                        COALESCE(strict_match_provider, ''),
                        COALESCE(strict_reject_reason, ''),
                        COALESCE(strict_tracklist_score, 0.0)
                    FROM files_albums
                    WHERE id IN (
                        SELECT DISTINCT album_id
                        FROM files_artist_album_links
                        WHERE artist_id = %s
                    )
                    ORDER BY title ASC
                    """,
                    (artist_id,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return jsonify({"error": "No albums found for this artist", "started": False}), 404
        items = [
            {
                "artist": artist_name,
                "album_id": int(r[0]),
                "album_title": (r[1] or "").strip() or f"Album {int(r[0])}",
                "folder": (r[2] or "").strip(),
                "musicbrainz_id": (r[3] or "").strip(),
                "strict_match_verified": bool(r[4]),
                "strict_match_provider": _normalize_identity_provider(str(r[5] or "")),
                "strict_reject_reason": str(r[6] or "").strip(),
                "strict_tracklist_score": float(r[7] or 0.0),
                "_audit_enabled": True,
                "_run_kind": "manual_artist",
            }
            for r in rows
        ]
        thread = threading.Thread(
            target=_run_improve_all_albums_global,
            args=(items,),
            daemon=True,
        )
        thread.start()
        return jsonify({"started": True, "total": len(items)})

    return jsonify({
        "error": "Plex source improvement is disabled. Configure the files library and use Files album improvement endpoints.",
        "started": False,
    }), 410

def api_library_improve_all_impl():
    """Start global 'Fix all albums': improve each 'best' edition from duplicate groups + all albums from last scan that have a MusicBrainz match (tags + cover + artist image from MB → Discogs → Last.fm → Bandcamp)."""
    if _get_library_mode() != "files":
        return jsonify({
            "error": "Plex source improvement is disabled. Configure the files library and use Files album improvement endpoints.",
            "started": False,
        }), 410
    with lock:
        if state.get("improve_all") and state["improve_all"].get("running"):
            return jsonify({"error": "Improve-all already running", "started": False}), 409
        if not state["duplicates"]:
            state["duplicates"] = load_scan_from_db()
        best_albums = []
        seen_ids = set()
        for artist_name, groups in state["duplicates"].items():
            for g in groups:
                best = g.get("best")
                if not best:
                    continue
                album_id = best.get("album_id")
                if album_id is None or album_id in seen_ids:
                    continue
                seen_ids.add(album_id)
                best_albums.append({
                    "artist": artist_name,
                    "album_id": album_id,
                    "album_title": best.get("title_raw") or best.get("album_norm") or f"Album {album_id}",
                    "musicbrainz_id": best.get("musicbrainz_id"),
                    "folder": str(best.get("folder") or "").strip(),
                    "strict_match_verified": bool(best.get("strict_match_verified")),
                    "strict_match_provider": best.get("strict_match_provider") or "",
                    "strict_reject_reason": best.get("strict_reject_reason") or "",
                    "strict_tracklist_score": float(best.get("strict_tracklist_score") or 0.0),
                })
        # Include all albums from the last scan so improve-all can enrich tags/covers
        # even when there is no MusicBrainz ID yet (for example Bandcamp/Last.fm-only matches
        # or new required tags such as genre).
        scan_id = get_last_completed_scan_id()
        if scan_id is not None:
            con = sqlite3.connect(str(STATE_DB_FILE))
            cur = con.cursor()
            try:
                cur.execute(
                    """
                    SELECT artist, album_id, title_raw, musicbrainz_id, folder,
                           strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score
                    FROM scan_editions
                    WHERE scan_id = ?
                    """,
                    (scan_id,),
                )
                rows = cur.fetchall()
                for row in rows:
                    artist_name = row[0]
                    album_id = row[1]
                    title_raw = row[2] or ""
                    mbid = (row[3] or "").strip()
                    folder = row[4] or ""
                    strict_match_verified = bool(row[5])
                    strict_match_provider = str(row[6] or "").strip()
                    strict_reject_reason = str(row[7] or "").strip()
                    try:
                        strict_tracklist_score = float(row[8] or 0.0)
                    except Exception:
                        strict_tracklist_score = 0.0
                    if album_id in seen_ids:
                        continue
                    seen_ids.add(album_id)
                    best_albums.append({
                        "artist": artist_name,
                        "album_id": album_id,
                        "album_title": (title_raw or "").strip() or f"Album {album_id}",
                        "musicbrainz_id": mbid or "",
                        "folder": (folder or "").strip(),
                        "strict_match_verified": strict_match_verified,
                        "strict_match_provider": strict_match_provider,
                        "strict_reject_reason": strict_reject_reason,
                        "strict_tracklist_score": strict_tracklist_score,
                    })
            except Exception as e:
                logging.debug("Fix-all: could not load scan_editions for extra albums: %s", e)
            finally:
                con.close()
    if not best_albums:
        return jsonify({"error": "No albums to fix (no duplicate groups and no scan with MusicBrainz matches). Run a scan first.", "started": False}), 404
    thread = threading.Thread(target=_run_improve_all_albums_global, args=(best_albums,), daemon=True)
    thread.start()
    return jsonify({"started": True, "total": len(best_albums)})

def api_library_improve_all_progress_impl():
    """Return current improve-all job progress (per-artist or global: running, current, total, result, error)."""
    with lock:
        prog = state.get("improve_all")
    if prog is None:
        return jsonify({"running": False, "finished": False})
    out = {
        "running": prog.get("running", False),
        "global": prog.get("global", False),
        "current": prog.get("current", 0),
        "total": prog.get("total", 0),
        "albums_processed": prog.get("current", 0),
        "total_albums": prog.get("total", 0),
        "current_album_id": prog.get("current_album_id"),
        "current_album": prog.get("current_album"),
        "current_artist": prog.get("current_artist"),
        "current_provider": prog.get("current_provider"),
        "provider_status": prog.get("provider_status", {}),
        "current_steps": prog.get("current_steps", []),
        "album_log": prog.get("log", []),
        "finished": not prog.get("running", True) and (prog.get("result") is not None or prog.get("error") is not None),
        "result": prog.get("result"),
        "error": prog.get("error"),
    }
    return jsonify(out)
