"""Runtime-bound duplicate best-edition selection helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_dupe_ai_cache_get',
    '_dupe_ai_cache_put',
    '_dupe_choose_best_heuristic',
    'process_ai_groups_batch',
    'choose_best',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[name] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
                if original is not None:
                    globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _dupe_ai_cache_get(artist: str, group_key: str) -> Optional[dict]:
    if not artist or not group_key:
        return None
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            SELECT best_folder, rationale, merge_list, ai_provider, ai_model, confidence
            FROM dupe_ai_cache
            WHERE artist = ? AND group_key = ?
            """,
            ((artist or "").strip(), (group_key or "").strip()),
        )
        row = cur.fetchone()
        con.close()
    except Exception:
        return None
    if not row:
        return None
    best_folder, rationale, merge_list_json, provider, model, confidence = row
    try:
        merge_list = json.loads(merge_list_json) if merge_list_json else []
        if not isinstance(merge_list, list):
            merge_list = []
    except Exception:
        merge_list = []
    conf = None
    try:
        if confidence is not None:
            conf = int(confidence)
    except Exception:
        conf = None
    return {
        "best_folder": (best_folder or "").strip(),
        "rationale": (rationale or "").strip(),
        "merge_list": merge_list,
        "ai_provider": (provider or "").strip(),
        "ai_model": (model or "").strip(),
        "confidence": conf,
    }

def _dupe_ai_cache_put(
    *,
    artist: str,
    group_key: str,
    best_folder: str,
    rationale: str,
    merge_list: list[str],
    ai_provider: str,
    ai_model: str,
    confidence: int | None,
) -> None:
    if not artist or not group_key:
        return
    now = time.time()
    try:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO dupe_ai_cache
              (artist, group_key, best_folder, rationale, merge_list, ai_provider, ai_model, confidence, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(artist, group_key) DO UPDATE SET
              best_folder = excluded.best_folder,
              rationale   = excluded.rationale,
              merge_list  = excluded.merge_list,
              ai_provider = excluded.ai_provider,
              ai_model    = excluded.ai_model,
              confidence  = excluded.confidence,
              updated_at  = excluded.updated_at
            """,
            (
                (artist or "").strip(),
                (group_key or "").strip(),
                (best_folder or "").strip(),
                (rationale or "").strip(),
                json.dumps(list(merge_list or [])),
                (ai_provider or "").strip(),
                (ai_model or "").strip(),
                int(confidence) if confidence is not None else None,
                now,
                now,
            ),
        )
        con.commit()
        con.close()
    except Exception:
        # Cache failures must never break the scan.
        return

def _dupe_choose_best_heuristic(editions: list[dict]) -> tuple[Optional[dict], str, list[str], bool]:
    """
    Deterministic fallback for choosing the "best" edition without AI.
    Returns (best, rationale, merge_list, confident).

    "Confident" is intentionally conservative: we only return confident=True when
    the pick is obviously superior (or identical track counts with better technical quality),
    so we can skip expensive AI calls without increasing wrong auto-moves.
    """
    if not editions:
        return (None, "", [], False)

    def _metrics(e: dict) -> tuple:
        # Deterministic quality/health signals (cheap):
        # - prefer editions with provider identity and MBIDs (less ambiguous)
        # - prefer editions with complete tags and a cover (better user experience and safer dedupe)
        has_identity = 1 if (
            _dupe_get_mb_release_group_id(e)
            or _dupe_get_mb_release_id(e)
            or _dupe_get_discogs_id(e)
            or _dupe_get_lastfm_mbid(e)
            or _dupe_get_bandcamp_url(e)
        ) else 0
        missing_required = e.get("missing_required_tags") or []
        has_complete_tags = 1 if (not missing_required) else 0
        has_cover = 1 if bool(e.get("has_cover")) else 0
        has_artist_image = 1 if bool(e.get("has_artist_image")) else 0
        missing_count_neg = -int(len(missing_required))

        # Prefer "clean" folder names when everything else is equal.
        # This is especially helpful for test sets and for user-created variants like "(no tags)".
        folder_raw = e.get("folder")
        folder_name = ""
        try:
            folder_name = str(Path(folder_raw).name if folder_raw else "").lower()
        except Exception:
            folder_name = str(folder_raw or "").lower()
        noisy_tokens = (
            "no tags",
            "no cover",
            "gaps",
            "incomplete",
            "broken",
            "[dupe]",
            " dupe",
            "(dupe)",
        )
        variant_clean = 1
        for tok in noisy_tokens:
            if tok and tok in folder_name:
                variant_clean = 0
                break

        fmt_score = int(e.get("fmt_score") or 0)
        bd = int(e.get("bd") or 0)
        sr = int(e.get("sr") or 0)
        br = int(e.get("br") or 0)
        track_count = len(e.get("tracks") or [])
        file_count = int(e.get("file_count") or 0)
        return (
            has_identity,
            has_complete_tags,
            has_cover,
            has_artist_image,
            variant_clean,
            missing_count_neg,
            fmt_score,
            bd,
            sr,
            br,
            track_count,
            file_count,
        )

    ranked = sorted(list(editions), key=_metrics, reverse=True)
    best = ranked[0]
    if len(ranked) == 1:
        (
            has_identity,
            has_complete_tags,
            has_cover,
            has_artist_image,
            variant_clean,
            missing_count_neg,
            fmt_score,
            bd,
            sr,
            br,
            track_count,
            file_count,
        ) = _metrics(best)
        rationale = (
            "Heuristic: single edition "
            f"(identity={has_identity}, tags={has_complete_tags}, cover={has_cover}, artist_img={has_artist_image}, "
            f"clean_name={variant_clean}, missing_score={missing_count_neg}, "
            f"fmt_score={fmt_score}, bd={bd}, sr={sr}, br={br}, tracks={track_count}, files={file_count})"
        )
        return (best, rationale, [], True)

    second = ranked[1]
    (b_ident, b_tags, b_cov, b_img, b_clean, b_miss, b_fmt, b_bd, b_sr, b_br, b_tr, b_fc) = _metrics(best)
    (s_ident, s_tags, s_cov, s_img, s_clean, s_miss, s_fmt, s_bd, s_sr, s_br, s_tr, s_fc) = _metrics(second)

    # Conservative confidence rules.
    confident = False

    # Never be "confident" if the best has fewer tracks than runner-up (deluxe/bonus vs standard ambiguity).
    if b_tr < s_tr:
        confident = False
    else:
        # Strong health signals should be enough to avoid AI.
        if (b_ident, b_tags, b_cov, b_img, b_clean) != (s_ident, s_tags, s_cov, s_img, s_clean):
            # If we have a clear improvement on identity/tags/cover, it's safe and deterministic.
            if (b_ident > s_ident) or (b_tags > s_tags) or (b_cov > s_cov) or (b_img > s_img) or (b_clean > s_clean):
                confident = True
        # Big codec/container class difference (e.g., FLAC vs MP3).
        if not confident and b_fmt >= (s_fmt + 2):
            confident = True
        # Same track count + better technical quality.
        elif (not confident) and b_tr == s_tr:
            if b_fmt > s_fmt:
                confident = True
            elif b_fmt == s_fmt:
                if b_bd >= (s_bd + 8):
                    confident = True
                elif b_sr >= (s_sr + 22050):
                    confident = True
                elif (b_bd == s_bd) and (b_sr == s_sr) and (b_br >= (s_br + 200000)):
                    confident = True
        # Slightly more tracks and not worse quality: often the same album + bonus.
        elif (not confident) and b_tr > s_tr and (b_tr - s_tr) <= 2:
            if (b_fmt >= s_fmt) and (b_bd >= s_bd) and (b_sr >= s_sr):
                confident = True

    # If all technical/health metrics are essentially tied but the group shares a strong identity signal,
    # treat the heuristic pick as confident to avoid unnecessary AI spend.
    if not confident and len(editions) >= 2:
        try:
            discogs_ids = {_dupe_get_discogs_id(e) for e in editions}
            discogs_ids = {x for x in discogs_ids if x}
            lastfm_ids = {_dupe_get_lastfm_mbid(e) for e in editions}
            lastfm_ids = {x for x in lastfm_ids if x}
            bandcamp_urls = {_dupe_get_bandcamp_url(e) for e in editions}
            bandcamp_urls = {x for x in bandcamp_urls if x}
            mb_rg_ids = {_dupe_get_mb_release_group_id(e) for e in editions}
            mb_rg_ids = {x for x in mb_rg_ids if x}
            strong = (
                (len(mb_rg_ids) == 1 and mb_rg_ids)
                or (len(discogs_ids) == 1 and discogs_ids)
                or (len(lastfm_ids) == 1 and lastfm_ids)
                or (len(bandcamp_urls) == 1 and bandcamp_urls)
            )
            if strong and b_tr == s_tr and b_fc == s_fc:
                confident = True
        except Exception:
            pass

    rationale = (
        "Heuristic pick: "
        f"identity {b_ident} vs {s_ident}, "
        f"tags {b_tags} vs {s_tags}, "
        f"cover {b_cov} vs {s_cov}, "
        f"artist_img {b_img} vs {s_img}, "
        f"clean_name {b_clean} vs {s_clean}, "
        f"fmt_score {b_fmt} vs {s_fmt}, "
        f"bd {b_bd} vs {s_bd}, "
        f"sr {b_sr} vs {s_sr}, "
        f"br {b_br} vs {s_br}, "
        f"tracks {b_tr} vs {s_tr}"
    )
    return (best, rationale, [], confident)

def process_ai_groups_batch(ai_groups: List[dict], max_workers: int = None) -> List[dict]:
    """
    Process multiple groups requiring AI in parallel using choose_best().
    Returns list of completed group dicts with 'best' and 'losers' set.

    This function is deliberately tolerant: when AI fails for a group, the error
    is recorded in state["scan_ai_errors"] but the scan continues for other groups.
    """
    if not ai_groups:
        return []

    from concurrent.futures import ThreadPoolExecutor, as_completed

    total = len(ai_groups)
    workers = max_workers or min(10, total)
    results: List[dict] = []

    def _process_one(group: dict) -> Optional[dict]:
        artist = group.get("artist") or ""
        editions = group.get("editions") or []
        title = ""
        if editions:
            first = editions[0]
            title = first.get("title_raw") or first.get("album_norm") or ""
        group_label = f"{artist} – {title}" if artist or title else "unknown group"
        try:
            best = choose_best(editions, defer_ai=False)
            if not best:
                return None
            losers = [e for e in editions if e.get("album_id") != best.get("album_id")]

            # Preserve group-level evidence/flags for explainability + safe pipeline behavior.
            dupe_evidence = group.get("dupe_evidence")
            if dupe_evidence and isinstance(best, dict):
                try:
                    best["dupe_evidence"] = list(dupe_evidence)
                except Exception:
                    pass

            res = {
                "artist": artist,
                "album_id": best.get("album_id"),
                "best": best,
                "losers": losers,
                "fuzzy": group.get("fuzzy", False),
                "needs_ai": False,
            }
            for k in ("dupe_signal", "dupe_evidence", "same_folder", "no_move", "manual_review"):
                if k in group:
                    res[k] = group.get(k)
            return res
        except Exception as e:
            logging.error("[AI Batch] Error processing group for %s: %s", group_label, e)
            try:
                with lock:
                    state.setdefault("scan_ai_errors", []).append(
                        {"message": str(e), "group": group_label}
                    )
                    if len(state["scan_ai_errors"]) > 100:
                        state["scan_ai_errors"] = state["scan_ai_errors"][-80:]
            except Exception:
                pass
            return None

    processed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_group = {executor.submit(_process_one, g): g for g in ai_groups}
        for future in as_completed(future_to_group):
            res = future.result()
            processed += 1
            try:
                with lock:
                    state["scan_ai_batch_processed"] = processed
            except Exception:
                pass
            if res:
                results.append(res)

    return results

def choose_best(editions: List[dict], defer_ai: bool = False) -> dict | None:
    """Select the best edition for a duplicate group.

    Strategy:
    - Reuse stable AI cache when the exact same folder set appears again (dupe_ai_cache).
    - Prefer a conservative deterministic heuristic when the pick is obvious.
    - Otherwise call the AI provider (unless defer_ai=True), and cache the result.
    - On AI failure, fall back to the heuristic instead of dropping the group.
    """
    if not editions:
        return None

    # 0) Filter out broken albums if there are non-broken alternatives.
    non_broken = [e for e in editions if not e.get('is_broken', False)]
    broken = [e for e in editions if e.get('is_broken', False)]
    if non_broken and broken:
        try:
            log_dupes(
                "Filtering out %d broken album(s) in favor of %d non-broken album(s) for artist=%s",
                len(broken),
                len(non_broken),
                editions[0].get('artist', '?'),
            )
        except Exception:
            pass
        editions = non_broken

    artist = str((editions[0] or {}).get('artist') or '').strip()
    group_key = _dupe_group_key_from_editions(editions)

    # 1) AI cache reuse.
    cached = _dupe_ai_cache_get(artist, group_key)
    if cached and cached.get('best_folder'):
        best_folder_key = _dupe_folder_key_str(cached.get('best_folder'))
        best_cached = next(
            (e for e in editions if _dupe_folder_key_str((e or {}).get('folder')) == best_folder_key),
            None,
        )
        if best_cached is not None:
            best_cached['rationale'] = cached.get('rationale') or 'AI cache'
            best_cached['merge_list'] = cached.get('merge_list') or []
            best_cached['used_ai'] = True
            best_cached['ai_provider'] = cached.get('ai_provider') or ''
            best_cached['ai_model'] = cached.get('ai_model') or ''
            if cached.get('confidence') is not None:
                best_cached['ai_confidence'] = cached.get('confidence')
            return best_cached

    # 2) Heuristic fast path.
    h_best, h_rationale, h_merge, h_confident = _dupe_choose_best_heuristic(editions)
    if h_best is not None and (h_confident or not ai_provider_ready):
        h_best['rationale'] = h_rationale
        h_best['merge_list'] = h_merge or []
        h_best['used_ai'] = False
        h_best['ai_provider'] = ''
        h_best['ai_model'] = ''
        return h_best

    # 3) AI path for ambiguous groups.
    if ai_provider_ready and bool(getattr(sys.modules[__name__], "USE_AI_FOR_DEDUPE", True)):
        if defer_ai:
            return None

        template = AI_PROMPT_FILE.read_text(encoding='utf-8')
        user_msg = template + "\nCandidate editions:\n"
        for idx, e in enumerate(editions):
            track_count = len(e.get('tracks', []))
            folder_path = path_for_fs_access(Path(str(e.get("storage_access_path") or e.get('folder')))) if e.get('folder') else None
            size_mb = (safe_folder_size(folder_path) // (1024 * 1024)) if folder_path else 0
            folder_name = ""
            try:
                folder_name = str(Path(e.get('folder') or "").name or "").strip()
            except Exception:
                folder_name = str(e.get('folder') or "").strip()
            title_raw = str(e.get("title_raw") or e.get("album_norm") or "").strip()
            track_preview_rows = list(e.get("tracks") or [])[:8]

            def _track_preview_title(row: Any) -> str:
                if isinstance(row, dict):
                    return str(row.get("title") or row.get("name") or "").strip()
                return str(getattr(row, "title", "") or getattr(row, "name", "") or "").strip()

            track_preview_titles = [
                title
                for title in (_track_preview_title(row) for row in track_preview_rows)
                if title
            ]
            track_preview = ", ".join(track_preview_titles)
            if track_preview and len(list(e.get("tracks") or [])) > len(track_preview_rows):
                track_preview += ", ..."
            missing_required = list(e.get("missing_required_tags") or [])
            has_cover = bool(e.get("has_cover"))
            has_artist_image = bool(e.get("has_artist_image"))
            user_msg += (
                f"{idx}: title={title_raw!r}, folder={folder_name!r}, fmt_score={e.get('fmt_score', 0)}, bitdepth={e.get('bd', 0)}, "
                f"tracks={track_count}, track_count={track_count}, size_mb={size_mb}, files={e.get('file_count', 0)}, "
                f"bitrate={e.get('br', 0)}, samplerate={e.get('sr', 0)}, duration={e.get('dur', 0)}, "
                f"has_cover={has_cover}, has_artist_image={has_artist_image}, missing_required_tags={len(missing_required)}"
            )
            meta = e.get('meta') or {}
            year = meta.get('date') or meta.get('originaldate') or ''
            mbid = meta.get('musicbrainz_albumid', '')
            user_msg += f" year={year} mbid={mbid}"
            if track_preview:
                user_msg += f" track_preview=[{track_preview}]"
            if missing_required:
                user_msg += f" missing_tags={','.join(str(x or '').strip() for x in missing_required if str(x or '').strip())}"
            user_msg += "\n"

        rg_info = None
        for e in editions:
            if e.get('rg_info'):
                rg_info = e['rg_info']
                break
        if rg_info:
            user_msg += (
                'Release group info: '
                f"primary_type={rg_info.get('primary_type', '')}, "
                f"formats={rg_info.get('format_summary', '')}\n"
            )

        system_msg = (
            'You are an expert digital-music librarian.\n'
            'OUTPUT RULES (must follow exactly):\n'
            "- Return ONE single line only.\n"
            "- The line must contain EXACTLY two '|' characters.\n"
            "- Format: <index>|<brief rationale>|<comma-separated extra tracks>\n"
            "- Index is 0-based: 0 = first edition, 1 = second, etc. (candidates are listed as 0:, 1:, ...).\n"
            "- If there are no extra tracks, still include the final pipe but leave it empty.\n"
            "- Do not add any other text.\n"
            'Optionally end the rationale with (confidence: N) where N is 0-100.\n'
        )

        mod = sys.modules[__name__]
        model_to_use = getattr(mod, 'RESOLVED_MODEL', None) or OPENAI_MODEL or 'gpt-4o-mini'
        model_display = getattr(mod, 'RESOLVED_MODEL', None) or OPENAI_MODEL or model_to_use

        ai_confidence = None
        try:
            txt = _call_ai_provider_bounded(
                provider=AI_PROVIDER,
                model=model_to_use,
                system_msg=system_msg,
                user_msg=user_msg,
                max_tokens=256,
                analysis_type="dedupe_choose_best",
                timeout_sec=AI_SCAN_HARD_TIMEOUT_SEC,
                log_prefix="[Dedupe AI]",
            )
            lines = [l.strip() for l in (txt or '').replace('```', '').splitlines() if l.strip()]
            txt = lines[0] if lines else (txt or '')
            txt = re.sub(r'^(answer|réponse)\s*:\s*', '', txt, flags=re.IGNORECASE).strip()

            m = re.match(r'^(\d+)\s*\|\s*(.*?)\s*\|\s*(.*)$', txt)
            if m:
                idx = int(m.group(1))
                if idx == len(editions) and len(editions) > 1:
                    idx -= 1
                idx = max(0, min(len(editions) - 1, idx))
                rationale = m.group(2).strip()
                extras_raw = m.group(3).strip()
                merge_list = [t.strip() for t in extras_raw.split(',') if t.strip()]
                m_conf = re.search(r'\s*\(confidence:\s*(\d+)\)\s*$', rationale, re.I)
                if m_conf:
                    ai_confidence = min(100, max(0, int(m_conf.group(1))))
                    rationale = rationale[: m_conf.start()].strip()
            else:
                m_num = re.search(r'(\d+)', txt)
                if not m_num:
                    raise ValueError(f'Invalid AI response format (no index found) – got: {txt!r}')
                idx = int(m_num.group(1))
                if idx == len(editions) and len(editions) > 1:
                    idx -= 1
                idx = max(0, min(len(editions) - 1, idx))
                rationale = 'minimal AI reply; fallback parser used'
                merge_list = []

            best = editions[idx]
            best.update({
                'rationale': rationale,
                'merge_list': merge_list,
                'used_ai': True,
                'ai_provider': (AI_PROVIDER or ''),
                'ai_model': (model_display or ''),
            })
            if ai_confidence is not None:
                best['ai_confidence'] = ai_confidence

            _dupe_ai_cache_put(
                artist=artist,
                group_key=group_key,
                best_folder=_dupe_folder_key_str(best.get('folder')),
                rationale=rationale,
                merge_list=merge_list,
                ai_provider=(AI_PROVIDER or ''),
                ai_model=(model_display or ''),
                confidence=ai_confidence,
            )
            return best
        except Exception as e:
            group_label = f"{artist} – {editions[0].get('title_raw', editions[0].get('album_norm', ''))}" if editions else (artist or '')
            with lock:
                state.setdefault('scan_ai_errors', []).append({'message': str(e), 'group': group_label})
                if len(state['scan_ai_errors']) > 100:
                    state['scan_ai_errors'] = state['scan_ai_errors'][-80:]
            logging.warning('AI failed for dupe group (%s): %s; falling back to heuristic', group_label, e)

    # 4) Final fallback: heuristic even if not confident.
    if h_best is not None:
        h_best['rationale'] = h_rationale
        h_best['merge_list'] = h_merge or []
        h_best['used_ai'] = False
        h_best['ai_provider'] = ''
        h_best['ai_model'] = ''
        return h_best

    return None
_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def _dupe_ai_cache_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dupe_ai_cache_get(*args, **kwargs)

def _dupe_ai_cache_put_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dupe_ai_cache_put(*args, **kwargs)

def _dupe_choose_best_heuristic_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _dupe_choose_best_heuristic(*args, **kwargs)

def process_ai_groups_batch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return process_ai_groups_batch(*args, **kwargs)

def choose_best_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return choose_best(*args, **kwargs)
