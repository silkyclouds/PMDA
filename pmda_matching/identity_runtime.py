"""Runtime-bound album identity, MusicBrainz, and provider cross-check helpers."""
from __future__ import annotations

import html
import re
import unicodedata
from typing import Any

_EXTRACTED_NAMES = {
    '_normalize_identity_text_strict',
    '_normalize_identity_album_strict',
    '_split_identity_artist_credits',
    '_strip_identity_artist_feature_clause',
    '_identity_artist_credit_norms',
    '_identity_norm_is_various_artist',
    '_identity_artist_is_various_artists',
    '_identity_tags_mark_compilation',
    '_identity_artist_credit_overlap',
    '_strip_identity_album_trailing_markers',
    '_identity_album_variant_norms',
    '_identity_album_equivalent',
    '_provider_identity_album_score',
    '_provider_identity_artist_score',
    '_extract_mb_artist_names',
    'ai_verify_mb_match',
    '_identify_album_by_acoustic_id',
    '_musicbrainz_artist_identity_lookup',
    '_classical_track_entries',
    '_classical_total_duration_ms_for_paths',
    '_classical_identity_context',
    '_provider_classical_context',
    '_classical_context_for_edition',
    '_classical_identity_match_details',
    '_strict_identity_match_details',
    '_score_musicbrainz_release_payload_for_local_context',
    '_fetch_musicbrainz_strict_payload',
    '_fetch_musicbrainz_strict_payload_for_edition',
    '_strict_expected_provider_id',
    '_strict_payload_for_provider',
    '_strict_provider_cold_fetch_allowed',
    '_strict_validate_edition_match',
    '_strict_clear_identity_on_reject',
    '_resolve_edition_display_identity',
    '_infer_identity_from_local_context_ai',
    '_extract_files_identity_fields',
    '_build_album_provider_crosscheck',
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


def _normalize_identity_text_strict(value: str | None) -> str:
    """
    Normalize identity text for strict equality checks.
    Accent-insensitive, punctuation-insensitive normalization for deterministic equality checks.
    """
    raw = _fold_music_ascii(html.unescape(str(value or "")))
    if not raw:
        return ""
    text = unicodedata.normalize("NFKD", unicodedata.normalize("NFKC", raw))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).casefold().strip()
    if not text:
        return ""
    text = text.replace("&", " and ").replace("_", " ")
    text = re.sub(r"[`´’']", "", text)
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = " ".join(text.split())
    if text:
        return text
    # Keep symbol-only identities deterministic (e.g. "!!!", "/\\/\\/\\")
    # so they can still match identical provider strings.
    symbol_fallback = re.sub(r"\s+", "", unicodedata.normalize("NFKC", raw).casefold()).strip()
    if symbol_fallback:
        return f"sym:{symbol_fallback[:240]}"
    return ""


def _normalize_identity_album_strict(value: str | None) -> str:
    """
    Normalize album title for strict identity checks while ignoring format/version suffixes.
    """
    raw = html.unescape(str(value or ""))
    if not raw:
        return ""
    dedup_norm = norm_album_for_dedup(raw, normalize_parenthetical=True)
    return _normalize_identity_text_strict(dedup_norm)


_IDENTITY_ARTIST_SPLIT_RE = re.compile(
    r"\s*(?:,|;|/|&|\+|×|\bx\b|\bfeat(?:uring)?\.?\b|\bft\.?\b|\bwith\b|\bvs\.?\b|\bversus\b|\bmeets\b)\s*",
    flags=re.IGNORECASE,
)
_IDENTITY_ARTIST_PREFIX_RE = re.compile(
    r"^(?:feat(?:uring)?\.?|ft\.?|with|vs\.?|versus)\s+",
    flags=re.IGNORECASE,
)
_IDENTITY_ARTIST_FEATURE_TRAIL_RE = re.compile(
    r"\s*(?:[\[(]\s*)?(?:feat(?:uring)?\.?|ft\.?|with)\b.*$",
    flags=re.IGNORECASE,
)
_IDENTITY_ALBUM_TRAILING_MARKER_PATTERNS = (
    r"original soundtrack",
    r"soundtrack(?:\s+from\b.*)?",
    r"ost",
    r"ep",
    r"lp",
    r"single",
    r"deluxe(?: edition)?",
    r"expanded(?: edition)?",
    r"anniversary(?: edition)?",
    r"remaster(?:ed)?",
    r"reissue",
)
_IDENTITY_ALBUM_TRAILING_CATALOG_RE = re.compile(
    r"(?:\s+[-–—:]\s+|\s+)(?:[A-Z]{2,6}[- ]?\d{2,8}|[A-Z]{2,6}\d{2,8})\s*$",
)
_IDENTITY_VARIOUS_ARTIST_NORMS = {
    "va",
    "v a",
    "vvaa",
    "various",
    "various artist",
    "various artists",
    "various artistes",
    "various interprets",
    "various performers",
    "various musicians",
    "various productions",
    "compilation",
    "compilations",
}


def _split_identity_artist_credits(value: str | None) -> list[str]:
    raw = re.sub(r"\s+", " ", html.unescape(str(value or "")).strip())
    if not raw:
        return []
    parts = _IDENTITY_ARTIST_SPLIT_RE.split(raw)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        clean = _IDENTITY_ARTIST_PREFIX_RE.sub("", str(part or "").strip(" \t\r\n-–—,;:/\\()[]{}")).strip()
        clean = re.sub(r"\s+", " ", clean).strip()
        if not clean:
            continue
        norm = _normalize_identity_text_strict(clean)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(clean)
    return out or ([raw] if raw else [])


def _strip_identity_artist_feature_clause(value: str | None) -> str:
    raw = re.sub(r"\s+", " ", html.unescape(str(value or "")).strip())
    if not raw:
        return ""
    stripped = _IDENTITY_ARTIST_FEATURE_TRAIL_RE.sub("", raw).strip(" \t\r\n-–—,;:/\\()[]{}")
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped or raw


def _identity_artist_credit_norms(value: str | None) -> list[str]:
    norms: list[str] = []
    seen: set[str] = set()
    base_value = str(value or "")
    feature_stripped = _strip_identity_artist_feature_clause(base_value)
    candidates: list[str] = [base_value]
    if feature_stripped and feature_stripped != base_value:
        candidates.append(feature_stripped)
    for source in (base_value, feature_stripped):
        candidates.extend(_split_identity_artist_credits(source))
    for candidate in candidates:
        norm = _normalize_identity_text_strict(candidate)
        if norm and norm not in seen:
            seen.add(norm)
            norms.append(norm)
    return norms


def _identity_norm_is_various_artist(value: str | None) -> bool:
    norm = _normalize_identity_text_strict(value)
    if not norm:
        return False
    if norm in _IDENTITY_VARIOUS_ARTIST_NORMS:
        return True
    compact = re.sub(r"\s+", "", norm)
    return compact in {"va", "vvaa", "variousartists", "variousartist"}


def _identity_artist_is_various_artists(value: str | None) -> bool:
    if _identity_norm_is_various_artist(value):
        return True
    return any(_identity_norm_is_various_artist(part) for part in _split_identity_artist_credits(value))


def _identity_tags_mark_compilation(tags: dict | None) -> bool:
    if not isinstance(tags, dict):
        return False
    lower = {str(k or "").strip().lower(): v for k, v in tags.items()}
    for key in ("compilation", "itunescompilation", "albumartistssort", "releasetype", "release_type"):
        raw = lower.get(key)
        if raw is None:
            continue
        values = raw if isinstance(raw, (list, tuple, set)) else [raw]
        for value in values:
            txt = str(value or "").strip().lower()
            if txt in {"1", "true", "yes", "y", "compilation", "various artists"}:
                return True
    return False


def _identity_artist_credit_overlap(local_artist: str, candidate_artist: str | None) -> tuple[bool, bool]:
    local_parts = {
        norm
        for norm in (_normalize_identity_text_strict(part) for part in _split_identity_artist_credits(local_artist))
        if norm
    }
    candidate_parts = {
        norm
        for norm in (_normalize_identity_text_strict(part) for part in _split_identity_artist_credits(candidate_artist))
        if norm
    }
    if not local_parts or not candidate_parts:
        return (False, False)
    overlap = local_parts & candidate_parts
    equal_multi = bool(len(overlap) >= 2 and overlap == local_parts == candidate_parts)
    partial = bool(
        overlap
        and (
            len(overlap) == 1
            or (len(local_parts) == 1 and overlap == local_parts)
            or (len(candidate_parts) == 1 and overlap == candidate_parts)
            or (len(local_parts) >= 2 and overlap == local_parts)
            or (len(candidate_parts) >= 2 and overlap == candidate_parts)
            or len(overlap) >= 2
        )
    )
    return (equal_multi, partial)


def _strip_identity_album_trailing_markers(value: str | None) -> str:
    txt = re.sub(r"\s+", " ", html.unescape(str(value or "")).strip())
    if not txt:
        return ""
    out = txt
    while True:
        prior = out
        out = _IDENTITY_ALBUM_TRAILING_CATALOG_RE.sub("", out).strip() or out
        for pattern in _IDENTITY_ALBUM_TRAILING_MARKER_PATTERNS:
            updated = re.sub(rf"\s*[\[(]\s*{pattern}\s*[\])]\s*$", "", out, flags=re.IGNORECASE).strip()
            if updated and updated != out:
                out = updated
            updated = re.sub(rf"(?:\s+[-–—:]\s+|\s+){pattern}\s*$", "", out, flags=re.IGNORECASE).strip()
            if updated and updated != out:
                out = updated
        if out == prior:
            break
    return out.strip() or txt


def _identity_album_variant_norms(value: str | None, *, artist_hints: list[str] | tuple[str, ...] | None = None) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    variants: set[str] = set()
    for candidate in {raw, strip_parenthetical_suffixes(raw), _strip_identity_album_trailing_markers(raw)}:
        norm = _normalize_identity_album_strict(candidate)
        if norm:
            variants.add(norm)
    artist_norms = [
        _normalize_identity_text_strict(item)
        for item in (artist_hints or [])
        if _normalize_identity_text_strict(item)
    ]
    derived = list(variants)
    for variant in derived:
        for artist_norm in artist_norms:
            if artist_norm and variant.startswith(f"{artist_norm} "):
                remainder = variant[len(artist_norm):].strip()
                if remainder:
                    variants.add(remainder)
    return {item for item in variants if item}


def _identity_album_equivalent(
    local_title: str | None,
    candidate_title: str | None,
    *,
    artist_hints: list[str] | tuple[str, ...] | None = None,
) -> bool:
    local_variants = _identity_album_variant_norms(local_title, artist_hints=artist_hints)
    candidate_variants = _identity_album_variant_norms(candidate_title, artist_hints=artist_hints)
    if not local_variants or not candidate_variants:
        return False
    if local_variants & candidate_variants:
        return True
    for local_variant in local_variants:
        for candidate_variant in candidate_variants:
            if (
                len(local_variant) >= 8
                and len(candidate_variant) >= 8
                and (local_variant in candidate_variant or candidate_variant in local_variant)
            ):
                return True
    return False


def _provider_identity_album_score(
    local_title: str,
    provider_title: str,
    *,
    artist_hints: list[str] | tuple[str, ...] | None = None,
) -> float:
    best = float(_provider_identity_text_score(local_title, provider_title))
    local_variants = _identity_album_variant_norms(local_title, artist_hints=artist_hints)
    provider_variants = _identity_album_variant_norms(provider_title, artist_hints=artist_hints)
    if not local_variants or not provider_variants:
        return best
    if local_variants & provider_variants:
        return max(best, 1.0)
    try:
        from difflib import SequenceMatcher
    except Exception:
        SequenceMatcher = None  # type: ignore[assignment]
    for local_variant in local_variants:
        for provider_variant in provider_variants:
            if (
                len(local_variant) >= 8
                and len(provider_variant) >= 8
                and (local_variant in provider_variant or provider_variant in local_variant)
            ):
                best = max(best, 0.96)
            elif SequenceMatcher is not None:
                try:
                    best = max(best, float(SequenceMatcher(None, local_variant, provider_variant).ratio()))
                except Exception:
                    pass
    return max(0.0, min(1.0, float(best)))


def _provider_identity_artist_score(local_artist: str, provider_artist: str) -> float:
    best = float(_provider_identity_text_score(local_artist, provider_artist))
    local_norms = _identity_artist_credit_norms(local_artist)
    provider_norms = _identity_artist_credit_norms(provider_artist)
    if not local_norms or not provider_norms:
        return best
    if any(local_norm == provider_norm for local_norm in local_norms for provider_norm in provider_norms):
        best = max(best, 1.0)
    equal_multi, partial_overlap = _identity_artist_credit_overlap(local_artist, provider_artist)
    if equal_multi:
        best = max(best, 1.0)
    elif partial_overlap:
        best = max(best, 0.92)
    local_parts = {norm for norm in local_norms[1:] if norm}
    provider_parts = {norm for norm in provider_norms[1:] if norm}
    if local_parts and provider_parts:
        overlap = local_parts & provider_parts
        if len(local_parts) == 1 and overlap:
            best = max(best, 0.89)
        if len(provider_parts) == 1 and overlap:
            best = max(best, 0.89)
    return max(0.0, min(1.0, float(best)))


def _extract_mb_artist_names(payload: dict | None) -> list[str]:
    """Extract readable artist names from a MusicBrainz payload."""
    if not isinstance(payload, dict):
        return []
    out: list[str] = []
    artist_credit = payload.get("artist-credit")
    if isinstance(artist_credit, list):
        for item in artist_credit:
            if not isinstance(item, dict):
                continue
            nm = (item.get("name") or (item.get("artist") or {}).get("name") or "").strip()
            if nm:
                out.append(nm)
    phrase = (payload.get("artist-credit-phrase") or "").strip()
    if phrase:
        out.append(phrase)
    deduped: list[str] = []
    seen = set()
    for name in out:
        key = _normalize_identity_text_strict(name)
        if key and key not in seen:
            deduped.append(name)
            seen.add(key)
    base_names = list(deduped)
    if len(base_names) >= 2:
        deduped.append(" & ".join(base_names))
        deduped.append("; ".join(base_names))
    return deduped


def ai_verify_mb_match(
    artist: str,
    title_raw: Optional[str],
    title_norm: str,
    track_titles: Optional[List[str]],
    track_count: int,
    candidates: List[tuple],
    has_cover: bool = False,
    extra_sources: Optional[List[dict]] = None,
) -> tuple[Optional[tuple], Optional[int]]:
    """
    Ask the AI to pick which MusicBrainz candidate matches our album (or NONE).
    candidates: list of (rg, result_dict) where rg has 'title','id', result_dict has 'id','track_count'.
    extra_sources: optional list of {"source": "Discogs"|"Last.fm"|"Bandcamp", "title": ..., "artist": ...} for disambiguation.
    Returns (chosen_candidate_or_None, confidence_or_None). When confidence is below AI_CONFIDENCE_MIN, returns (None, conf) so caller can try other sources.
    """
    if not getattr(sys.modules[__name__], "USE_AI_FOR_MB_VERIFY", False):
        return (None, None)
    if not getattr(sys.modules[__name__], "ai_provider_ready", False):
        return (None, None)
    if not candidates:
        return (None, None)
    # Keep prompts bounded: MB search can return many candidates and full tracklists are token-expensive.
    # These caps dramatically reduce input tokens while preserving enough signal for disambiguation.
    max_candidates = int(getattr(sys.modules[__name__], "AI_MB_VERIFY_MAX_CANDIDATES", 8) or 8)
    max_local_tracks = int(getattr(sys.modules[__name__], "AI_MB_VERIFY_LOCAL_TRACK_PREVIEW", 12) or 12)
    max_mb_tracks = int(getattr(sys.modules[__name__], "AI_MB_VERIFY_MB_TRACK_PREVIEW", 5) or 5)
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    choices = []
    for i, (rg, result_dict) in enumerate(candidates[:max_candidates]):
        tc = result_dict.get("track_count", "?")
        line = f"{letters[i]}: {rg.get('title', 'Unknown')} (id={rg.get('id', '')}, {tc} tracks)"
        mb_tracks = result_dict.get("track_titles")
        if mb_tracks:
            track_preview = ", ".join(mb_tracks[:max_mb_tracks]) + ("..." if len(mb_tracks) > max_mb_tracks else "")
            line += f" — tracks: [{track_preview}]"
        choices.append(line)
    track_list_str = ", ".join((track_titles or [])[:max_local_tracks]) if track_titles else "(none)"
    if track_titles and len(track_titles) > max_local_tracks:
        track_list_str += ", ..."
    user_msg = (
        f"Album: artist={artist!r}; title={title_raw or title_norm!r}; norm={title_norm!r}; "
        f"tracks={track_count}; local_tracks=[{track_list_str}]; cover={has_cover}.\n"
        "Candidates:\n" + "\n".join(choices)
    )
    if extra_sources:
        other_lines = []
        for s in extra_sources:
            src = s.get("source", "?")
            t = s.get("title") or s.get("album") or "?"
            a = s.get("artist") or s.get("artist_name") or "?"
            other_lines.append(f"  {src}: {t!r} (artist: {a!r})")
        user_msg += "\n\nOther sources (for disambiguation):\n" + "\n".join(other_lines)
    user_msg += (
        "\n\nWhich candidate is the same release? Consider title variants (e.g. Volume I = volume i). "
        "Reply with only the letter (A, B, ...) or NONE if no candidate matches. "
        "Optionally end with (confidence: N) where N is 0-100."
    )
    system_msg = "Reply with only: a single letter (A, B, C, ...) or NONE. Optionally add (confidence: N)."
    try:
        model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4")
        provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
        verify_timeout = float(
            getattr(
                sys.modules[__name__],
                "MB_AI_VERIFY_TIMEOUT_SEC",
                MB_AI_VERIFY_TIMEOUT_SEC,
            )
            or MB_AI_VERIFY_TIMEOUT_SEC
        )
        logging.info(
            "[MusicBrainz Verify] AI disambiguation start (candidates=%d, timeout=%.1fs)",
            len(candidates),
            verify_timeout,
        )
        reply = _call_ai_provider_bounded(
            provider=provider,
            model=model,
            system_msg=system_msg,
            user_msg=user_msg,
            max_tokens=30,
            analysis_type="mb_match_verify",
            timeout_sec=verify_timeout,
            log_prefix="[MusicBrainz Verify]",
        )
        reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
        reply_clean = reply_clean.upper()
        if ai_confidence is not None:
            logging.info("[MusicBrainz Verify] AI confidence: %d", ai_confidence)
        if reply_clean == "NONE":
            return (None, ai_confidence)
        letter = reply_clean[:1]
        idx = letters.find(letter)
        if 0 <= idx < len(candidates):
            logging.info("[MusicBrainz Verify] AI selected candidate %s: %s", letter, candidates[idx][0].get("title"))
            confidence_min = getattr(sys.modules[__name__], "AI_CONFIDENCE_MIN", 0)
            if confidence_min > 0 and ai_confidence is not None and ai_confidence < confidence_min:
                logging.info("[MusicBrainz Verify] Low confidence (%d < %d): rejecting match, caller may try other sources", ai_confidence, confidence_min)
                return (None, ai_confidence)
            return (candidates[idx], ai_confidence)
        return (None, ai_confidence)
    except Exception as e:
        logging.debug("[MusicBrainz Verify] AI verify failed: %s", e)
        return (None, None)


def _identify_album_by_acoustic_id(
    folder: Path,
    artist: str,
    album_norm: str,
) -> tuple[Optional[dict], bool]:
    """
    Identify album via AcoustID fingerprint when tags are missing. Fingerprints audio files,
    looks up recordings, maps to MusicBrainz release-groups. Returns (rg_info, verified_by_ai).
    Logs and updates step_summary/step_response via caller.
    """
    if not getattr(sys.modules[__name__], "USE_ACOUSTID", False):
        return (None, False)
    api_key = (getattr(sys.modules[__name__], "ACOUSTID_API_KEY", "") or "").strip()
    if not api_key:
        logging.debug("[AcousticID] Skipped: no API key")
        return (None, False)
    try:
        import acoustid
    except ImportError as ie:
        logging.warning("[AcousticID] pyacoustid not available: %s", ie)
        return (None, False)
    audio_files = sorted([p for p in folder.rglob("*") if AUDIO_RE.search(p.name)])
    if not audio_files:
        logging.debug("[AcousticID] No audio files in %s", folder)
        return (None, False)
    # Limit to first 20 tracks to avoid long runtime (e.g. live albums)
    max_tracks = 20
    files_to_use = audio_files[:max_tracks]
    recording_scores: List[tuple[str, float]] = []  # (recording_mbid, score)
    for path in files_to_use:
        path_str = str(path)
        cached = get_cached_acoustid(path_str)
        if cached:
            duration, fingerprint = cached
        else:
            try:
                res = _fpcalc_fingerprint_file(path_str, length_sec=120, timeout_sec=45)
                if not res:
                    continue
                duration, fingerprint = res
                set_cached_acoustid(path_str, duration, fingerprint)
            except Exception as ex:
                logging.debug("[AcousticID] fingerprint_file failed for %s: %s", path.name, ex)
                continue
        try:
            response = acoustid.lookup(api_key, fingerprint, duration)
            for score, recording_id, _title, _artist in acoustid.parse_lookup_result(response):
                if recording_id and score >= 0.5:
                    recording_scores.append((recording_id, score))
                    break
        except Exception as ex:
            logging.debug("[AcousticID] lookup failed for %s: %s", path.name, ex)
    if not recording_scores:
        log_acoustid("No matches for folder %s (%d files fingerprinted)", folder, len(files_to_use))
        return (None, False)
    log_acoustid(
        "Folder %s: %d file(s) fingerprinted -> %d recording(s) from lookup",
        folder,
        len(files_to_use),
        len(recording_scores),
    )
    for rec_id, sc in recording_scores[:15]:
        log_acoustid("Recording %s score=%.2f", rec_id, sc)
    if len(recording_scores) > 15:
        log_acoustid("... and %d more recording candidate(s)", len(recording_scores) - 15)
    # Map each recording to release-group via MusicBrainz
    rg_counts: Dict[str, int] = {}
    rg_scores: Dict[str, float] = {}
    for recording_id, score in recording_scores:
        try:
            rec = musicbrainzngs.get_recording_by_id(recording_id, includes=["releases"])
            rec_data = rec.get("recording") if isinstance(rec.get("recording"), dict) else rec
            release_list = rec_data.get("release-list") or rec_data.get("releases") or []
            for rel in release_list[:5]:
                rg = rel.get("release-group")
                if isinstance(rg, dict):
                    rg_id = rg.get("id")
                elif isinstance(rg, str):
                    rg_id = rg
                else:
                    rg_id = rel.get("release-group-id")
                if rg_id:
                    rg_counts[rg_id] = rg_counts.get(rg_id, 0) + 1
                    rg_scores[rg_id] = max(rg_scores.get(rg_id, 0), score)
        except Exception as ex:
            logging.debug("[AcousticID] get_recording_by_id %s failed: %s", recording_id, ex)
    if not rg_counts:
        log_acoustid("No release-groups from recordings for %s", folder)
        return (None, False)
    log_acoustid("Release-groups from recordings (%d):", len(rg_counts))
    for rg_id in sorted(rg_counts.keys(), key=lambda x: (-rg_counts[x], -rg_scores.get(x, 0))):
        log_acoustid(
            "Release-group %s count=%d max_score=%.2f",
            rg_id,
            rg_counts[rg_id],
            rg_scores.get(rg_id, 0),
        )
    # Pick best release-group: most frequent, then highest score
    best_rg_id = max(rg_counts.keys(), key=lambda x: (rg_counts[x], rg_scores.get(x, 0)))
    candidates = [(rg_id, rg_counts[rg_id], rg_scores.get(rg_id, 0)) for rg_id in rg_counts]
    if len(candidates) > 1 and getattr(sys.modules[__name__], "USE_AI_FOR_MB_VERIFY", False) and getattr(sys.modules[__name__], "ai_provider_ready", False):
        # Multiple candidates: ask AI to pick (simplified – we only have rg IDs, so fetch titles and ask)
        try:
            choices = []
            for rg_id in list(rg_counts.keys())[:10]:
                info = musicbrainzngs.get_release_group_by_id(rg_id, includes=[])
                rg = info.get("release-group", {}) or {}
                title = rg.get("title", "?")
                artist_credit = rg.get("artist-credit") or []
                ac_name = ""
                if artist_credit and isinstance(artist_credit[0], dict):
                    ac_name = (artist_credit[0].get("artist") or {}).get("name", "") or artist_credit[0].get("name", "")
                elif artist_credit and isinstance(artist_credit[0], str):
                    ac_name = artist_credit[0]
                choices.append((rg_id, title, ac_name))
            if len(choices) >= 2:
                letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                log_acoustid("Candidates for AI choice (folder has no tags):")
                for i, (rg_id, title, ac_name) in enumerate(choices):
                    log_acoustid(
                        "Candidate %s: %s by %s (id=%s)",
                        letters[i],
                        title or "?",
                        ac_name or "?",
                        rg_id,
                    )
                context_line = (
                    "The album folder is named: \"%s\". Artist from path: \"%s\". Album (normalized): \"%s\".\n\n"
                    % (folder.name, artist or "?", album_norm or "?")
                )
                prompt = (
                    "AcousticID candidates for an album (folder has no tags):\n"
                    + context_line
                    + "\n".join(
                        f"{letters[i]}) {title} by {ac_name} (id={rg_id})" for i, (rg_id, title, ac_name) in enumerate(choices)
                    )
                    + "\n\nWhich release group matches this album? Reply with one letter or the MBID or NONE. Optionally end with (confidence: N) where N is 0-100."
                )
                reply = _call_ai_provider_bounded(
                    provider=getattr(sys.modules[__name__], "AI_PROVIDER", "openai"),
                    model=getattr(sys.modules[__name__], "RESOLVED_MODEL", "gpt-4o-mini"),
                    system_msg="Reply with a single letter (A,B,...) or an MBID (UUID) or NONE. Optionally end with (confidence: N).",
                    user_msg=prompt,
                    max_tokens=40,
                    analysis_type="acoustid_candidate_disambiguation",
                    timeout_sec=AI_SCAN_HARD_TIMEOUT_SEC,
                    log_prefix="[AcousticID]",
                )
                reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
                if ai_confidence is not None:
                    logging.info("[AcousticID] AI candidate choice confidence: %d", ai_confidence)
                reply = reply_clean.upper()
                if reply and reply != "NONE":
                    idx = letters.find(reply[:1])
                    if 0 <= idx < len(choices):
                        best_rg_id = choices[idx][0]
                    else:
                        mbid_match = re.search(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}", reply)
                        if mbid_match and any(c[0] == mbid_match.group(0) for c in choices):
                            best_rg_id = mbid_match.group(0)
        except Exception as ex:
            logging.debug("[AcousticID] AI verify failed: %s", ex)
    try:
        rg_info, _ = fetch_mb_release_group_info(best_rg_id)
        if rg_info:
            logging.info(
                "[AcousticID] Identified folder %s as release group %s (%s)",
                folder,
                best_rg_id,
                rg_info.get("title", "?"),
            )
            return (rg_info, len(candidates) > 1)
    except Exception as ex:
        logging.warning("[AcousticID] fetch_mb_release_group_info %s failed: %s", best_rg_id, ex)
    return (None, False)


def _musicbrainz_artist_identity_lookup(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    limit: int = 8,
) -> dict[str, Any]:
    if not USE_MUSICBRAINZ:
        return {}
    query = " ".join(str(artist_name or "").split()).strip()
    if not query:
        return {}
    person_like = _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints)
    kind = str(entity_kind or "").strip().lower()
    role_norms = {str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}
    try:
        search = musicbrainzngs.search_artists(artist=query, limit=max(1, min(int(limit or 8), 10)))
    except Exception:
        return {}
    artist_list = (search or {}).get("artist-list") or []
    if not isinstance(artist_list, list) or not artist_list:
        return {}
    query_norm = _norm_artist_key(query)
    query_signature = _classical_person_signature_key(query)
    query_person_sig = _classical_person_alias_signature(query) if person_like else {}
    query_surname = str(query_person_sig.get("surname") or "").strip()
    query_initials = {str(ch or "").strip() for ch in (query_person_sig.get("initials") or set()) if str(ch or "").strip()}
    query_long_givens = {str(tok or "").strip() for tok in (query_person_sig.get("long_givens") or set()) if str(tok or "").strip()}
    query_initials_only = bool(person_like and query_signature and query_surname and query_initials and not query_long_givens)

    def _person_candidate_compatible(*names: str) -> bool:
        if not person_like or not query_person_sig:
            return True
        query_surname = str(query_person_sig.get("surname") or "").strip()
        query_initials = {str(ch or "").strip() for ch in (query_person_sig.get("initials") or set()) if str(ch or "").strip()}
        query_long = {str(tok or "").strip() for tok in (query_person_sig.get("long_givens") or set()) if str(tok or "").strip()}
        saw_person_sig = False
        for raw_name in names:
            cand_sig = _classical_person_alias_signature(raw_name)
            if not cand_sig:
                cand_norm = _norm_artist_key(raw_name)
                if query_surname and cand_norm and query_surname in cand_norm:
                    return True
                continue
            saw_person_sig = True
            cand_surname = str(cand_sig.get("surname") or "").strip()
            if query_surname and cand_surname and cand_surname != query_surname:
                continue
            cand_initials = {str(ch or "").strip() for ch in (cand_sig.get("initials") or set()) if str(ch or "").strip()}
            cand_long = {str(tok or "").strip() for tok in (cand_sig.get("long_givens") or set()) if str(tok or "").strip()}
            if query_long and cand_long and query_long.intersection(cand_long):
                return True
            if query_initials and cand_initials and query_initials.intersection(cand_initials):
                return True
            if query_surname and cand_surname and query_surname == cand_surname and (not query_initials or not cand_initials):
                return True
        return (not query_surname) and (not saw_person_sig)

    best: dict[str, Any] | None = None
    best_score = 0.0
    for cand in artist_list[: max(3, min(int(limit or 8), 10))]:
        if not isinstance(cand, dict):
            continue
        cand_name = " ".join(str(cand.get("name") or "").split()).strip()
        if not cand_name:
            continue
        cand_sort = " ".join(str(cand.get("sort-name") or "").split()).strip()
        cand_type = str(cand.get("type") or "").strip().lower()
        if person_like and not _person_candidate_compatible(cand_name, cand_sort):
            continue
        score = _provider_identity_text_score(query, cand_name)
        if cand_sort:
            score = max(score, _provider_identity_text_score(query, cand_sort))
        cand_sig = _classical_person_alias_signature(cand_name) if person_like else {}
        if person_like and _classical_person_names_equivalent(query, cand_name):
            score = max(score, 0.95)
        if person_like and query_signature and _classical_person_signature_key(cand_name) == query_signature:
            score = max(score, 0.94)
        if query_norm and cand_sort and _norm_artist_key(cand_sort) == query_norm:
            score = max(score, 0.93)
        if person_like and cand_type == "person":
            score += 0.05
        elif person_like and cand_type:
            score -= 0.03
        if person_like and query_initials_only and cand_sig:
            cand_surname = str(cand_sig.get("surname") or "").strip()
            cand_long = {str(tok or "").strip() for tok in (cand_sig.get("long_givens") or set()) if str(tok or "").strip()}
            cand_initials = {str(ch or "").strip() for ch in (cand_sig.get("initials") or set()) if str(ch or "").strip()}
            if cand_surname and cand_surname == query_surname and cand_initials and query_initials <= cand_initials:
                if cand_long:
                    score += 0.12
                elif _norm_artist_key(cand_name) == query_norm:
                    score -= 0.08
        elif not person_like and (kind == "ensemble" or "orchestra" in role_norms or "ensemble" in role_norms) and cand_type in {"group", "orchestra", "choir"}:
            score += 0.03
        if score < 0.74:
            continue
        if best is None or score > best_score:
            best = cand
            best_score = score
    if not best:
        return {}
    artist_id = str(best.get("id") or "").strip()
    if not artist_id:
        return {}
    out = {
        "mbid": artist_id,
        "name": " ".join(str(best.get("name") or "").split()).strip(),
        "sort_name": " ".join(str(best.get("sort-name") or "").split()).strip(),
        "aliases": [],
        "urls": [],
    }
    alias_values: list[str] = []
    if out["name"]:
        alias_values.append(out["name"])
    if out["sort_name"]:
        alias_values.append(out["sort_name"])
    try:
        full = musicbrainzngs.get_artist_by_id(artist_id, includes=["aliases", "url-rels"])
    except Exception:
        full = {}
    artist_data = (full or {}).get("artist") or {}
    full_name = " ".join(str(artist_data.get("name") or "").split()).strip()
    full_sort = " ".join(str(artist_data.get("sort-name") or "").split()).strip()
    if full_name:
        out["name"] = full_name
        alias_values.append(full_name)
    if full_sort:
        out["sort_name"] = full_sort
        alias_values.append(full_sort)
    alias_list = artist_data.get("alias-list") or []
    if isinstance(alias_list, list):
        for item in alias_list:
            if not isinstance(item, dict):
                continue
            alias = " ".join(str(item.get("alias") or item.get("name") or "").split()).strip()
            if alias:
                alias_values.append(alias)
    if person_like and not _person_candidate_compatible(
        full_name,
        full_sort,
        out.get("name") or "",
        out.get("sort_name") or "",
        *alias_values,
    ):
        return {}
    seen: set[str] = set()
    deduped: list[str] = []
    for alias in alias_values:
        alias_norm = _norm_artist_key(alias)
        if not alias_norm or alias_norm in seen:
            continue
        seen.add(alias_norm)
        deduped.append(alias)
    out["aliases"] = deduped[:16]
    url_values: list[str] = []
    url_relations = artist_data.get("url-relation-list") or []
    if isinstance(url_relations, list):
        for relation in url_relations:
            if not isinstance(relation, dict):
                continue
            target = str(relation.get("target") or "").strip()
            if target:
                url_values.append(target)
    out["urls"] = _dedupe_keep_order(url_values)[:12]
    return out


def _classical_track_entries(track_source: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not isinstance(track_source, list):
        return out
    for i, tr in enumerate(track_source):
        title = ""
        disc = 1
        idx = i + 1
        dur_ms = 0
        if isinstance(tr, dict):
            title = str(tr.get("title") or tr.get("name") or "").strip()
            disc = _parse_int_loose(tr.get("disc") or tr.get("disc_num"), 1) or 1
            idx = _parse_int_loose(tr.get("idx") or tr.get("index") or tr.get("track") or tr.get("track_num"), i + 1) or (i + 1)
            dur_raw = tr.get("dur") or tr.get("duration_ms") or tr.get("duration")
            if isinstance(dur_raw, str):
                dur_ms = int(max(0.0, _parse_duration_seconds_loose(dur_raw, 0.0)) * 1000)
            else:
                try:
                    dur_ms = int(dur_raw or 0)
                except Exception:
                    dur_ms = 0
        elif isinstance(tr, str):
            title = tr.strip()
        else:
            title = str(getattr(tr, "title", "") or "").strip()
            disc = _parse_int_loose(getattr(tr, "disc", 1), 1) or 1
            idx = _parse_int_loose(getattr(tr, "idx", i + 1), i + 1) or (i + 1)
            try:
                dur_ms = int(getattr(tr, "dur", 0) or 0)
            except Exception:
                dur_ms = 0
        if title:
            out.append({"title": title, "disc": disc, "idx": idx, "dur_ms": max(0, dur_ms)})
    out.sort(key=lambda item: (int(item.get("disc") or 1), int(item.get("idx") or 0)))
    return out


def _classical_total_duration_ms_for_paths(paths: list[Any]) -> int:
    total = 0
    for raw in (paths or [])[:80]:
        try:
            path = Path(str(raw))
        except Exception:
            continue
        try:
            sec = float(_run_ffprobe_duration_sec(str(path)) or 0.0)
        except Exception:
            sec = 0.0
        if sec > 0:
            total += int(sec * 1000)
    return max(0, total)


def _classical_identity_context(
    *,
    local_artist: str,
    local_title: str,
    local_tracks: list[Any] | None = None,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    provider_payloads: dict | None = None,
    provider: str = "",
    provider_payload: dict | None = None,
    candidate_artist: str | list[str] | tuple[str, ...] | None = None,
    candidate_title: str = "",
) -> dict[str, Any]:
    tags = local_tags if isinstance(local_tags, dict) else {}
    track_entries = _classical_track_entries(list(local_tracks or []))
    track_titles = [str(item.get("title") or "").strip() for item in track_entries if str(item.get("title") or "").strip()]
    title_value = str(candidate_title or local_title or "").strip()
    artist_values = []
    if isinstance(candidate_artist, (list, tuple)):
        artist_values.extend([str(x or "").strip() for x in candidate_artist if str(x or "").strip()])
    else:
        artist_values.append(str(candidate_artist or local_artist or "").strip())
    base_artist_values = [v for v in artist_values if str(v or "").strip()]
    performance_tag_values = _classical_collect_tag_values(tags, _CLASSICAL_PERFORMANCE_TAG_KEYS)
    classical_role_values = _classical_collect_tag_values(tags, ("conductor", "orchestra", "ensemble", "choir", "chorus", "soloist", "soloists"))
    artist_values.extend(performance_tag_values)
    composer_values = _classical_collect_tag_values(tags, _CLASSICAL_COMPOSER_TAG_KEYS)
    work_values = _classical_collect_tag_values(tags, _CLASSICAL_WORK_TAG_KEYS)
    label_values = _classical_collect_tag_values(tags, _CLASSICAL_LABEL_TAG_KEYS)
    catalog_values = _classical_collect_tag_values(tags, _CLASSICAL_RELEASE_CATALOG_TAG_KEYS)
    genre_values = _classical_collect_tag_values(tags, ("genre", "style"))
    tag_override = _classical_tag_override(tags)
    meta_title_texts = [title_value] + work_values + track_titles[:20]
    work_tokens = _classical_work_tokens_from_texts(meta_title_texts)
    explicit_signal = _classical_has_explicit_signal(
        title=title_value,
        track_titles=track_titles,
        genre_values=genre_values,
        composer_values=composer_values,
        work_values=work_values,
        performer_values=performance_tag_values,
        catalog_values=catalog_values,
    )
    if not composer_values and base_artist_values:
        person_like_artists = [value for value in base_artist_values if _classical_person_alias_signature(value)]
        if explicit_signal and not performance_tag_values and person_like_artists:
            composer_values.extend(person_like_artists)
    composer_tokens = _classical_people_tokens(composer_values)
    performance_tokens = _classical_people_tokens(artist_values)
    performance_tokens -= composer_tokens
    disc_count = 0
    if track_entries:
        disc_count = max(int(item.get("disc") or 1) for item in track_entries)
    track_count = len(track_entries)
    total_duration_ms = sum(int(item.get("dur_ms") or 0) for item in track_entries)
    if total_duration_ms <= 0 and local_paths and track_count:
        total_duration_ms = _classical_total_duration_ms_for_paths(list(local_paths or []))
    year = ""
    for year_key in ("date", "originaldate", "year"):
        year = _mb_extract_year((tags or {}).get(year_key))
        if year:
            break
    genre_norms = {_classical_norm_text(v) for v in genre_values if _classical_norm_text(v)}
    pre_is_classical = bool(explicit_signal)
    provider_candidate_count = 0
    if isinstance(provider_payloads, dict):
        provider_candidate_count = sum(1 for value in provider_payloads.values() if isinstance(value, dict))
    cover_ocr_ctx = _identity_cover_ocr_context(
        local_artist="; ".join(base_artist_values[:3]),
        local_title=title_value,
        local_tracks=track_entries,
        local_tags=tags,
        local_paths=list(local_paths or []),
        provider_candidate_count=provider_candidate_count,
    )
    if cover_ocr_ctx:
        label_values.extend([str(v or "").strip() for v in cover_ocr_ctx.get("label_values") or [] if str(v or "").strip()])
        meta_title_texts.extend([str(v or "").strip() for v in cover_ocr_ctx.get("lines") or [] if str(v or "").strip()])
        work_tokens = _classical_work_tokens_from_texts(meta_title_texts[:32])
    label_tokens = _classical_label_tokens(label_values)
    label_tokens |= set(cover_ocr_ctx.get("label_tokens") or set())
    catalog_tokens = _classical_release_catalog_tokens_from_texts(catalog_values)
    catalog_tokens |= set(cover_ocr_ctx.get("catalog_tokens") or set())
    performance_tokens |= set(cover_ocr_ctx.get("performance_tokens") or set())
    performance_tokens -= composer_tokens
    ocr_classical_signal = bool(
        cover_ocr_ctx.get("work_tokens")
        and (
            _classical_genre_signal(genre_values)
            or bool(catalog_tokens)
            or bool(classical_role_values)
            or bool(pre_is_classical)
        )
    )
    forced_non_classical = bool(
        tag_override is False
        and not (
            pre_is_classical
            or bool(catalog_tokens)
            or bool(classical_role_values)
            or _classical_genre_signal(genre_values)
        )
    )
    is_classical = False if forced_non_classical else bool(pre_is_classical or ocr_classical_signal)
    return {
        "is_classical": bool(is_classical),
        "composer_tokens": composer_tokens,
        "work_tokens": work_tokens,
        "performance_tokens": performance_tokens,
        "label_tokens": label_tokens,
        "catalog_tokens": catalog_tokens,
        "genre_tokens": genre_norms,
        "track_count": int(track_count or 0),
        "disc_count": int(disc_count or 0),
        "total_duration_ms": int(total_duration_ms or 0),
        "year": year,
        "title_norm": _normalize_identity_album_strict(title_value),
        "artist_norms": [_classical_norm_text(v) for v in artist_values if _classical_norm_text(v)],
        "track_titles": track_titles,
        "provider": str(provider or "").strip().lower(),
        "cover_ocr_source": str(cover_ocr_ctx.get("source") or "").strip(),
        "cover_ocr_text": str(cover_ocr_ctx.get("text") or "").strip(),
        "cover_ocr_lines": list(cover_ocr_ctx.get("lines") or [])[:20],
        "cover_ocr_title_norms": set(cover_ocr_ctx.get("title_norms") or set()),
        "cover_ocr_artist_norms": set(cover_ocr_ctx.get("artist_norms") or set()),
    }


def _provider_classical_context(
    *,
    provider: str,
    payload: dict | None,
    candidate_artist: str | list[str] | tuple[str, ...] | None,
    candidate_title: str,
) -> dict[str, Any]:
    payload_dict = payload if isinstance(payload, dict) else {}
    tags = {}
    raw_tags = payload_dict.get("tags") or payload_dict.get("toptags")
    candidate_artist_values: list[str] = []
    if isinstance(candidate_artist, (list, tuple)):
        candidate_artist_values = [str(x or "").strip() for x in candidate_artist if str(x or "").strip()]
    else:
        candidate_artist_values = [str(candidate_artist or "").strip()] if str(candidate_artist or "").strip() else []
    if isinstance(raw_tags, list):
        tags["genre"] = ", ".join([str(x or "").strip() for x in raw_tags if str(x or "").strip()])
    elif isinstance(raw_tags, str):
        tags["genre"] = raw_tags
    for key in ("label", "publisher", "organization", "composer", "conductor", "orchestra", "ensemble", "performer", "catalog_number", "catalog_numbers", "catno"):
        if payload_dict.get(key):
            tags[key] = payload_dict.get(key)
    year_val = payload_dict.get("year") or payload_dict.get("date") or payload_dict.get("first-release-date")
    if year_val:
        tags["year"] = year_val
    explicit_composer = _classical_collect_tag_values(tags, _CLASSICAL_COMPOSER_TAG_KEYS)
    explicit_performance = _classical_collect_tag_values(tags, _CLASSICAL_PERFORMANCE_TAG_KEYS)
    explicit_signal = _classical_has_explicit_signal(
        title=candidate_title,
        genre_values=_classical_collect_tag_values(tags, ("genre", "style")),
        composer_values=explicit_composer,
        work_values=_classical_collect_tag_values(tags, _CLASSICAL_WORK_TAG_KEYS),
        conductor_values=_classical_collect_tag_values(tags, ("conductor",)),
        orchestra_values=_classical_collect_tag_values(tags, ("orchestra",)),
        ensemble_values=_classical_collect_tag_values(tags, ("ensemble", "choir", "chorus")),
        soloist_values=_classical_collect_tag_values(tags, ("soloist", "soloists")),
        performer_values=explicit_performance,
        catalog_values=_classical_collect_tag_values(tags, _CLASSICAL_RELEASE_CATALOG_TAG_KEYS),
    )
    candidate_artist_for_context: str | list[str] | tuple[str, ...] | None = candidate_artist
    if candidate_artist_values and not explicit_composer and explicit_signal:
        # Provider artist credit for classical releases is often the composer. Treat it as such
        # unless the payload already exposes a richer performance context.
        tags["composer"] = candidate_artist_values
        candidate_artist_for_context = candidate_artist if explicit_performance else ""
    ctx = _classical_identity_context(
        local_artist="",
        local_title="",
        local_tracks=list(payload_dict.get("tracklist") or payload_dict.get("track_titles") or []),
        local_tags=tags,
        local_paths=None,
        provider=provider,
        provider_payload=payload_dict,
        candidate_artist=candidate_artist_for_context,
        candidate_title=candidate_title,
    )
    ctx["identity_scope"] = str(payload_dict.get("identity_scope") or "").strip().lower()
    return ctx


def _classical_context_for_edition(edition: dict | None) -> dict[str, Any]:
    e = edition if isinstance(edition, dict) else {}
    cached = e.get("_classical_ctx")
    if isinstance(cached, dict):
        return cached
    tags: dict[str, Any] = {}
    if isinstance(e.get("meta"), dict):
        tags.update(e.get("meta") or {})
    if isinstance(e.get("tags"), dict):
        tags.update(e.get("tags") or {})
    ctx = _classical_identity_context(
        local_artist=str(e.get("artist_name") or e.get("artist") or e.get("album_artist") or "").strip(),
        local_title=str(e.get("title_raw") or e.get("plex_title") or e.get("album_title") or "").strip(),
        local_tracks=list(e.get("tracks") or []),
        local_tags=tags,
        local_paths=list(e.get("ordered_paths") or []),
    )
    e["_classical_ctx"] = ctx
    return ctx


def _classical_identity_match_details(
    *,
    local_artist: str,
    local_title: str,
    candidate_artist: str | list[str] | tuple[str, ...] | None,
    candidate_title: str,
    local_tracks: list[Any] | None = None,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    provider: str = "",
    provider_payload: dict | None = None,
    local_context: dict | None = None,
) -> tuple[bool, str]:
    local_ctx = local_context if isinstance(local_context, dict) else _classical_identity_context(
        local_artist=local_artist,
        local_title=local_title,
        local_tracks=list(local_tracks or []),
        local_tags=local_tags if isinstance(local_tags, dict) else {},
        local_paths=list(local_paths or []),
    )
    provider_ctx = _provider_classical_context(
        provider=provider,
        payload=provider_payload,
        candidate_artist=candidate_artist,
        candidate_title=candidate_title,
    )
    local_is_classical = bool(local_ctx.get("is_classical"))
    provider_is_classical = bool(provider_ctx.get("is_classical"))
    if not local_is_classical:
        return (False, "classical_context_missing")

    local_title_norm = str(local_ctx.get("title_norm") or _normalize_identity_album_strict(local_title))
    provider_title_norm = str(provider_ctx.get("title_norm") or _normalize_identity_album_strict(candidate_title))
    local_work = set(local_ctx.get("work_tokens") or set())
    provider_work = set(provider_ctx.get("work_tokens") or set())
    local_composer = set(local_ctx.get("composer_tokens") or set())
    provider_composer = set(provider_ctx.get("composer_tokens") or set())
    local_perf = set(local_ctx.get("performance_tokens") or set())
    provider_perf = set(provider_ctx.get("performance_tokens") or set())
    local_label = set(local_ctx.get("label_tokens") or set())
    provider_label = set(provider_ctx.get("label_tokens") or set())
    local_catalog = set(local_ctx.get("catalog_tokens") or set())
    provider_catalog = set(provider_ctx.get("catalog_tokens") or set())
    provider_scope = str(provider_ctx.get("identity_scope") or "").strip().lower()

    title_exact = bool(local_title_norm and provider_title_norm and local_title_norm == provider_title_norm)
    work_overlap = local_work & provider_work if local_work and provider_work else set()
    composer_overlap = local_composer & provider_composer if local_composer and provider_composer else set()
    perf_overlap = local_perf & provider_perf if local_perf and provider_perf else set()
    label_overlap = local_label & provider_label if local_label and provider_label else set()
    catalog_overlap = local_catalog & provider_catalog if local_catalog and provider_catalog else set()

    if local_work and provider_work and not work_overlap and not title_exact:
        return (False, "classical_work_mismatch")
    if local_composer and provider_composer and not composer_overlap:
        return (False, "classical_composer_mismatch")
    if local_perf and provider_perf and not perf_overlap:
        return (False, "classical_performance_mismatch")
    if local_catalog and provider_catalog and not catalog_overlap and provider_scope in {"release", "discogs_release", "discogs_master_resolved"}:
        return (False, "classical_catalog_mismatch")

    local_track_count = int(local_ctx.get("track_count") or 0)
    provider_track_count = int(provider_ctx.get("track_count") or 0)
    if local_track_count > 0 and provider_track_count > 0 and local_track_count != provider_track_count:
        return (False, f"classical_track_count_mismatch local={local_track_count} provider={provider_track_count}")

    local_disc_count = int(local_ctx.get("disc_count") or 0)
    provider_disc_count = int(provider_ctx.get("disc_count") or 0)
    if local_disc_count > 0 and provider_disc_count > 0 and local_disc_count != provider_disc_count:
        return (False, f"classical_disc_count_mismatch local={local_disc_count} provider={provider_disc_count}")

    local_total = int(local_ctx.get("total_duration_ms") or 0)
    provider_total = int(provider_ctx.get("total_duration_ms") or 0)
    if local_total > 0 and provider_total > 0:
        hi = max(local_total, provider_total)
        diff = abs(local_total - provider_total)
        if hi > 0 and diff > max(90000, int(hi * 0.03)):
            return (False, f"classical_duration_mismatch local={local_total} provider={provider_total}")

    if local_label and provider_label and not (local_label & provider_label) and local_perf and provider_perf and not perf_overlap:
        return (False, "classical_label_plus_performance_mismatch")

    local_year = _mb_extract_year(local_ctx.get("year"))
    provider_year = _mb_extract_year(provider_ctx.get("year"))
    if local_year and provider_year:
        try:
            if abs(int(local_year) - int(provider_year)) > 15 and local_perf and provider_perf and not perf_overlap:
                return (False, f"classical_year_mismatch local={local_year} provider={provider_year}")
        except Exception:
            pass

    if catalog_overlap and composer_overlap and (work_overlap or title_exact):
        return (True, "classical identity ok (catalog overlap)")
    if local_perf and provider_perf and perf_overlap:
        return (True, "classical identity ok (performance overlap)")
    if label_overlap and composer_overlap and (work_overlap or title_exact):
        return (True, "classical identity ok (label overlap)")
    if local_composer and provider_composer and composer_overlap and (work_overlap or title_exact):
        return (True, "classical identity ok (composer/work overlap)")
    if work_overlap and title_exact:
        return (True, "classical identity ok (work/title overlap)")
    if title_exact and local_track_count > 0 and provider_track_count > 0 and local_track_count == provider_track_count:
        return (True, "classical identity ok (title/structure exact)")
    return (False, "classical_context_insufficient")


def _strict_identity_match_details(
    *,
    local_artist: str,
    local_title: str,
    candidate_artist: str | list[str] | tuple[str, ...] | None,
    candidate_title: str,
    local_tracks: list[Any] | None = None,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    provider: str = "",
    provider_payload: dict | None = None,
    local_context: dict | None = None,
) -> tuple[bool, str]:
    """
    Strict identity gate:
    - normalized artist must match exactly
    - normalized album title must match exactly
    Returns (ok, reason).
    """
    local_artist_norm = _normalize_identity_text_strict(local_artist)
    local_title_norm = _normalize_identity_album_strict(local_title)
    candidate_title_norm = _normalize_identity_album_strict(candidate_title)
    if isinstance(candidate_artist, (list, tuple)):
        candidate_artist_values = [str(x or "") for x in candidate_artist]
    else:
        candidate_artist_values = [str(candidate_artist or "")]
    classical_ok, classical_reason = _classical_identity_match_details(
        local_artist=local_artist,
        local_title=local_title,
        candidate_artist=candidate_artist_values,
        candidate_title=candidate_title,
        local_tracks=list(local_tracks or []),
        local_tags=local_tags if isinstance(local_tags, dict) else {},
        local_paths=list(local_paths or []),
        provider=provider,
        provider_payload=provider_payload if isinstance(provider_payload, dict) else {},
        local_context=local_context if isinstance(local_context, dict) else None,
    )
    if classical_ok:
        return (True, classical_reason)
    if classical_reason and classical_reason != "classical_context_missing":
        return (False, classical_reason)
    candidate_artist_norms = []
    for value in candidate_artist_values:
        for norm in _identity_artist_credit_norms(value):
            if norm and norm not in candidate_artist_norms:
                candidate_artist_norms.append(norm)
    artist_ok = bool(
        local_artist_norm
        and candidate_artist_norms
        and local_artist_norm in candidate_artist_norms
    )
    equal_multi_artist, partial_artist_overlap = (False, False)
    if not artist_ok and local_artist_norm and candidate_artist_values:
        for value in candidate_artist_values:
            equal_multi_artist, partial_artist_overlap = _identity_artist_credit_overlap(local_artist, value)
            if equal_multi_artist:
                artist_ok = True
                break
    title_ok = bool(
        local_title_norm
        and candidate_title_norm
        and (
            local_title_norm == candidate_title_norm
            or _identity_album_equivalent(
                local_title,
                candidate_title,
                artist_hints=[local_artist, *candidate_artist_values],
            )
        )
    )
    local_is_va = _identity_artist_is_various_artists(local_artist)
    candidate_is_va = any(_identity_artist_is_various_artists(value) for value in candidate_artist_values)
    compilation_tag = _identity_tags_mark_compilation(local_tags)
    if title_ok and (
        (local_is_va and candidate_is_va)
        or (compilation_tag and candidate_is_va and not local_artist_norm)
    ):
        return (True, "strict identity ok (various artists/title exact)")
    ocr_title_ok = False
    ocr_artist_ok = False
    if isinstance(local_context, dict):
        ocr_title_ok = _cover_ocr_best_match_score(local_context, candidate_title, album_mode=True) >= 0.92
        ocr_artist_ok = any(
            _cover_ocr_best_match_score(local_context, value, album_mode=False) >= 0.88
            for value in candidate_artist_values
            if str(value or "").strip()
        )
    if artist_ok and title_ok:
        return (True, "strict identity ok (artist/title exact)")
    if title_ok and not local_artist_norm and ocr_artist_ok:
        return (True, "strict identity ok (cover ocr artist)")
    if artist_ok and not local_title_norm and ocr_title_ok:
        return (True, "strict identity ok (cover ocr title)")
    if not local_title_norm and not local_artist_norm and ocr_title_ok and ocr_artist_ok:
        return (True, "strict identity ok (cover ocr)")
    reasons: list[str] = []
    if not local_artist_norm:
        reasons.append("local artist missing")
    if not local_title_norm:
        reasons.append("local title missing")
    if not candidate_artist_norms:
        reasons.append("candidate artist missing")
    elif not artist_ok:
        if partial_artist_overlap:
            reasons.append(
                f"artist partial overlap local={local_artist_norm!r} candidate={candidate_artist_norms[:4]!r}"
            )
        else:
            reasons.append(
                f"artist mismatch local={local_artist_norm!r} candidate={candidate_artist_norms[:4]!r}"
            )
    if not candidate_title_norm:
        reasons.append("candidate title missing")
    elif not title_ok:
        reasons.append(
            f"title mismatch local={local_title_norm!r} candidate={candidate_title_norm!r}"
        )
    return (False, "; ".join(reasons) if reasons else "strict identity failed")


def _fetch_musicbrainz_strict_payload(mbid: str) -> dict:
    """Fetch MB payload with strict fields (artist/title/tracklist/id).

    The input may be either a release id or a release-group id. When a release id is
    available we must prefer it, otherwise multi-disc or alternate releases can get the
    wrong tracklist and the wrong cover downstream.
    """
    out = {
        "id": "",
        "title": "",
        "mb_artist_names": [],
        "tracklist": [],
        "cover_url": "",
        "year": "",
        "release_count": 0,
        "versions": [],
        "release_group_id": "",
        "url": "",
        "label": [],
        "catalog_number": [],
        "identity_scope": "",
    }
    ref_id = str(mbid or "").strip()
    if not ref_id or not USE_MUSICBRAINZ:
        return out
    out["id"] = ref_id

    try:
        def _fetch_rel():
            return musicbrainzngs.get_release_by_id(
                ref_id,
                includes=["recordings", "release-groups", "artist-credits", "labels"],
            )
        rel_resp = get_mb_queue().submit(f"rel_strict_{ref_id}", _fetch_rel) if (MB_QUEUE_ENABLED and USE_MUSICBRAINZ) else _fetch_rel()
        release_data = (rel_resp or {}).get("release") or {}
        if isinstance(release_data, dict) and str(release_data.get("id") or "").strip():
            release_id = str(release_data.get("id") or "").strip()
            release_group = release_data.get("release-group") or {}
            release_group_id = str((release_group or {}).get("id") or "").strip()
            out["id"] = release_id
            out["url"] = f"https://musicbrainz.org/release/{quote(release_id, safe='')}"
            out["title"] = str(release_data.get("title") or "").strip()
            out["mb_artist_names"] = _extract_mb_artist_names(release_data)
            out["tracklist"] = _extract_track_titles_from_mb_release(rel_resp)
            out["year"] = _mb_extract_year(release_data.get("date"))
            out["cover_url"] = f"https://coverartarchive.org/release/{quote(release_id, safe='')}/front"
            out["label"], out["catalog_number"] = _extract_musicbrainz_release_label_info(release_data)
            out["identity_scope"] = "release"
            if release_group_id:
                out["release_group_id"] = release_group_id
                release_count, versions, rg_raw = _fetch_musicbrainz_release_group_versions(release_group_id)
                out["release_count"] = release_count
                out["versions"] = versions
                if not out["title"]:
                    out["title"] = str((rg_raw or {}).get("title") or "").strip()
                if not out["mb_artist_names"]:
                    out["mb_artist_names"] = _extract_mb_artist_names(rg_raw)
                if not out["year"]:
                    out["year"] = _mb_extract_year((rg_raw or {}).get("first-release-date"))
            return out
    except Exception:
        pass

    try:
        rg_info, _ = fetch_mb_release_group_info(ref_id)
    except Exception:
        rg_info = {}
    if isinstance(rg_info, dict):
        out["title"] = str(rg_info.get("title") or "").strip()
        artists = rg_info.get("mb_artist_names") or _extract_mb_artist_names(rg_info)
        if isinstance(artists, list):
            out["mb_artist_names"] = artists
    try:
        release_count, versions, rg_raw = _fetch_musicbrainz_release_group_versions(ref_id)
        out["release_group_id"] = ref_id
        out["release_count"] = release_count
        out["versions"] = versions
        out["cover_url"] = f"https://coverartarchive.org/release-group/{quote(ref_id, safe='')}/front"
        out["url"] = f"https://musicbrainz.org/release-group/{quote(ref_id, safe='')}"
        if not out["title"]:
            out["title"] = str((rg_raw or {}).get("title") or "").strip()
        if not out["mb_artist_names"]:
            out["mb_artist_names"] = _extract_mb_artist_names(rg_raw)
        if not out["year"]:
            out["year"] = _mb_extract_year((rg_raw or {}).get("first-release-date"))
        release_id = str((versions[0] or {}).get("id") or "").strip() if versions else ""
        if release_id:
            try:
                def _fetch_rel():
                    return musicbrainzngs.get_release_by_id(release_id, includes=["recordings", "labels", "artist-credits"])
                rel_resp = get_mb_queue().submit(f"rel_strict_rg_{release_id}", _fetch_rel) if (MB_QUEUE_ENABLED and USE_MUSICBRAINZ) else _fetch_rel()
                out["tracklist"] = _extract_track_titles_from_mb_release(rel_resp)
                release_data = (rel_resp or {}).get("release") or {}
                out["label"], out["catalog_number"] = _extract_musicbrainz_release_label_info(release_data)
                out["identity_scope"] = "release_group_fallback"
            except Exception:
                out["tracklist"] = []
    except Exception:
        pass
    return out


def _score_musicbrainz_release_payload_for_local_context(payload: dict | None, local_ctx: dict | None) -> float:
    if not isinstance(payload, dict) or not isinstance(local_ctx, dict):
        return -999.0
    score = 0.0
    local_title_norm = str(local_ctx.get("title_norm") or "")
    provider_title_norm = _normalize_identity_album_strict(str(payload.get("title") or ""))
    if local_title_norm and provider_title_norm and local_title_norm == provider_title_norm:
        score += 2.0

    local_track_count = int(local_ctx.get("track_count") or 0)
    provider_track_count = len(payload.get("tracklist") or [])
    if local_track_count > 0 and provider_track_count > 0:
        if local_track_count == provider_track_count:
            score += 2.0
        else:
            score -= min(2.0, abs(local_track_count - provider_track_count) * 0.5)

    local_year = _mb_extract_year(local_ctx.get("year"))
    provider_year = _mb_extract_year(payload.get("year"))
    if local_year and provider_year:
        try:
            diff = abs(int(local_year) - int(provider_year))
        except Exception:
            diff = 99
        if diff == 0:
            score += 1.5
        elif diff <= 1:
            score += 1.0
        elif diff <= 5:
            score += 0.4
        else:
            score -= min(1.5, diff / 10.0)

    local_label = set(local_ctx.get("label_tokens") or set())
    provider_label = _classical_label_tokens(_classical_split_values(payload.get("label") or []))
    if local_label and provider_label:
        if local_label & provider_label:
            score += 2.5
        else:
            score -= 1.5

    local_catalog = set(local_ctx.get("catalog_tokens") or set())
    provider_catalog = _classical_release_catalog_tokens_from_texts(_classical_split_values(payload.get("catalog_number") or []))
    if local_catalog and provider_catalog:
        if local_catalog & provider_catalog:
            score += 4.0
        else:
            score -= 2.0
    return score


def _fetch_musicbrainz_strict_payload_for_edition(mbid: str, edition: dict | None) -> dict | None:
    payload = _fetch_musicbrainz_strict_payload(mbid)
    if not isinstance(payload, dict) or not isinstance(edition, dict):
        return payload if isinstance(payload, dict) else None
    local_ctx = _classical_context_for_edition(edition)
    if not bool(local_ctx.get("is_classical")):
        return payload
    versions = payload.get("versions") or []
    if not isinstance(versions, list) or not versions:
        return payload
    if not (local_ctx.get("label_tokens") or local_ctx.get("catalog_tokens") or str(local_ctx.get("year") or "").strip()):
        return payload

    best_payload = payload
    best_score = _score_musicbrainz_release_payload_for_local_context(payload, local_ctx)
    ranked_versions = list(versions[:10])
    local_year = _mb_extract_year(local_ctx.get("year"))
    if local_year:
        def _year_distance(item: dict[str, Any]) -> int:
            try:
                year = int(_mb_extract_year(item.get("date")))
                return abs(int(local_year) - year)
            except Exception:
                return 999
        ranked_versions.sort(key=_year_distance)

    for version in ranked_versions:
        release_id = str((version or {}).get("id") or "").strip()
        if not release_id or release_id == str(payload.get("id") or "").strip():
            continue
        try:
            candidate = _fetch_musicbrainz_strict_payload(release_id)
        except Exception:
            candidate = None
        if not isinstance(candidate, dict):
            continue
        candidate_score = _score_musicbrainz_release_payload_for_local_context(candidate, local_ctx)
        if candidate_score > (best_score + 0.35):
            best_payload = candidate
            best_score = candidate_score
    return best_payload


def _strict_expected_provider_id(provider: str, edition: dict) -> str:
    p = _normalize_identity_provider(provider)
    meta = edition.get("meta") if isinstance(edition.get("meta"), dict) else {}
    if p == "musicbrainz":
        return str(
            edition.get("musicbrainz_release_id")
            or edition.get("musicbrainz_albumid")
            or meta.get("musicbrainz_releaseid")
            or meta.get("musicbrainz_albumid")
            or edition.get("musicbrainz_id")
            or (edition.get("rg_info") or {}).get("id")
            or meta.get("musicbrainz_releasegroupid")
            or meta.get("musicbrainz_id")
            or ""
        ).strip()
    if p == "discogs":
        return str(
            edition.get("discogs_release_id")
            or meta.get("discogs_release_id")
            or ""
        ).strip()
    if p == "bandcamp":
        return str(
            edition.get("bandcamp_album_url")
            or meta.get("bandcamp_album_url")
            or ""
        ).strip()
    if p == "lastfm":
        return str(
            edition.get("lastfm_album_mbid")
            or meta.get("lastfm_album_mbid")
            or ""
        ).strip()
    if p == "itunes":
        return str(
            edition.get("itunes_collection_id")
            or meta.get("itunes_collection_id")
            or ""
        ).strip()
    if p == "deezer":
        return str(
            edition.get("deezer_album_id")
            or meta.get("deezer_album_id")
            or ""
        ).strip()
    if p == "spotify":
        return str(
            edition.get("spotify_album_id")
            or meta.get("spotify_album_id")
            or ""
        ).strip()
    if p == "qobuz":
        return str(
            edition.get("qobuz_album_id")
            or meta.get("qobuz_album_id")
            or ""
        ).strip()
    if p == "tidal":
        return str(
            edition.get("tidal_album_id")
            or meta.get("tidal_album_id")
            or ""
        ).strip()
    if p == "audiodb":
        return str(
            edition.get("audiodb_album_id")
            or meta.get("audiodb_album_id")
            or ""
        ).strip()
    return ""


def _strict_payload_for_provider(
    provider: str,
    *,
    artist_name: str,
    album_title: str,
    edition: dict,
) -> dict | None:
    p = _normalize_identity_provider(provider)
    try:
        if p == "musicbrainz":
            mbid = _strict_expected_provider_id("musicbrainz", edition)
            if not mbid:
                return None
            return _fetch_musicbrainz_strict_payload_for_edition(mbid, edition)
        if p == "discogs":
            payload = edition.get("fallback_discogs") if isinstance(edition.get("fallback_discogs"), dict) else None
            expected_id = _strict_expected_provider_id("discogs", edition)
            if payload is None and expected_id:
                try:
                    payload = _fetch_discogs_release_by_id(expected_id)
                except DiscogsRateLimited:
                    payload = None
            if payload is None and USE_DISCOGS:
                try:
                    payload = _fetch_discogs_release(artist_name, album_title)
                except DiscogsRateLimited:
                    payload = None
            return payload if isinstance(payload, dict) else None
        if p == "bandcamp":
            payload = edition.get("fallback_bandcamp") if isinstance(edition.get("fallback_bandcamp"), dict) else None
            expected_id = _strict_expected_provider_id("bandcamp", edition)
            if payload is None and USE_BANDCAMP:
                payload = _fetch_bandcamp_album_info(artist_name, album_title, album_url_hint=expected_id)
            return payload if isinstance(payload, dict) else None
        if p == "lastfm":
            payload = edition.get("fallback_lastfm") if isinstance(edition.get("fallback_lastfm"), dict) else None
            expected = _strict_expected_provider_id("lastfm", edition)
            if payload is None and USE_LASTFM:
                payload = _fetch_lastfm_album_info(artist_name, album_title, expected or None)
            return payload if isinstance(payload, dict) else None
        if p == "itunes":
            payload = edition.get("fallback_itunes") if isinstance(edition.get("fallback_itunes"), dict) else None
            if payload is None and USE_ITUNES:
                payload = _fetch_itunes_album_info(artist_name, album_title)
            return payload if isinstance(payload, dict) else None
        if p == "deezer":
            payload = edition.get("fallback_deezer") if isinstance(edition.get("fallback_deezer"), dict) else None
            if payload is None and USE_DEEZER:
                payload = _fetch_deezer_album_info(artist_name, album_title)
            return payload if isinstance(payload, dict) else None
        if p == "spotify":
            payload = edition.get("fallback_spotify") if isinstance(edition.get("fallback_spotify"), dict) else None
            if payload is None and USE_SPOTIFY:
                payload = _fetch_spotify_album_info(artist_name, album_title)
            return payload if isinstance(payload, dict) else None
        if p == "qobuz":
            payload = edition.get("fallback_qobuz") if isinstance(edition.get("fallback_qobuz"), dict) else None
            if payload is None and USE_QOBUZ:
                payload = _fetch_qobuz_album_info(artist_name, album_title)
            return payload if isinstance(payload, dict) else None
        if p == "tidal":
            payload = edition.get("fallback_tidal") if isinstance(edition.get("fallback_tidal"), dict) else None
            if payload is None and USE_TIDAL:
                payload = _fetch_tidal_album_info(artist_name, album_title)
            return payload if isinstance(payload, dict) else None
        if p == "audiodb":
            payload = edition.get("fallback_audiodb") if isinstance(edition.get("fallback_audiodb"), dict) else None
            if payload is None and THEAUDIODB_API_KEY:
                payload = _fetch_audiodb_album_info(artist_name, album_title)
            return payload if isinstance(payload, dict) else None
    except Exception as exc:
        logging.debug(
            "[Strict Validate] provider payload fetch failed provider=%s artist=%r album=%r: %s",
            p or provider,
            artist_name,
            album_title,
            exc,
        )
        return None
    return None


def _strict_provider_cold_fetch_allowed(provider: str, edition: dict) -> bool:
    """
    Strict validation should not fan out into fresh provider lookups for every edition.

    The matching phase already does the broad provider arbitration work. The strict phase
    should mainly validate:
    - providers with a concrete stored ID already attached to the edition, or
    - the primary provider that already won soft identity arbitration.

    This keeps strict verification deterministic while avoiding a second cold-fetch fanout
    that can serialize the whole scan on throttled providers such as Discogs.
    """
    p = _normalize_identity_provider(provider)
    if not p:
        return False
    try:
        expected_id = _strict_expected_provider_id(p, edition)
    except Exception:
        expected_id = ""
    if str(expected_id or "").strip():
        return True
    if _scan_inline_matching_active() or _ai_scan_lifecycle_phase_active():
        return False
    primary = _normalize_identity_provider(
        str(
            edition.get("primary_metadata_source")
            or edition.get("identity_provider")
            or edition.get("metadata_source")
            or edition.get("_strict_provider")
            or ""
        )
    )
    return bool(primary and primary == p)


def _strict_clear_identity_on_reject(edition: dict) -> None:
    """Fail-closed: clear provider identity fields when strict validation fails."""
    edition["musicbrainz_id"] = ""
    edition["musicbrainz_type"] = ""
    edition["discogs_release_id"] = ""
    edition["lastfm_album_mbid"] = ""
    edition["bandcamp_album_url"] = ""
    edition.pop("rg_info", None)
    edition.pop("primary_metadata_source", None)
    edition.pop("metadata_source", None)
    meta = edition.get("meta") if isinstance(edition.get("meta"), dict) else {}
    for k in (
        "musicbrainz_releasegroupid",
        "musicbrainz_releaseid",
        "musicbrainz_id",
        "discogs_release_id",
        "lastfm_album_mbid",
        "bandcamp_album_url",
        "primary_metadata_source",
        PMDA_MATCH_PROVIDER_TAG,
    ):
        try:
            meta.pop(k, None)
        except Exception:
            pass
    edition["meta"] = meta


def _resolve_edition_display_identity(
    edition: dict | None,
    *,
    default_artist: str = "",
    default_title: str = "",
    folder_name: str = "",
) -> tuple[str, str]:
    e = edition if isinstance(edition, dict) else {}
    hint = e.get("_lookup_identity_hint") if isinstance(e.get("_lookup_identity_hint"), dict) else {}
    missing_required = _edition_missing_required_tags_set(e)
    hint_source = str(hint.get("source") or "").strip().lower()
    try:
        hint_confidence = int(float(hint.get("confidence") or 0))
    except Exception:
        hint_confidence = 0
    hint_reason = str(hint.get("reason") or "").strip().lower()
    current_artist = str(
        e.get("artist")
        or e.get("artist_name")
        or default_artist
        or ""
    ).strip()
    current_title = str(
        e.get("title_raw")
        or e.get("album_title")
        or default_title
        or ""
    ).strip()
    hinted_artist = str(
        e.get("_lookup_artist_name")
        or hint.get("artist")
        or ""
    ).strip()
    hinted_album = str(
        e.get("_lookup_album_title")
        or hint.get("album")
        or ""
    ).strip()
    force_hint_override = bool(
        hinted_artist
        and hinted_album
        and hint_source == "filename_pattern"
        and hint_confidence >= 90
        and (
            "artist_missing_or_generic" in hint_reason
            or "album_missing_or_generic" in hint_reason
            or "album_conflict_same_artist" in hint_reason
        )
    )
    verified_hint_override = bool(
        hinted_artist
        and hinted_album
        and hint_source in {"filename_pattern", "ai_local_context"}
        and hint_confidence >= 90
        and _edition_has_verified_provider_identity(e)
    )
    verified_artist = str(e.get("_verified_artist_name") or "").strip()
    verified_album = str(e.get("_verified_album_title") or "").strip()
    verified_provider_override = bool(
        bool(e.get("strict_match_verified"))
        and verified_artist
        and verified_album
    )
    if hint_source == "ai_local_context" and not _edition_has_verified_provider_identity(e):
        hinted_artist = ""
        hinted_album = ""
    if verified_provider_override:
        artist_resolved = verified_artist
        album_resolved = verified_album
    elif force_hint_override or verified_hint_override:
        logging.info(
            "[Identity] forcing filename-pattern override artist=%r -> %r album=%r -> %r reason=%s confidence=%s verified=%s",
            current_artist,
            hinted_artist,
            current_title,
            hinted_album,
            hint_reason or "",
            hint_confidence,
            bool(verified_hint_override),
        )
        artist_resolved = hinted_artist
        album_resolved = hinted_album
    else:
        artist_resolved = _prefer_identity_hint_value(
            current_value=current_artist,
            hinted_value=hinted_artist,
            field_name="artist",
            missing_required=missing_required,
            folder_name=folder_name,
        )
        album_resolved = _prefer_identity_hint_value(
            current_value=current_title,
            hinted_value=hinted_album,
            field_name="album",
            missing_required=missing_required,
            folder_name=folder_name,
        )
    artist_final = artist_resolved or current_artist or default_artist or "Unknown Artist"
    album_final = album_resolved or current_title or default_title or "Unknown Album"
    return (artist_final.strip(), _sanitize_album_title_display(album_final))


def _infer_identity_from_local_context_ai(
    *,
    local_artist: str,
    local_album: str,
    folder_path: Path | str | None,
    track_titles: list[str],
    file_paths: list[Path | str] | None = None,
    local_tags: dict | None = None,
    missing_required_tags: list[str] | None = None,
    force_try: bool = False,
) -> dict[str, Any]:
    """
    Infer artist+album identity from folder/file context when tags are missing/weak.
    Returns {"artist","album","confidence","reason","source"} or {}.
    """
    titles = [str(t or "").strip() for t in (track_titles or []) if str(t or "").strip()][:40]
    if not titles:
        return {}
    missing = [str(x or "").strip().lower() for x in (missing_required_tags or []) if str(x or "").strip()]
    local_artist_txt = str(local_artist or "").strip()
    local_album_txt = str(local_album or "").strip()
    folder_txt = str(folder_path or "").strip()
    try:
        p = Path(folder_txt) if folder_txt else None
    except Exception:
        p = None
    folder_name = (p.name if p else "").strip()
    parent_name = (p.parent.name if p and p.parent else "").strip()

    album_prefix_hint = _album_hint_from_track_titles(titles)
    filename_hints = _filename_identity_hints(file_paths)
    filename_artist_hint = str(filename_hints.get("artist") or "").strip()
    filename_album_hint = str(filename_hints.get("album") or "").strip()
    local_context = _classical_identity_context(
        local_artist=local_artist_txt,
        local_title=local_album_txt,
        local_tracks=[{"title": value} for value in titles[:20]],
        local_tags=local_tags if isinstance(local_tags, dict) else {},
        local_paths=list(file_paths or []),
    )
    local_is_classical = bool(local_context.get("is_classical"))
    filename_album_conflict = bool(
        filename_album_hint
        and local_album_txt
        and _normalize_identity_text_strict(filename_album_hint) != _normalize_identity_text_strict(local_album_txt)
    )
    filename_artist_conflict = bool(
        filename_artist_hint
        and local_artist_txt
        and _normalize_identity_text_strict(filename_artist_hint) != _normalize_identity_text_strict(local_artist_txt)
    )
    allow_filename_conflict_override = bool(
        not local_is_classical
        and bool(filename_artist_hint)
        and bool(filename_album_hint)
        and bool(local_album_txt)
        and filename_album_conflict
        and not filename_artist_conflict
        and _identity_artist_fallback_is_usable(local_artist_txt)
        and _identity_album_fallback_is_usable(local_album_txt, missing_required=missing, folder_name=folder_name)
    )
    should_try = _should_try_local_context_identity_ai(
        local_artist=local_artist_txt,
        local_album=local_album_txt,
        folder_name=folder_name,
        missing_required_tags=missing,
        force_try=force_try,
    ) or allow_filename_conflict_override
    if not should_try:
        return {}

    stable_filename_identity = bool(filename_artist_hint and filename_album_hint)
    if stable_filename_identity:
        local_artist_norm = _normalize_identity_text_strict(local_artist_txt)
        filename_artist_norm = _normalize_identity_text_strict(filename_artist_hint)
        parent_norm = _normalize_identity_text_strict(parent_name)
        artist_needs_help = bool(
            ("artist" in missing)
            or _identity_text_is_generic(local_artist_txt)
            or not local_artist_txt
        )
        album_needs_help = bool(
            ("album" in missing)
            or _identity_text_is_generic(local_album_txt)
            or not local_album_txt
        )
        deterministic_reason_bits: list[str] = []
        if artist_needs_help:
            deterministic_reason_bits.append("artist_missing_or_generic")
        if album_needs_help:
            deterministic_reason_bits.append("album_missing_or_generic")
        if deterministic_reason_bits:
            return {
                "artist": filename_artist_hint if artist_needs_help else local_artist_txt,
                "album": filename_album_hint if album_needs_help else local_album_txt,
                "confidence": 96,
                "reason": "stable filename pattern (" + ", ".join(deterministic_reason_bits) + ")",
                "source": "filename_pattern",
                "album_prefix_hint": album_prefix_hint or "",
                "filename_artist_hint": filename_artist_hint,
                "filename_album_hint": filename_album_hint,
            }
        artist_confirms_filename = bool(
            filename_artist_norm
            and (
                (local_artist_norm and filename_artist_norm == local_artist_norm)
                or (parent_norm and filename_artist_norm == parent_norm)
            )
        )
        if (
            not local_is_classical
            and artist_confirms_filename
            and filename_album_conflict
            and not filename_artist_conflict
            and local_album_txt
            and not _identity_text_is_generic(local_album_txt)
        ):
            return {
                "artist": filename_artist_hint or local_artist_txt,
                "album": filename_album_hint,
                "confidence": 97,
                "reason": "stable filename pattern (album_conflict_same_artist)",
                "source": "filename_pattern",
                "album_prefix_hint": album_prefix_hint or "",
                "filename_artist_hint": filename_artist_hint,
                "filename_album_hint": filename_album_hint,
            }
    if not bool(getattr(sys.modules[__name__], "ai_provider_ready", False)):
        return {}

    provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
    model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
    prompt = (
        "Infer the correct music artist and album identity from local filesystem context.\n"
        "Return ONLY JSON object with keys: artist, album, confidence, reason.\n"
        "confidence must be integer 0..100.\n"
        "If uncertain, set confidence < 60.\n\n"
        f"Current local artist: {local_artist_txt!r}\n"
        f"Current local album: {local_album_txt!r}\n"
        f"Folder name: {folder_name!r}\n"
        f"Parent folder: {parent_name!r}\n"
        f"Missing required tags: {missing or []}\n"
        f"Track titles from files ({len(titles)}):\n- " + "\n- ".join(titles[:30]) + "\n"
    )
    if album_prefix_hint:
        prompt += f"\nDetected filename-prefix album hint: {album_prefix_hint!r}\n"
    if filename_artist_hint:
        prompt += f"\nDetected common filename artist hint: {filename_artist_hint!r}\n"
    if filename_album_hint:
        prompt += f"\nDetected common filename album hint: {filename_album_hint!r}\n"
    prompt += "\nDo not invent random artists/albums. Prefer exact identity visible in filenames."
    if force_try:
        prompt += "\nCurrent tags/title may be wrong. If track titles clearly indicate a different known album, prefer the track-title evidence."

    try:
        out = _call_ai_provider_bounded(
            provider=provider,
            model=model,
            system_msg="Return strict JSON object only. No markdown.",
            user_msg=prompt,
            max_tokens=220,
            analysis_type="identity_inference_no_tags",
            timeout_sec=AI_SCAN_HARD_TIMEOUT_SEC,
            log_prefix="[AI Identity]",
        )
        obj = _assistant_extract_json_obj(out or "")
        artist_guess = str(obj.get("artist") or "").strip()
        album_guess = str(obj.get("album") or "").strip()
        conf_raw = obj.get("confidence")
        try:
            conf = int(float(conf_raw))
        except Exception:
            conf = 0
        conf = max(0, min(100, conf))
        if conf < 65:
            return {}
        if not artist_guess or not album_guess:
            return {}
        if _identity_text_is_generic(artist_guess) or _identity_text_is_generic(album_guess):
            return {}
        return {
            "artist": artist_guess,
            "album": album_guess,
            "confidence": conf,
            "reason": str(obj.get("reason") or "").strip(),
            "source": "ai_local_context",
            "album_prefix_hint": album_prefix_hint or "",
            "filename_artist_hint": filename_artist_hint or "",
            "filename_album_hint": filename_album_hint or "",
        }
    except Exception as e:
        logging.debug(
            "[AI Identity] local context inference failed artist=%r album=%r folder=%r: %s",
            local_artist_txt,
            local_album_txt,
            folder_txt,
            e,
        )
        return {}


def _extract_files_identity_fields(
    *,
    tags: dict | None = None,
    edition: dict | None = None,
    cached: dict | None = None,
) -> dict:
    """
    Resolve album identity in Files mode.
    Identity can come from:
    - MusicBrainz ID
    - Fallback provider match (Discogs / Last.fm / Bandcamp)
    """
    tags = dict(tags or {})
    edition = dict(edition or {})
    cached = dict(cached or {})

    mbid = (
        str(
            edition.get("musicbrainz_id")
            or _extract_musicbrainz_id_from_meta(tags)
            or cached.get("musicbrainz_id")
            or ""
        ).strip()
    )
    musicbrainz_release_id = str(
        edition.get("musicbrainz_release_id")
        or _extract_musicbrainz_release_id_from_meta(tags)
        or cached.get("musicbrainz_release_id")
        or ""
    ).strip()
    discogs_release_id = str(
        edition.get("discogs_release_id")
        or tags.get("discogs_release_id")
        or cached.get("discogs_release_id")
        or ""
    ).strip()
    lastfm_album_mbid = str(
        edition.get("lastfm_album_mbid")
        or tags.get("lastfm_album_mbid")
        or cached.get("lastfm_album_mbid")
        or ""
    ).strip()
    bandcamp_album_url = str(
        edition.get("bandcamp_album_url")
        or tags.get("bandcamp_album_url")
        or cached.get("bandcamp_album_url")
        or ""
    ).strip()
    metadata_source_raw = (
        edition.get("metadata_source")
        or edition.get("primary_metadata_source")
        or edition.get("provider_used")
        or edition.get("pmda_match_provider")
        or tags.get(PMDA_MATCH_PROVIDER_TAG)
        or cached.get("metadata_source")
        or cached.get("identity_provider")
        or ""
    )
    metadata_source = _normalize_identity_provider(str(metadata_source_raw or ""))
    if "strict_match_verified" in edition:
        strict_match_verified = bool(edition.get("strict_match_verified"))
    elif "strict_match_verified" in cached:
        strict_match_verified = bool(cached.get("strict_match_verified"))
    else:
        strict_match_verified = False
    strict_match_provider = _normalize_identity_provider(
        str(
            edition.get("strict_match_provider")
            or cached.get("strict_match_provider")
            or ""
        )
    )
    strict_reject_reason = str(
        edition.get("strict_reject_reason")
        or cached.get("strict_reject_reason")
        or ""
    ).strip()
    try:
        strict_tracklist_score = float(
            edition.get("strict_tracklist_score")
            if ("strict_tracklist_score" in edition)
            else cached.get("strict_tracklist_score")
        )
    except Exception:
        strict_tracklist_score = 0.0
    soft_match_verified = bool(
        edition.get("provider_identity_soft_match")
        or edition.get("soft_match_verified")
        or cached.get("soft_match_verified")
    )
    soft_match_provider = _normalize_identity_provider(
        str(
            edition.get("identity_provider")
            or edition.get("provider_identity_soft_provider")
            or edition.get("provider_identity_soft_match_provider")
            or edition.get("soft_match_provider")
            or cached.get("soft_match_provider")
            or ""
        )
    )
    has_identity = bool(strict_match_verified or soft_match_verified)
    identity_provider = strict_match_provider if strict_match_verified else soft_match_provider
    # Backward-compatible fallback: older cache rows may carry has_identity without strict_* columns.
    if not has_identity:
        if "has_identity" in edition:
            has_identity = bool(edition.get("has_identity"))
        elif "has_identity" in cached:
            has_identity = bool(cached.get("has_identity"))
        if has_identity and not identity_provider:
            identity_provider = _normalize_identity_provider(
                str(
                    edition.get("identity_provider")
                    or cached.get("identity_provider")
                    or metadata_source
                    or ""
                )
            )
    # Pragmatic identity fallback: if a trusted provider ID exists, keep identity for caching/run-scope.
    # Mutations are still controlled by strict_* gates.
    if not has_identity and _has_trusted_album_identity(
        musicbrainz_id=mbid,
        discogs_release_id=discogs_release_id,
        lastfm_album_mbid=lastfm_album_mbid,
        bandcamp_album_url=bandcamp_album_url,
    ):
        has_identity = True
        if not identity_provider:
            if mbid:
                identity_provider = "musicbrainz"
            elif discogs_release_id:
                identity_provider = "discogs"
            elif lastfm_album_mbid:
                identity_provider = "lastfm"
            elif bandcamp_album_url:
                identity_provider = "bandcamp"
            elif metadata_source:
                identity_provider = metadata_source
    return {
        "musicbrainz_id": mbid,
        "musicbrainz_release_id": musicbrainz_release_id,
        "has_mbid": bool(mbid),
        "discogs_release_id": discogs_release_id,
        "lastfm_album_mbid": lastfm_album_mbid,
        "bandcamp_album_url": bandcamp_album_url,
        "metadata_source": metadata_source,
        "identity_provider": identity_provider,
        "has_identity": has_identity,
        "soft_match_verified": bool(soft_match_verified),
        "soft_match_provider": soft_match_provider,
        "strict_match_verified": bool(strict_match_verified),
        "strict_match_provider": strict_match_provider,
        "strict_reject_reason": strict_reject_reason,
        "strict_tracklist_score": strict_tracklist_score,
    }


def _build_album_provider_crosscheck(
    *,
    artist_name: str,
    album_title: str,
    local_track_titles: list[str],
    selected_provider: str,
    known_mbid: str,
    known_discogs_release_id: str,
    known_lastfm_album_mbid: str,
    known_bandcamp_album_url: str,
) -> list[dict[str, Any]]:
    provider_payloads: dict[str, Any] = {
        "musicbrainz": None,
        "discogs": None,
        "lastfm": None,
        "bandcamp": None,
    }
    try:
        fetched = _fetch_album_provider_fallbacks_parallel(artist_name, album_title) or {}
    except Exception:
        fetched = {}
    for p in ("discogs", "lastfm", "bandcamp"):
        payload = fetched.get(p)
        if isinstance(payload, dict):
            provider_payloads[p] = payload
    if str(known_bandcamp_album_url or "").strip():
        try:
            payload = _fetch_bandcamp_album_info(
                artist_name,
                album_title,
                allow_web_fallback=False,
                album_url_hint=str(known_bandcamp_album_url or "").strip(),
            )
            if isinstance(payload, dict):
                provider_payloads["bandcamp"] = payload
        except Exception:
            pass

    mbid = str(known_mbid or "").strip()
    mb_ai_used = False
    if mbid:
        try:
            mb_payload = _fetch_musicbrainz_strict_payload(mbid)
            if isinstance(mb_payload, dict):
                provider_payloads["musicbrainz"] = mb_payload
        except Exception:
            pass
    elif USE_MUSICBRAINZ:
        try:
            rg_info, verified_by_ai = search_mb_release_group_by_metadata(
                artist_name,
                norm_album(album_title or ""),
                set(local_track_titles or []),
                title_raw=album_title,
                scan_inline=False,
            )
            if isinstance(rg_info, dict) and str(rg_info.get("id") or "").strip():
                mbid = str(rg_info.get("id") or "").strip()
                mb_payload = _fetch_musicbrainz_strict_payload(mbid)
                if isinstance(mb_payload, dict):
                    provider_payloads["musicbrainz"] = mb_payload
                mb_ai_used = bool(verified_by_ai)
        except Exception:
            pass

    expected_by_provider = {
        "musicbrainz": str(mbid or "").strip(),
        "discogs": str(known_discogs_release_id or "").strip(),
        "lastfm": str(known_lastfm_album_mbid or "").strip(),
        "bandcamp": str(known_bandcamp_album_url or "").strip(),
    }
    selected = _normalize_identity_provider(selected_provider)
    local_titles = [str(t or "").strip() for t in (local_track_titles or []) if str(t or "").strip()]
    local_track_count = int(len(local_titles))
    out: list[dict[str, Any]] = []

    for provider in _MATCH_PROVIDER_ORDER:
        payload = provider_payloads.get(provider)
        payload_dict = payload if isinstance(payload, dict) else None
        provider_id = (
            _provider_id_for_strict(provider, payload_dict or {})
            or expected_by_provider.get(provider, "")
        ).strip()
        title_val = _provider_payload_title(provider, payload_dict)
        artist_val = _provider_payload_artist(provider, payload_dict)
        cover_url = _provider_cover_url_from_payload(provider, payload_dict)
        year_val = _provider_year_from_payload(payload_dict)
        provider_titles = _provider_track_titles_for_strict(provider, payload_dict or {})
        provider_track_count = int(len(provider_titles))
        if local_titles and provider_titles:
            track_score = max(
                float(_crosscheck_tracklist(local_titles, provider_titles)),
                float(_crosscheck_tracklist_perfect(local_titles, provider_titles)),
            )
            track_count_ratio = min(local_track_count, provider_track_count) / max(local_track_count, provider_track_count)
        else:
            track_score = 0.0
            track_count_ratio = 1.0
        title_score = float(_provider_identity_text_score(album_title, title_val)) if title_val else 0.0
        artist_score = float(_provider_identity_text_score(artist_name, artist_val)) if artist_val else 0.0
        confidence = 0.0
        if title_val or artist_val:
            if local_titles and provider_titles:
                confidence = (title_score * 0.45) + (artist_score * 0.35) + (track_score * 0.20)
                if track_count_ratio < 0.55:
                    confidence -= 0.25
                elif track_count_ratio < 0.75:
                    confidence -= 0.10
            elif provider_titles:
                confidence = (title_score * 0.50) + (artist_score * 0.35) + (track_score * 0.15)
            else:
                confidence = (title_score * 0.56) + (artist_score * 0.44)
            confidence = max(0.0, min(1.0, confidence))
        strict = _strict_provider_match_100(
            local_artist=artist_name,
            local_title=album_title,
            local_tracks=local_titles,
            provider=provider,
            provider_payload=payload_dict or {},
            expected_provider_id=expected_by_provider.get(provider, ""),
        )
        cand_soft = {
            "title_score": title_score,
            "artist_score": artist_score,
            "track_score": track_score,
            "confidence": confidence,
            "has_provider_tracklist": bool(provider_track_count > 0),
            "has_local_tracklist": bool(local_track_count > 0),
            "track_count_ratio": float(track_count_ratio),
        }
        soft_ok, soft_reason = _provider_candidate_soft_identity_ok(cand_soft, min_confidence=max(0.62, PROVIDER_IDENTITY_MIN_SCORE - 0.08))
        source_url = ""
        if provider == "musicbrainz" and payload_dict:
            source_url = str(payload_dict.get("url") or "").strip()
        if not source_url:
            source_url = _provider_reference_link(
                provider=provider,
                ref=provider_id,
                artist_name=artist_name,
                album_title=album_title,
            ) or ""
        if not source_url and provider == "bandcamp" and payload_dict:
            source_url = str(payload_dict.get("album_url") or "").strip()
        has_review = False
        if payload_dict:
            if provider == "lastfm":
                has_review = bool(
                    str(payload_dict.get("wiki_summary") or "").strip()
                    or str(payload_dict.get("wiki_content") or "").strip()
                )
            elif provider == "bandcamp":
                has_review = bool(
                    str(payload_dict.get("description") or "").strip()
                    or str(payload_dict.get("about") or "").strip()
                )

        versions = _provider_versions_from_payload(provider, payload_dict)
        versions_count = None
        if provider == "musicbrainz":
            try:
                versions_count = int((payload_dict or {}).get("release_count") or 0)
            except Exception:
                versions_count = 0

        out.append(
            {
                "provider": provider,
                "label": _match_provider_label(provider),
                "attempted": bool(payload_dict) or bool(provider_id),
                "selected": provider == selected,
                "provider_id": provider_id or None,
                "source_url": source_url or None,
                "title": title_val or None,
                "artist": artist_val or None,
                "year": year_val,
                "cover_url": cover_url or None,
                "track_count": provider_track_count,
                "local_track_count": local_track_count,
                "title_score": round(title_score, 4),
                "artist_score": round(artist_score, 4),
                "track_score": round(track_score, 4),
                "confidence": round(confidence, 4),
                "strict_match_verified": bool(strict.get("strict_match_verified")),
                "strict_reject_reason": str(strict.get("strict_reject_reason") or "").strip() or None,
                "soft_match_ok": bool(soft_ok),
                "soft_match_reason": str(soft_reason or "").strip() or None,
                "has_review": bool(has_review),
                "ai_used": bool(provider == "musicbrainz" and mb_ai_used),
                "ai_confidence": None,
                "versions_count": versions_count,
                "versions": versions,
            }
        )
    return out


def _strict_validate_edition_match(
    *,
    artist_name: str,
    album_title: str,
    edition: dict,
) -> dict:
    """Run final strict provider validation for one local edition."""
    local_tracks = list(edition.get("tracks") or [])
    attempts: list[str] = []
    for provider in ("musicbrainz", "discogs", "itunes", "deezer", "bandcamp", "lastfm"):
        try:
            expected_id = _strict_expected_provider_id(provider, edition)
            prefetched_payload = None
            if provider == str(edition.get("primary_metadata_source") or "").strip().lower():
                prefetched_payload = edition.get("_strict_provider_payload")
            payload = None
            if isinstance(prefetched_payload, dict):
                payload = prefetched_payload
            elif _strict_provider_cold_fetch_allowed(provider, edition):
                payload = _strict_payload_for_provider(
                    provider,
                    artist_name=artist_name,
                    album_title=album_title,
                    edition=edition,
                )
            if not expected_id and not isinstance(payload, dict):
                continue
            verdict = _strict_provider_match_100(
                local_artist=artist_name,
                local_title=album_title,
                local_tracks=local_tracks,
                local_tags=edition.get("tags") if isinstance(edition.get("tags"), dict) else {},
                local_paths=list(edition.get("ordered_paths") or []),
                provider=provider,
                provider_payload=payload or {},
                expected_provider_id=expected_id,
            )
        except Exception as exc:
            logging.debug(
                "[Strict Validate] provider validation failed provider=%s artist=%r album=%r: %s",
                provider,
                artist_name,
                album_title,
                exc,
            )
            attempts.append(f"{provider}:provider_unreachable")
            continue
        if bool(verdict.get("strict_match_verified")):
            return verdict
        attempts.append(f"{provider}:{verdict.get('strict_reject_reason') or 'strict_reject'}")
    reason = "provider_no_tracklist"
    if attempts:
        reason = str(attempts[0].split(":", 1)[-1] or "strict_reject")
    return {
        "strict_match_verified": False,
        "strict_match_provider": "",
        "strict_reject_reason": reason,
        "strict_tracklist_score": 0.0,
        "strict_attempts": attempts,
    }

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}

def ai_verify_mb_match_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return ai_verify_mb_match(*args, **kwargs)

def _identify_album_by_acoustic_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identify_album_by_acoustic_id(*args, **kwargs)

def _musicbrainz_artist_identity_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _musicbrainz_artist_identity_lookup(*args, **kwargs)

def _classical_track_entries_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _classical_track_entries(*args, **kwargs)

def _classical_total_duration_ms_for_paths_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _classical_total_duration_ms_for_paths(*args, **kwargs)

def _classical_identity_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _classical_identity_context(*args, **kwargs)

def _provider_classical_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_classical_context(*args, **kwargs)

def _classical_context_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _classical_context_for_edition(*args, **kwargs)

def _classical_identity_match_details_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _classical_identity_match_details(*args, **kwargs)

def _strict_identity_match_details_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_identity_match_details(*args, **kwargs)

def _score_musicbrainz_release_payload_for_local_context_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _score_musicbrainz_release_payload_for_local_context(*args, **kwargs)

def _fetch_musicbrainz_strict_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_musicbrainz_strict_payload(*args, **kwargs)

def _fetch_musicbrainz_strict_payload_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_musicbrainz_strict_payload_for_edition(*args, **kwargs)

def _strict_expected_provider_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_expected_provider_id(*args, **kwargs)

def _strict_payload_for_provider_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_payload_for_provider(*args, **kwargs)

def _strict_provider_cold_fetch_allowed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_provider_cold_fetch_allowed(*args, **kwargs)

def _strict_validate_edition_match_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_validate_edition_match(*args, **kwargs)

def _strict_clear_identity_on_reject_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_clear_identity_on_reject(*args, **kwargs)

def _resolve_edition_display_identity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _resolve_edition_display_identity(*args, **kwargs)

def _infer_identity_from_local_context_ai_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _infer_identity_from_local_context_ai(*args, **kwargs)

def _extract_files_identity_fields_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_files_identity_fields(*args, **kwargs)

def _build_album_provider_crosscheck_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_album_provider_crosscheck(*args, **kwargs)

def _normalize_identity_text_strict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_identity_text_strict(*args, **kwargs)

def _normalize_identity_album_strict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _normalize_identity_album_strict(*args, **kwargs)

def _split_identity_artist_credits_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _split_identity_artist_credits(*args, **kwargs)

def _strip_identity_artist_feature_clause_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strip_identity_artist_feature_clause(*args, **kwargs)

def _identity_artist_credit_norms_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_artist_credit_norms(*args, **kwargs)

def _identity_norm_is_various_artist_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_norm_is_various_artist(*args, **kwargs)

def _identity_artist_is_various_artists_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_artist_is_various_artists(*args, **kwargs)

def _identity_tags_mark_compilation_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_tags_mark_compilation(*args, **kwargs)

def _identity_artist_credit_overlap_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_artist_credit_overlap(*args, **kwargs)

def _strip_identity_album_trailing_markers_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strip_identity_album_trailing_markers(*args, **kwargs)

def _identity_album_variant_norms_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_album_variant_norms(*args, **kwargs)

def _identity_album_equivalent_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _identity_album_equivalent(*args, **kwargs)

def _provider_identity_album_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_identity_album_score(*args, **kwargs)

def _provider_identity_artist_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_identity_artist_score(*args, **kwargs)

def _extract_mb_artist_names_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _extract_mb_artist_names(*args, **kwargs)
