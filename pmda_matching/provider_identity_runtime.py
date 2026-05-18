"""Runtime-owned provider identity arbitration and confidence helpers."""
from __future__ import annotations

import logging
import re
import sys
import unicodedata
from typing import Any, List, Optional

_EXTRACTED_NAMES = {
    '_provider_identity_text_score',
    '_strict_reject_code',
    '_norm_track_title_strict',
    '_local_track_titles_for_strict',
    '_provider_track_titles_for_strict',
    '_provider_id_for_strict',
    '_strict_tracklist_match_details',
    '_strict_candidate_artist_text',
    '_strict_year_from_tags',
    '_strict_year_from_payload',
    '_strict_flat_text_values',
    '_strict_tag_text_tokens',
    '_strict_payload_text_tokens',
    '_strict_secondary_identity_signal',
    '_strict_tracklist_similarity_details',
    '_strict_smart_provider_match_verdict',
    '_strict_provider_match_100',
    '_provider_candidate_id',
    '_build_provider_identity_candidates',
    '_provider_candidate_soft_identity_ok',
    '_provider_candidate_match_classification',
    '_annotate_provider_identity_candidates',
    '_provider_identity_ai_skip_reason',
    '_provider_candidate_near_perfect_identity',
    '_provider_candidates_support_consensus',
    '_edition_soft_identity_survives_strict_reject',
    '_ai_choose_provider_identity_candidate',
    '_arbitrate_provider_identity',
    '_strict_discogs_payload_from_release_data',
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
        if name == "_bind_runtime" or name.endswith("_for_runtime"):
            continue
        globals()[name] = value

def _provider_identity_text_score(local_value: str, provider_value: str) -> float:
    """
    Lightweight fuzzy score used to pre-filter provider candidates before strict gating.
    """
    pairs = [
        (
            _normalize_identity_text_strict(local_value or ""),
            _normalize_identity_text_strict(provider_value or ""),
        ),
        (
            norm_album(local_value or ""),
            norm_album(provider_value or ""),
        ),
    ]
    best = 0.0
    try:
        from difflib import SequenceMatcher
    except Exception:
        SequenceMatcher = None  # type: ignore[assignment]

    for local_norm, provider_norm in pairs:
        if not local_norm or not provider_norm:
            continue
        if local_norm == provider_norm:
            best = max(best, 1.0)
            continue
        if local_norm in provider_norm or provider_norm in local_norm:
            best = max(best, 0.9)
            continue
        if SequenceMatcher is not None:
            try:
                best = max(best, float(SequenceMatcher(None, local_norm, provider_norm).ratio()))
            except Exception:
                pass
    return float(best)


def _strict_reject_code(raw_reason: str) -> str:
    reason = str(raw_reason or "").strip().lower()
    reason = reason.replace("_", " ").replace("-", " ")
    reason = " ".join(reason.split())
    if not reason:
        return "strict_reject"
    if "classical_work_mismatch" in reason:
        return "classical_work_mismatch"
    if "classical_composer_mismatch" in reason:
        return "classical_composer_mismatch"
    if "classical_performance_mismatch" in reason:
        return "classical_performance_mismatch"
    if "classical_track_count_mismatch" in reason:
        return "classical_track_count_mismatch"
    if "classical_disc_count_mismatch" in reason:
        return "classical_disc_count_mismatch"
    if "classical_duration_mismatch" in reason:
        return "classical_duration_mismatch"
    if "classical_label_plus_performance_mismatch" in reason:
        return "classical_label_plus_performance_mismatch"
    if "classical_year_mismatch" in reason:
        return "classical_year_mismatch"
    if "classical_context_insufficient" in reason:
        return "classical_context_insufficient"
    if "artist partial overlap" in reason:
        return "artist_partial_overlap"
    if "local artist missing" in reason or "candidate artist missing" in reason or "artist mismatch" in reason:
        return "artist_mismatch"
    if "local title missing" in reason or "candidate title missing" in reason or "title mismatch" in reason:
        return "album_mismatch"
    if "provider id missing" in reason:
        return "provider_id_missing"
    if "provider id mismatch" in reason:
        return "provider_id_mismatch"
    if "tracklist missing" in reason:
        return "provider_no_tracklist"
    if "track count mismatch" in reason:
        return "track_count_mismatch"
    if "track title mismatch" in reason:
        return "track_title_mismatch"
    return "strict_reject"


def _norm_track_title_strict(value: str | None) -> str:
    cleaned = _clean_track_title_from_text(str(value or ""), 1)
    raw = (cleaned or value or "").strip().lower()
    if not raw:
        return ""
    had_remaster = bool(re.search(r"\bremaster", raw))
    raw = _fold_music_ascii(raw)
    raw = unicodedata.normalize("NFKD", unicodedata.normalize("NFKC", raw))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = re.sub(r"^\s*\d+\s*[-.)]\s*", "", raw)
    raw = raw.replace("&", " and ")
    raw = re.sub(r"[^\w\s]+", " ", raw)
    raw = " ".join(raw.split()).strip()
    if not raw:
        return ""
    if had_remaster:
        tokens = [t for t in raw.split() if not re.fullmatch(r"remaster(?:ed|ing)?", t)]
        if len(tokens) >= 2 and re.fullmatch(r"(19|20)\d{2}", tokens[-1] or ""):
            tokens = tokens[:-1]
        raw = " ".join(tokens).strip()
    return raw[:240]


def _local_track_titles_for_strict(local_tracks: List) -> List[str]:
    """
    Build deterministic local track-title order for strict matching.
    Uses (disc, track) when available, otherwise preserves original order.
    """
    if not local_tracks:
        return []
    items: list[tuple[int, int, int, str]] = []
    for i, tr in enumerate(local_tracks):
        if isinstance(tr, str):
            title = tr.strip()
            disc = 1
            idx = i + 1
        elif isinstance(tr, dict):
            title = str(tr.get("title") or "").strip()
            disc = _parse_int_loose(tr.get("disc") or tr.get("disc_num"), 1) or 1
            idx = _parse_int_loose(
                tr.get("idx") or tr.get("index") or tr.get("track_num") or tr.get("track"),
                i + 1,
            ) or (i + 1)
        else:
            title = str(getattr(tr, "title", "") or "").strip()
            disc = _parse_int_loose(getattr(tr, "disc", 1), 1) or 1
            idx = _parse_int_loose(getattr(tr, "idx", i + 1), i + 1) or (i + 1)
        if not title:
            continue
        items.append((disc, idx, i, title))
    items.sort(key=lambda t: (int(t[0] or 1), int(t[1] or 0), int(t[2])))
    return [t[3] for t in items]


def _provider_track_titles_for_strict(provider: str, payload: dict | None) -> List[str]:
    if not isinstance(payload, dict):
        return []
    p = (provider or "").strip().lower()
    if p in {"musicbrainz", "discogs", "bandcamp", "lastfm", "itunes", "deezer", "spotify", "qobuz", "tidal", "audiodb"}:
        tracklist = payload.get("tracklist") or payload.get("track_titles") or []
        if isinstance(tracklist, list):
            out: list[str] = []
            for t in tracklist:
                if isinstance(t, dict):
                    title = (t.get("title") or t.get("name") or "").strip()
                else:
                    title = str(t or "").strip()
                if title:
                    out.append(title)
            return out
    return []


def _provider_id_for_strict(provider: str, payload: dict | None) -> str:
    p = (provider or "").strip().lower()
    if not isinstance(payload, dict):
        return ""
    if p == "musicbrainz":
        return str(payload.get("id") or payload.get("musicbrainz_id") or "").strip()
    if p == "discogs":
        return str(payload.get("release_id") or payload.get("master_id") or "").strip()
    if p == "lastfm":
        return str(payload.get("mbid") or "").strip()
    if p == "bandcamp":
        return str(payload.get("album_url") or payload.get("url") or "").strip()
    if p == "itunes":
        return str(payload.get("collection_id") or payload.get("collectionId") or "").strip()
    if p == "deezer":
        return str(payload.get("album_id") or payload.get("id") or "").strip()
    if p in {"spotify", "qobuz", "tidal"}:
        return str(payload.get("album_id") or payload.get("id") or payload.get("url") or "").strip()
    if p == "audiodb":
        return str(payload.get("album_id") or payload.get("idAlbum") or payload.get("url") or "").strip()
    return ""


def _strict_tracklist_match_details(local_titles: List[str], provider_titles: List[str]) -> tuple[bool, str]:
    local_norm = [_norm_track_title_strict(x) for x in (local_titles or []) if str(x or "").strip()]
    provider_norm = [_norm_track_title_strict(x) for x in (provider_titles or []) if str(x or "").strip()]
    if not provider_norm:
        return (False, "provider tracklist missing")
    if len(local_norm) != len(provider_norm):
        return (False, f"track count mismatch local={len(local_norm)} provider={len(provider_norm)}")
    for idx, (lt, pt) in enumerate(zip(local_norm, provider_norm), start=1):
        if lt != pt:
            return (False, f"track title mismatch at position {idx}: local={lt!r} provider={pt!r}")
    return (True, "tracklist exact")


def _strict_candidate_artist_text(candidate_artist: Any) -> str:
    if isinstance(candidate_artist, (list, tuple, set)):
        return "; ".join(str(v or "").strip() for v in candidate_artist if str(v or "").strip())
    return str(candidate_artist or "").strip()


def _strict_year_from_tags(tags: dict | None) -> str:
    if not isinstance(tags, dict):
        return ""
    for key in (
        "originaldate",
        "originalyear",
        "date",
        "year",
        "releasedate",
        "release_date",
    ):
        year = _mb_extract_year(tags.get(key))
        if year:
            return year
    return ""


def _strict_year_from_payload(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in (
        "year",
        "date",
        "release_date",
        "releasedate",
        "first_release_date",
        "first-release-date",
        "originaldate",
    ):
        year = _mb_extract_year(payload.get(key))
        if year:
            return year
    return ""


def _strict_flat_text_values(value: Any) -> list[str]:
    out: list[str] = []
    if isinstance(value, dict):
        for nested in value.values():
            out.extend(_strict_flat_text_values(nested))
    elif isinstance(value, (list, tuple, set)):
        for nested in value:
            out.extend(_strict_flat_text_values(nested))
    else:
        raw = str(value or "").strip()
        if raw:
            out.append(raw)
    return out


def _strict_tag_text_tokens(tags: dict | None, keys: tuple[str, ...]) -> set[str]:
    if not isinstance(tags, dict):
        return set()
    values: list[str] = []
    lower_map = {str(k or "").strip().lower(): v for k, v in tags.items()}
    for key in keys:
        if key in tags:
            values.extend(_strict_flat_text_values(tags.get(key)))
        lowered = key.lower()
        if lowered in lower_map:
            values.extend(_strict_flat_text_values(lower_map.get(lowered)))
    return {
        _normalize_identity_text_strict(v)
        for v in values
        if _normalize_identity_text_strict(v)
    }


def _strict_payload_text_tokens(payload: dict | None, keys: tuple[str, ...]) -> set[str]:
    if not isinstance(payload, dict):
        return set()
    values: list[str] = []
    for key in keys:
        if key in payload:
            values.extend(_strict_flat_text_values(payload.get(key)))
    return {
        _normalize_identity_text_strict(v)
        for v in values
        if _normalize_identity_text_strict(v)
    }


def _strict_secondary_identity_signal(local_tags: dict | None, payload: dict | None) -> tuple[bool, str]:
    """
    Extra non-tracklist evidence for smart move-safe identity.

    This intentionally requires data agreement, not just provider presence. It is used
    only when provider tracklists are absent or incomplete.
    """
    local_year = _strict_year_from_tags(local_tags)
    provider_year = _strict_year_from_payload(payload)
    if local_year and provider_year:
        try:
            if abs(int(local_year) - int(provider_year)) <= 1:
                return (True, f"year:{local_year}/{provider_year}")
        except Exception:
            if local_year == provider_year:
                return (True, f"year:{local_year}")

    label_keys = (
        "label",
        "organization",
        "publisher",
        "recordlabel",
        "record_label",
        "LABEL",
    )
    catalog_keys = (
        "catalog",
        "catalogue",
        "catalognumber",
        "catalog_number",
        "catalogue_number",
        "catno",
        "CATALOGNUMBER",
    )
    local_label = _strict_tag_text_tokens(local_tags, label_keys)
    provider_label = _strict_payload_text_tokens(payload, ("label", "labels", "publisher"))
    if local_label and provider_label and (local_label & provider_label):
        return (True, "label")
    local_catalog = _strict_tag_text_tokens(local_tags, catalog_keys)
    provider_catalog = _strict_payload_text_tokens(payload, ("catalog_number", "catalog", "catno", "catalogue_number"))
    if local_catalog and provider_catalog and (local_catalog & provider_catalog):
        return (True, "catalog")
    return (False, "")


def _strict_tracklist_similarity_details(local_titles: List[str], provider_titles: List[str]) -> dict[str, Any]:
    local_norm = [_norm_track_title_strict(x) for x in (local_titles or []) if str(x or "").strip()]
    provider_norm = [_norm_track_title_strict(x) for x in (provider_titles or []) if str(x or "").strip()]
    local_norm = [x for x in local_norm if x]
    provider_norm = [x for x in provider_norm if x]
    local_count = len(local_norm)
    provider_count = len(provider_norm)
    if not local_norm or not provider_norm:
        return {
            "score": 0.0,
            "loose_score": 0.0,
            "perfect_score": 0.0,
            "shared": 0,
            "local_count": local_count,
            "provider_count": provider_count,
            "local_coverage": 0.0,
            "provider_coverage": 0.0,
            "count_ratio": 0.0,
            "reason": "tracklist_missing",
        }
    try:
        loose_score = float(_crosscheck_tracklist(local_norm, provider_norm))
    except Exception:
        loose_score = 0.0
    try:
        perfect_score = float(_crosscheck_tracklist_perfect(local_norm, provider_norm))
    except Exception:
        perfect_score = 0.0
    local_set = {x for x in local_norm if x}
    provider_set = {x for x in provider_norm if x}
    shared = local_set & provider_set
    local_coverage = len(shared) / max(1, len(local_set))
    provider_coverage = len(shared) / max(1, len(provider_set))
    count_ratio = min(local_count, provider_count) / max(1, max(local_count, provider_count))
    positional = 0
    for lt, pt in zip(local_norm, provider_norm):
        if lt == pt:
            positional += 1
    positional_score = positional / max(1, min(local_count, provider_count))
    score = max(loose_score, perfect_score, local_coverage, provider_coverage, positional_score)
    return {
        "score": float(max(0.0, min(1.0, score))),
        "loose_score": float(max(0.0, min(1.0, loose_score))),
        "perfect_score": float(max(0.0, min(1.0, perfect_score))),
        "shared": int(len(shared)),
        "local_count": int(local_count),
        "provider_count": int(provider_count),
        "local_coverage": float(local_coverage),
        "provider_coverage": float(provider_coverage),
        "count_ratio": float(count_ratio),
        "positional_score": float(positional_score),
        "reason": "",
    }


def _strict_smart_provider_match_verdict(
    *,
    local_artist: str,
    local_title: str,
    local_titles: List[str],
    local_tags: dict | None,
    provider: str,
    provider_payload: dict,
    provider_id: str,
    expected_provider_id: str = "",
    candidate_artist: Any,
    candidate_title: str,
    id_reason: str = "",
    tracks_reason: str = "",
    local_context: dict | None = None,
) -> dict | None:
    """
    Smart but fail-closed promotion.

    The historical strict gate meant "exact provider tracklist". This helper keeps
    destructive/tag mutation safety high while allowing common real-world releases:
    bonus-track editions, suffix variants, and guest credits. It still requires a
    stable provider identity plus strong corroborating evidence.
    """
    p = _normalize_identity_provider(provider)
    if p not in {"musicbrainz", "discogs", "bandcamp", "lastfm", "itunes", "deezer"}:
        return None
    if not str(provider_id or "").strip():
        return None
    expected = str(expected_provider_id or "").strip()
    if expected:
        exp_cmp = expected.rstrip("/").lower() if p == "bandcamp" else expected
        got_cmp = str(provider_id or "").rstrip("/").lower() if p == "bandcamp" else str(provider_id or "")
        if exp_cmp != got_cmp:
            return None
    if isinstance(local_context, dict) and bool(local_context.get("is_classical")):
        # Classical matching is deliberately stricter because composer/performer/work
        # ambiguity makes fuzzy promotion unsafe.
        return None

    candidate_artist_text = _strict_candidate_artist_text(candidate_artist)
    title_score = _provider_identity_album_score(
        str(local_title or ""),
        str(candidate_title or ""),
        artist_hints=[str(local_artist or ""), candidate_artist_text],
    )
    artist_score = _provider_identity_artist_score(str(local_artist or ""), candidate_artist_text)
    equal_multi, partial_artist = _identity_artist_credit_overlap(str(local_artist or ""), candidate_artist_text)
    title_equivalent = _identity_album_equivalent(
        str(local_title or ""),
        str(candidate_title or ""),
        artist_hints=[str(local_artist or ""), candidate_artist_text],
    )
    id_code = _strict_reject_code(id_reason)
    tracks_code = _strict_reject_code(tracks_reason)
    title_gate = max(float(title_score), 1.0 if title_equivalent else 0.0)
    artist_gate = max(float(artist_score), 1.0 if equal_multi else (0.92 if partial_artist else 0.0))
    if id_code in {
        "artist_partial_overlap",
        "artist_mismatch",
        "album_mismatch",
        "classical_work_mismatch",
        "classical_composer_mismatch",
        "classical_performance_mismatch",
        "classical_track_count_mismatch",
        "classical_disc_count_mismatch",
        "classical_duration_mismatch",
        "classical_label_plus_performance_mismatch",
        "classical_year_mismatch",
    }:
        if not (
            id_code == "artist_partial_overlap"
            and title_gate >= 0.97
            and artist_gate >= 0.88
        ):
            return None
    if title_gate < 0.94:
        return None
    if artist_gate < 0.84:
        return None

    provider_titles = _provider_track_titles_for_strict(p, provider_payload)
    sim = _strict_tracklist_similarity_details(local_titles, provider_titles)
    local_count = int(sim.get("local_count") or len(local_titles or []))
    provider_count = int(sim.get("provider_count") or len(provider_titles or []))
    shared = int(sim.get("shared") or 0)
    score = float(sim.get("score") or 0.0)
    count_ratio = float(sim.get("count_ratio") or 0.0)
    local_coverage = float(sim.get("local_coverage") or 0.0)
    provider_coverage = float(sim.get("provider_coverage") or 0.0)

    if provider_titles and local_titles:
        if min(local_count, provider_count) <= 2:
            return None
        strong_identity = bool(title_gate >= 0.97 and artist_gate >= 0.88)
        very_strong_identity = bool(title_gate >= 0.995 and artist_gate >= 0.94)
        local_subset = bool(local_count <= provider_count and local_coverage >= 0.86 and shared >= min(4, local_count))
        provider_subset = bool(provider_count <= local_count and provider_coverage >= 0.86 and shared >= min(4, provider_count))
        close_variant = bool(
            abs(local_count - provider_count) <= 2
            and count_ratio >= 0.78
            and score >= 0.72
        )
        high_overlap = bool(score >= 0.84 and count_ratio >= 0.62 and shared >= min(5, min(local_count, provider_count)))
        if strong_identity and (local_subset or provider_subset or close_variant or high_overlap):
            reason = "smart_tracklist_variant"
            if id_code == "artist_partial_overlap":
                reason = "smart_guest_artist_overlap"
            elif tracks_code == "track_count_mismatch":
                reason = "smart_track_count_variant"
            return {
                "strict_match_verified": True,
                "strict_match_provider": p,
                "strict_reject_reason": "",
                "strict_tracklist_score": max(0.90, min(0.99, score)),
                "provider_id": provider_id,
                "strict_artist_name": _provider_payload_artist(p, provider_payload),
                "strict_album_title": _provider_payload_title(p, provider_payload),
                "smart_match_verified": True,
                "smart_match_reason": reason,
                "smart_match_evidence": {
                    "title_score": round(title_gate, 4),
                    "artist_score": round(artist_gate, 4),
                    "track_score": round(score, 4),
                    "local_coverage": round(local_coverage, 4),
                    "provider_coverage": round(provider_coverage, 4),
                    "local_tracks": local_count,
                    "provider_tracks": provider_count,
                    "shared_tracks": shared,
                },
            }
        if very_strong_identity and score >= 0.68 and count_ratio >= 0.55 and shared >= 3:
            return {
                "strict_match_verified": True,
                "strict_match_provider": p,
                "strict_reject_reason": "",
                "strict_tracklist_score": max(0.88, min(0.96, score)),
                "provider_id": provider_id,
                "strict_artist_name": _provider_payload_artist(p, provider_payload),
                "strict_album_title": _provider_payload_title(p, provider_payload),
                "smart_match_verified": True,
                "smart_match_reason": "smart_high_identity_track_overlap",
                "smart_match_evidence": {
                    "title_score": round(title_gate, 4),
                    "artist_score": round(artist_gate, 4),
                    "track_score": round(score, 4),
                    "count_ratio": round(count_ratio, 4),
                    "shared_tracks": shared,
                },
            }
        return None

    # No provider tracklist: do not infer full albums from title/artist fuzz alone.
    # Require exact-ish title/artist plus a stable provider id and a secondary signal.
    secondary_ok, secondary_reason = _strict_secondary_identity_signal(local_tags, provider_payload)
    sparse_release = bool(local_count <= 3 and title_gate >= 0.995 and artist_gate >= 0.94)
    full_album_safe = bool(
        local_count >= 4
        and title_gate >= 0.995
        and artist_gate >= 0.96
        and secondary_ok
        and p in {"discogs", "bandcamp", "itunes", "deezer", "musicbrainz"}
    )
    lastfm_safe = bool(
        p == "lastfm"
        and local_count <= 3
        and title_gate >= 0.995
        and artist_gate >= 0.96
        and str(provider_id or "").strip()
    )
    if sparse_release or full_album_safe or lastfm_safe:
        return {
            "strict_match_verified": True,
            "strict_match_provider": p,
            "strict_reject_reason": "",
            "strict_tracklist_score": 0.90 if (full_album_safe or sparse_release) else 0.88,
            "provider_id": provider_id,
            "strict_artist_name": _provider_payload_artist(p, provider_payload),
            "strict_album_title": _provider_payload_title(p, provider_payload),
            "smart_match_verified": True,
            "smart_match_reason": (
                "smart_no_tracklist_secondary_signal"
                if full_album_safe
                else "smart_sparse_release_identity"
            ),
            "smart_match_evidence": {
                "title_score": round(title_gate, 4),
                "artist_score": round(artist_gate, 4),
                "local_tracks": local_count,
                "provider_tracks": provider_count,
                "secondary_signal": secondary_reason,
            },
        }
    return None


def _strict_provider_match_100(
    *,
    local_artist: str,
    local_title: str,
    local_tracks: List,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    local_context: dict | None = None,
    provider: str,
    provider_payload: dict | None,
    expected_provider_id: str = "",
) -> dict:
    """
    Global strict gate for provider identity:
    artist exact + album exact + provider id present + tracklist available +
    exact track count + exact positional track titles.
    """
    p = _normalize_identity_provider(provider)
    payload = provider_payload if isinstance(provider_payload, dict) else {}
    local_titles = _local_track_titles_for_strict(local_tracks or [])
    out = {
        "strict_match_verified": False,
        "strict_match_provider": "",
        "strict_reject_reason": "strict_reject",
        "strict_tracklist_score": 0.0,
        "provider_id": "",
        "strict_artist_name": "",
        "strict_album_title": "",
    }
    if p not in {"musicbrainz", "discogs", "bandcamp", "lastfm", "itunes", "deezer"}:
        out["strict_reject_reason"] = "provider_missing"
        return out

    candidate_title = (
        payload.get("title")
        or payload.get("album")
        or payload.get("mb_title")
        or ""
    )
    if p == "musicbrainz":
        candidate_artist = payload.get("mb_artist_names") or _extract_mb_artist_names(payload)
    elif p == "lastfm":
        candidate_artist = payload.get("artist") or payload.get("artist_name") or ""
    else:
        candidate_artist = payload.get("artist_name") or payload.get("artist") or ""

    provider_id = _provider_id_for_strict(p, payload)
    out["provider_id"] = provider_id

    id_ok, id_reason = _strict_identity_match_details(
        local_artist=local_artist,
        local_title=local_title,
        candidate_artist=candidate_artist,
        candidate_title=str(candidate_title or ""),
        local_tracks=list(local_tracks or []),
        local_tags=local_tags if isinstance(local_tags, dict) else {},
        local_paths=list(local_paths or []),
        provider=p,
        provider_payload=payload,
        local_context=local_context if isinstance(local_context, dict) else None,
    )
    if not id_ok:
        smart = _strict_smart_provider_match_verdict(
            local_artist=local_artist,
            local_title=local_title,
            local_titles=local_titles,
            local_tags=local_tags if isinstance(local_tags, dict) else {},
            provider=p,
            provider_payload=payload,
            provider_id=provider_id,
            expected_provider_id=expected_provider_id,
            candidate_artist=candidate_artist,
            candidate_title=str(candidate_title or ""),
            id_reason=id_reason,
            tracks_reason="",
            local_context=local_context if isinstance(local_context, dict) else None,
        )
        if smart is not None:
            return smart
        out["strict_reject_reason"] = _strict_reject_code(id_reason)
        return out

    if not provider_id:
        out["strict_reject_reason"] = "provider_id_missing"
        return out

    expected = str(expected_provider_id or "").strip()
    if expected:
        exp_cmp = expected.rstrip("/").lower() if p == "bandcamp" else expected
        got_cmp = provider_id.rstrip("/").lower() if p == "bandcamp" else provider_id
        if exp_cmp != got_cmp:
            out["strict_reject_reason"] = "provider_id_mismatch"
            return out

    provider_titles = _provider_track_titles_for_strict(p, payload)
    tracks_ok, tracks_reason = _strict_tracklist_match_details(local_titles, provider_titles)
    if not tracks_ok:
        smart = _strict_smart_provider_match_verdict(
            local_artist=local_artist,
            local_title=local_title,
            local_titles=local_titles,
            local_tags=local_tags if isinstance(local_tags, dict) else {},
            provider=p,
            provider_payload=payload,
            provider_id=provider_id,
            expected_provider_id=expected_provider_id,
            candidate_artist=candidate_artist,
            candidate_title=str(candidate_title or ""),
            id_reason="",
            tracks_reason=tracks_reason,
            local_context=local_context if isinstance(local_context, dict) else None,
        )
        if smart is not None:
            return smart
        out["strict_reject_reason"] = _strict_reject_code(tracks_reason)
        return out

    out["strict_match_verified"] = True
    out["strict_match_provider"] = p
    out["strict_reject_reason"] = ""
    out["strict_tracklist_score"] = 1.0
    out["strict_artist_name"] = _provider_payload_artist(p, payload)
    out["strict_album_title"] = _provider_payload_title(p, payload)
    return out


def _provider_candidate_id(payload: dict, provider: str) -> str:
    p = (provider or "").strip().lower()
    if not isinstance(payload, dict):
        return ""
    if p == "discogs":
        return str(payload.get("release_id") or payload.get("master_id") or "").strip()
    if p == "lastfm":
        return str(payload.get("mbid") or "").strip()
    if p == "bandcamp":
        return str(payload.get("album_url") or payload.get("url") or "").strip()
    if p == "itunes":
        return str(payload.get("collection_id") or payload.get("collectionId") or "").strip()
    if p == "deezer":
        return str(payload.get("album_id") or payload.get("id") or "").strip()
    return ""


def _build_provider_identity_candidates(
    artist_name: str,
    album_title: str,
    local_track_titles: List[str],
    provider_payloads: dict,
    *,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    local_context: dict | None = None,
) -> list[dict]:
    """Build scored provider candidates in deterministic priority order."""
    out: list[dict] = []
    local_titles = [str(t or "").strip() for t in (local_track_titles or []) if str(t or "").strip()]
    local_track_count = len(local_titles)
    resolved_local_context = local_context if isinstance(local_context, dict) else _classical_identity_context(
        local_artist=artist_name,
        local_title=album_title,
        local_tracks=list(local_titles or []),
        local_tags=local_tags if isinstance(local_tags, dict) else {},
        local_paths=list(local_paths or []),
        provider_payloads=provider_payloads if isinstance(provider_payloads, dict) else None,
    )
    provider_order = ("discogs", "itunes", "deezer", "bandcamp", "lastfm")
    for provider in provider_order:
        payload = provider_payloads.get(provider)
        if not isinstance(payload, dict):
            continue
        src_title = (
            (payload.get("title") if provider != "lastfm" else payload.get("title") or payload.get("album"))
            or ""
        ).strip()
        src_artist = (
            (payload.get("artist_name") if provider != "lastfm" else payload.get("artist") or payload.get("artist_name"))
            or ""
        ).strip()
        if not src_title:
            continue
        strict_ok, strict_reason = _strict_identity_match_details(
            local_artist=artist_name,
            local_title=album_title,
            candidate_artist=src_artist,
            candidate_title=src_title,
            local_tracks=list(local_titles or []),
            local_tags=local_tags if isinstance(local_tags, dict) else {},
            local_paths=list(local_paths or []),
            provider=provider,
            provider_payload=payload,
            local_context=resolved_local_context,
        )
        strict_verdict = _strict_provider_match_100(
            local_artist=artist_name,
            local_title=album_title,
            local_tracks=list(local_titles or []),
            local_tags=local_tags if isinstance(local_tags, dict) else {},
            local_paths=list(local_paths or []),
            local_context=resolved_local_context,
            provider=provider,
            provider_payload=payload,
            expected_provider_id=_provider_candidate_id(payload, provider),
        )
        provider_context = _provider_classical_context(
            provider=provider,
            payload=payload,
            candidate_artist=src_artist,
            candidate_title=src_title,
        )
        classical_guard_applies = bool(
            resolved_local_context.get("is_classical") if isinstance(resolved_local_context, dict) else False
        )
        provider_titles = _provider_track_titles_for_strict(provider, payload)
        provider_track_count = len(provider_titles)
        has_provider_tracklist = provider_track_count > 0
        has_local_tracklist = local_track_count > 0

        if has_provider_tracklist and has_local_tracklist:
            track_score_loose = float(_crosscheck_tracklist(local_titles, provider_titles))
            track_score_exact = float(_crosscheck_tracklist_perfect(local_titles, provider_titles))
            track_score = max(track_score_loose, track_score_exact)
        else:
            track_score = 0.0

        if has_provider_tracklist and has_local_tracklist:
            track_count_ratio = min(local_track_count, provider_track_count) / max(local_track_count, provider_track_count)
        else:
            track_count_ratio = 1.0

        title_score = _provider_identity_album_score(
            album_title,
            src_title,
            artist_hints=[artist_name, src_artist],
        )
        artist_score = _provider_identity_artist_score(artist_name, src_artist)
        ocr_title_score = _cover_ocr_best_match_score(resolved_local_context, src_title, album_mode=True)
        ocr_artist_score = _cover_ocr_best_match_score(resolved_local_context, src_artist, album_mode=False)
        strict_verified = bool(strict_verdict.get("strict_match_verified"))

        if strict_verified:
            confidence = 1.0
            track_score = 1.0
            track_count_ratio = 1.0
            title_score = 1.0
            artist_score = 1.0
        else:
            if has_provider_tracklist and has_local_tracklist:
                confidence = (title_score * 0.45) + (artist_score * 0.35) + (track_score * 0.20)
                if track_count_ratio < 0.55:
                    confidence -= 0.25
                elif track_count_ratio < 0.75:
                    confidence -= 0.10
            elif has_provider_tracklist:
                confidence = (title_score * 0.50) + (artist_score * 0.35) + (track_score * 0.15)
            else:
                confidence = (title_score * 0.56) + (artist_score * 0.44)
            if ocr_title_score >= 0.92:
                confidence += 0.08
            elif ocr_title_score >= 0.82:
                confidence += 0.04
            if ocr_artist_score >= 0.90:
                confidence += 0.05
            elif ocr_artist_score >= 0.80:
                confidence += 0.02
            if (
                title_score < 0.76
                and ocr_title_score >= 0.90
                and max(artist_score, ocr_artist_score) >= 0.70
            ):
                confidence = max(confidence, 0.74)
            if (
                not has_provider_tracklist
                and ocr_title_score < 0.55
                and title_score < 0.75
            ):
                confidence = min(confidence, 0.58)
            if strict_ok:
                confidence += 0.04
            if classical_guard_applies and not strict_ok:
                confidence = min(confidence, 0.45)
            confidence = max(0.0, min(1.0, confidence))

        provider_id = _provider_candidate_id(payload, provider)
        out.append(
            {
                "provider": provider,
                "payload": payload,
                "provider_id": provider_id,
                "title_score": title_score,
                "artist_score": artist_score,
                "ocr_title_score": ocr_title_score,
                "ocr_artist_score": ocr_artist_score,
                "track_score": track_score,
                "confidence": confidence,
                "title": src_title,
                "artist": src_artist,
                "strict_ok": strict_ok,
                "strict_reason": strict_reason,
                "strict_match_verified": strict_verified,
                "strict_reject_reason": str(strict_verdict.get("strict_reject_reason") or strict_reason or ""),
                "strict_tracklist_score": float(strict_verdict.get("strict_tracklist_score") or 0.0),
                "has_provider_tracklist": bool(has_provider_tracklist),
                "has_local_tracklist": bool(has_local_tracklist),
                "provider_track_count": int(provider_track_count),
                "local_track_count": int(local_track_count),
                "track_count_ratio": float(track_count_ratio),
                "classical_guard_applies": bool(classical_guard_applies),
                "classical_guard_ok": bool((not classical_guard_applies) or strict_ok),
                "classical_guard_reason": str(strict_reason or ""),
            }
        )
    return out


def _provider_candidate_soft_identity_ok(
    candidate: dict,
    *,
    min_confidence: float = 0.72,
) -> tuple[bool, str]:
    """
    Conservative soft identity guard:
    - strong artist/title similarity
    - confidence above threshold
    - if both tracklists exist, reject clearly incompatible tracklists
    """
    title_score = float(candidate.get("title_score") or 0.0)
    artist_score = float(candidate.get("artist_score") or 0.0)
    ocr_title_score = float(candidate.get("ocr_title_score") or 0.0)
    ocr_artist_score = float(candidate.get("ocr_artist_score") or 0.0)
    track_score = float(candidate.get("track_score") or 0.0)
    confidence = float(candidate.get("confidence") or 0.0)
    provider = _normalize_identity_provider(str(candidate.get("provider") or ""))
    has_provider_tracklist = bool(candidate.get("has_provider_tracklist"))
    has_local_tracklist = bool(candidate.get("has_local_tracklist"))
    track_count_ratio = float(candidate.get("track_count_ratio") or 0.0)
    local_track_count = int(candidate.get("local_track_count") or 0)
    provider_track_count = int(candidate.get("provider_track_count") or 0)
    payload = candidate.get("payload") if isinstance(candidate.get("payload"), dict) else {}
    strict_reject = _strict_reject_code(
        str(candidate.get("strict_reject_reason") or candidate.get("strict_reason") or "")
    )
    title_gate = max(title_score, ocr_title_score)
    artist_gate = max(artist_score, ocr_artist_score)
    lastfm_album_page = provider == "lastfm" and _lastfm_payload_has_album_page(payload)
    provider_identity_support = bool(str(candidate.get("provider_id") or "").strip()) or lastfm_album_page
    strong_identity = bool(title_gate >= 0.96 and artist_gate >= 0.88)
    very_strong_identity = bool(title_gate >= 0.99 and artist_gate >= 0.92)
    strong_provider_identity = bool(provider_identity_support and title_gate >= 0.94 and artist_gate >= 0.84)
    very_strong_provider_identity = bool(provider_identity_support and title_gate >= 0.97 and artist_gate >= 0.90)
    relaxed_title_variant = bool(
        provider_identity_support
        and artist_gate >= 0.90
        and title_gate >= 0.68
        and confidence >= max(float(min_confidence), 0.78)
    )
    provider_suffix_title_variant = bool(
        strict_reject == "album_mismatch"
        and provider_identity_support
        and artist_gate >= 0.88
        and title_gate >= 0.64
        and confidence >= max(float(min_confidence), 0.74)
    )
    partial_artist_ok = bool(
        strict_reject == "artist_partial_overlap"
        and provider_identity_support
        and title_gate >= 0.94
        and artist_gate >= 0.58
        and confidence >= max(float(min_confidence), 0.76)
    )
    if bool(candidate.get("classical_guard_applies")) and not bool(candidate.get("classical_guard_ok")):
        return (False, str(candidate.get("classical_guard_reason") or "classical_context_insufficient"))

    if title_gate < 0.78 and not (relaxed_title_variant or provider_suffix_title_variant):
        return (False, "album_mismatch")
    if artist_gate < 0.74 and not (
        (provider_identity_support and title_gate >= 0.96 and artist_gate >= 0.70)
        or partial_artist_ok
    ):
        return (False, "artist_mismatch")
    if confidence < float(min_confidence):
        return (False, f"confidence_below_min({confidence:.2f}<{float(min_confidence):.2f})")

    if has_provider_tracklist and has_local_tracklist:
        track_count_delta = abs(local_track_count - provider_track_count)
        if track_count_ratio < 0.55:
            if not (
                (very_strong_identity and provider_identity_support and track_score >= 0.35)
                or (very_strong_provider_identity and track_count_ratio >= 0.48 and track_score >= 0.24)
            ):
                return (False, "track_count_mismatch")
        if track_count_delta >= 4:
            if not (
                (
                    very_strong_identity
                    and provider_identity_support
                    and track_count_ratio >= 0.58
                    and track_score >= 0.42
                )
                or (
                    very_strong_provider_identity
                    and track_count_ratio >= 0.52
                    and track_score >= 0.30
                )
            ):
                return (False, "track_count_mismatch")
        if track_count_delta >= 2 and track_count_ratio < 0.86:
            if not (
                (
                    strong_provider_identity
                    and track_count_ratio >= 0.58
                    and track_score >= 0.40
                )
                or (
                    very_strong_provider_identity
                    and track_count_ratio >= 0.55
                    and track_score >= 0.32
                )
                or (
                    strong_provider_identity
                    and track_count_ratio >= 0.72
                    and track_score >= 0.30
                )
            ):
                return (False, "track_count_mismatch")
        if track_count_delta >= 1 and min(local_track_count, provider_track_count) >= 10 and track_score < 0.78:
            if not (
                (strong_provider_identity and track_score >= 0.50)
                or (very_strong_provider_identity and track_score >= 0.42)
                or (strong_provider_identity and track_count_ratio >= 0.88 and track_score >= 0.44)
            ):
                return (False, "track_count_mismatch")
        # When both sides expose a meaningful tracklist, reject obviously wrong
        # editions even if the counts are similar. Similar counts alone are not
        # enough when titles do not line up.
        if (
            min(local_track_count, provider_track_count) >= 5
            and track_score < 0.55
        ):
            if not (
                (very_strong_identity and provider_identity_support and track_score >= 0.45)
                or (very_strong_provider_identity and track_score >= 0.40)
            ):
                return (False, "track_title_mismatch")
        if track_count_delta >= 1 and min(local_track_count, provider_track_count) >= 8 and track_score < 0.68:
            if not (
                (strong_identity and provider_identity_support and track_score >= 0.55)
                or (very_strong_provider_identity and track_score >= 0.48)
            ):
                return (False, "track_title_mismatch")
        if track_score < 0.40 and track_count_ratio < 0.80:
            return (False, "track_title_mismatch")
    else:
        # No provider tracklist: only allow near-exact identity. This keeps sparse
        # providers from "winning" on title/artist fuzz alone.
        if strict_reject in {
            "artist_mismatch",
            "classical_work_mismatch",
            "classical_composer_mismatch",
            "classical_performance_mismatch",
        }:
            return (False, strict_reject)
        lastfm_page_identity_ok = bool(
            lastfm_album_page
            and title_gate >= 0.90
            and artist_gate >= 0.84
            and confidence >= max(float(min_confidence), 0.78)
        )
        if strict_reject == "album_mismatch" and not lastfm_page_identity_ok:
            return (False, strict_reject)
        if lastfm_page_identity_ok:
            return (True, "lastfm_album_page")
        if local_track_count >= 4:
            return (False, "provider_no_tracklist_full_album")
        if (not has_provider_tracklist) and (
            title_gate < 0.94
            or artist_gate < 0.88
            or confidence < max(float(min_confidence), 0.86)
        ):
            return (False, "provider_no_tracklist")
        if local_track_count >= 5 and (
            title_gate < 0.97
            or artist_gate < 0.92
            or confidence < max(float(min_confidence), 0.90)
        ):
            return (False, "provider_no_tracklist")
        if provider == "lastfm" and local_track_count >= 3 and confidence < max(float(min_confidence), 0.92):
            return (False, "provider_no_tracklist")

    return (True, "soft_identity_ok")


def _provider_candidate_match_classification(
    candidate: dict | None,
    *,
    min_confidence: float = 0.72,
    ai_selected: bool = False,
) -> dict[str, Any]:
    """
    Explain a provider candidate in the same tier language used by
    materialization and MCP analytics. The classification is descriptive only;
    filesystem mutation remains governed by _materialization_confidence_policy().
    """
    c = candidate if isinstance(candidate, dict) else {}
    provider = _normalize_identity_provider(str(c.get("provider") or ""))
    try:
        confidence = float(c.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    soft_ok = False
    soft_reason = ""
    if not bool(c.get("strict_match_verified")) and not ai_selected:
        soft_ok, soft_reason = _provider_candidate_soft_identity_ok(c, min_confidence=min_confidence)
    return _provider_matching_core.candidate_match_classification(
        c,
        provider=provider,
        confidence=max(0.0, min(1.0, confidence)),
        ai_selected=ai_selected,
        soft_identity_ok=soft_ok,
        soft_identity_reason=soft_reason,
    )


def _annotate_provider_identity_candidates(
    candidates: list[dict],
    *,
    min_confidence: float = 0.72,
) -> list[dict]:
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        classification = _provider_candidate_match_classification(candidate, min_confidence=min_confidence)
        candidate["confidence_tier"] = classification.get("tier")
        candidate["confidence_reason"] = classification.get("reason")
        candidate["match_explanation"] = classification
    return candidates


def _provider_identity_ai_skip_reason(
    candidates: list[dict],
    *,
    min_confidence: float,
) -> str:
    """
    Skip AI arbitration when every viable candidate already fails on a
    deterministic mismatch that an LLM cannot repair.
    """
    reasons: list[str] = []
    for candidate in (candidates or [])[:6]:
        ok, reason = _provider_candidate_soft_identity_ok(candidate, min_confidence=min_confidence)
        if ok:
            return ""
        clean = str(reason or "").strip().lower()
        if not clean:
            return ""
        reasons.append(clean)
    if not reasons:
        return ""
    for reason in reasons:
        if not any(reason.startswith(prefix) for prefix in _PROVIDER_IDENTITY_AI_SKIP_REASON_PREFIXES):
            return ""
    return ", ".join(sorted(set(reasons)))


def _provider_candidate_near_perfect_identity(candidate: dict) -> bool:
    if not isinstance(candidate, dict):
        return False
    if not bool(candidate.get("has_provider_tracklist")) or not bool(candidate.get("has_local_tracklist")):
        return False
    return bool(
        float(candidate.get("title_score") or 0.0) >= 0.995
        and float(candidate.get("artist_score") or 0.0) >= 0.995
        and float(candidate.get("track_score") or 0.0) >= 0.96
        and float(candidate.get("track_count_ratio") or 0.0) >= 0.96
        and float(candidate.get("confidence") or 0.0) >= 0.90
    )


def _provider_candidates_support_consensus(top: dict | None, runner: dict | None) -> bool:
    if not isinstance(top, dict) or not isinstance(runner, dict):
        return False
    top_title = str(top.get("title") or "").strip()
    top_artist = str(top.get("artist") or "").strip()
    runner_title = str(runner.get("title") or "").strip()
    runner_artist = str(runner.get("artist") or "").strip()
    if not top_title or not top_artist or not runner_title or not runner_artist:
        return False
    same_title = bool(
        _identity_album_equivalent(top_title, runner_title, artist_hints=[top_artist, runner_artist])
        or _provider_identity_album_score(top_title, runner_title, artist_hints=[top_artist, runner_artist]) >= 0.96
    )
    same_artist = bool(_provider_identity_artist_score(top_artist, runner_artist) >= 0.90)
    if not (same_title and same_artist):
        return False
    if float(top.get("title_score") or 0.0) < 0.94 or float(top.get("artist_score") or 0.0) < 0.84:
        return False
    if float(runner.get("title_score") or 0.0) < 0.94 or float(runner.get("artist_score") or 0.0) < 0.84:
        return False
    top_track_ratio = float(top.get("track_count_ratio") or 0.0)
    runner_track_ratio = float(runner.get("track_count_ratio") or 0.0)
    if bool(top.get("has_provider_tracklist")) and bool(top.get("has_local_tracklist")) and top_track_ratio < 0.55:
        return False
    if bool(runner.get("has_provider_tracklist")) and bool(runner.get("has_local_tracklist")) and runner_track_ratio < 0.55:
        return False
    return True


_STRICT_REJECT_BLOCKS_SOFT_IDENTITY = {
    "artist_mismatch",
    "track_title_mismatch",
    "classical_work_mismatch",
    "classical_composer_mismatch",
    "classical_performance_mismatch",
    "classical_track_count_mismatch",
}


def _edition_soft_identity_survives_strict_reject(edition: dict | None) -> bool:
    e = edition if isinstance(edition, dict) else {}
    strict_reject = _strict_reject_code(
        str(e.get("strict_reject_reason") or e.get("_match_reject_reason") or "")
    )
    if strict_reject in _STRICT_REJECT_BLOCKS_SOFT_IDENTITY:
        return False
    soft_provider = _normalize_identity_provider(
        str(
            e.get("identity_provider")
            or e.get("primary_metadata_source")
            or e.get("metadata_source")
            or ""
        )
    )
    explicit_soft = bool(
        e.get("provider_identity_soft_match")
        or e.get("soft_match_verified")
    )
    provider_soft_id = bool(
        str(e.get("discogs_release_id") or "").strip()
        or str(e.get("lastfm_album_mbid") or "").strip()
        or str(e.get("bandcamp_album_url") or "").strip()
    )
    if strict_reject in {"album_mismatch", "track_count_mismatch"}:
        if explicit_soft and soft_provider in {"discogs", "lastfm", "bandcamp"}:
            return True
        return False
    if soft_provider == "musicbrainz" and not explicit_soft and not provider_soft_id:
        return False
    return bool(explicit_soft or provider_soft_id)


def _ai_choose_provider_identity_candidate(
    artist_name: str,
    album_title: str,
    local_track_titles: List[str],
    candidates: list[dict],
) -> tuple[dict | None, int | None]:
    """
    Ask AI to pick provider identity candidate when heuristics are ambiguous.
    """
    if not candidates:
        return (None, None)
    if not getattr(sys.modules[__name__], "ai_provider_ready", False):
        return (None, None)
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    choices = []
    for i, cand in enumerate(candidates[:12]):
        choices.append(
            (
                f"{letters[i]}) provider={cand.get('provider')} "
                f"title={cand.get('title')!r} artist={cand.get('artist')!r} "
                f"track_score={float(cand.get('track_score') or 0.0):.2f} "
                f"heuristic={float(cand.get('confidence') or 0.0):.2f}"
            )
        )
    tracks_preview = ", ".join(local_track_titles[:30]) if local_track_titles else "(none)"
    if local_track_titles and len(local_track_titles) > 30:
        tracks_preview += ", ..."
    prompt = (
        f"Album identity arbitration.\n"
        f"Local album: artist={artist_name!r}, title={album_title!r}, tracks=[{tracks_preview}].\n\n"
        f"Provider candidates:\n" + "\n".join(choices) + "\n\n"
        "Pick the best matching provider candidate for this same release. "
        "Reply with one letter (A/B/...) or NONE if no candidate is reliable enough. "
        "Optionally append (confidence: N) with N from 0 to 100."
    )
    system_msg = "Reply with a single letter (A, B, C, ...) or NONE. Optionally end with (confidence: N). No explanation."
    try:
        provider = getattr(sys.modules[__name__], "AI_PROVIDER", "openai")
        model = getattr(sys.modules[__name__], "RESOLVED_MODEL", None) or getattr(sys.modules[__name__], "OPENAI_MODEL", "gpt-4o-mini")
        reply = _call_ai_provider_bounded(
            provider=provider,
            model=model,
            system_msg=system_msg,
            user_msg=prompt,
            max_tokens=30,
            analysis_type="provider_identity_verify",
            timeout_sec=min(
                float(AI_SCAN_HARD_TIMEOUT_SEC or 120.0),
                _FILES_SCAN_PROVIDER_IDENTITY_AI_TIMEOUT_SEC if _scan_pipeline_active() else 45.0,
            ),
            log_prefix="[Providers]",
        )
        reply_clean, ai_confidence = parse_ai_confidence((reply or "").strip())
        cleaned = (reply_clean or "").strip().upper()
        if cleaned == "NONE":
            return (None, ai_confidence)
        idx = letters.find(cleaned[:1])
        if 0 <= idx < len(candidates):
            conf_min = max(
                int(getattr(sys.modules[__name__], "AI_CONFIDENCE_MIN", 0) or 0),
                55,
            )
            if ai_confidence is not None and ai_confidence < conf_min:
                logging.info(
                    "[Providers] AI candidate rejected because confidence is too low (%s < %s)",
                    ai_confidence,
                    conf_min,
                )
                return (None, ai_confidence)
            return (candidates[idx], ai_confidence)
    except Exception as e:
        logging.debug("[Providers] AI choice failed: %s", e)
    return (None, None)


def _arbitrate_provider_identity(
    artist_name: str,
    album_title: str,
    local_track_titles: List[str],
    provider_payloads: dict,
    *,
    local_tags: dict | None = None,
    local_paths: list[Any] | None = None,
    local_context: dict | None = None,
    log_negative: bool = True,
    log_skips: bool = True,
) -> dict | None:
    """Provider arbitration: strict first, then conservative soft-match, then AI as last resort."""
    resolved_local_context = local_context if isinstance(local_context, dict) else {}
    if not resolved_local_context:
        try:
            resolved_local_context = _classical_identity_context(
                local_artist=str(artist_name or "").strip(),
                local_title=str(album_title or "").strip(),
                local_tracks=list(local_track_titles or []),
                local_tags=dict(local_tags or {}),
                local_paths=list(local_paths or []),
            )
        except Exception:
            resolved_local_context = {}
    candidates = _build_provider_identity_candidates(
        artist_name,
        album_title,
        local_track_titles,
        provider_payloads,
        local_tags=local_tags,
        local_paths=local_paths,
        local_context=resolved_local_context,
    )
    if not candidates:
        if log_negative:
            logging.info(
                "[Providers] Could not verify %r - %r because no provider candidates survived.",
                artist_name,
                album_title,
            )
        return None
    min_score = max(0.0, min(1.0, float(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_MIN_SCORE", 0.72) or 0.72)))
    score_margin = max(0.0, min(1.0, float(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_SCORE_MARGIN", 0.08) or 0.08)))
    _annotate_provider_identity_candidates(candidates, min_confidence=min_score)
    strict_candidates = [c for c in candidates if bool(c.get("strict_match_verified"))]
    if strict_candidates:
        top = strict_candidates[0]
        explanation = _provider_candidate_match_classification(top, min_confidence=min_score)
        logging.info(
            "[Providers] Matched %r - %r with %s using strict verification.",
            artist_name,
            album_title,
            top.get("provider"),
        )
        return {
            "provider": top.get("provider"),
            "payload": top.get("payload"),
            "provider_id": top.get("provider_id") or "",
            "confidence": 1.0,
            "confidence_source": "strict",
            "title_score": 1.0,
            "artist_score": 1.0,
            "track_score": 1.0,
            "strict_match_verified": True,
            "strict_reject_reason": "",
            "strict_tracklist_score": 1.0,
            "soft_match_verified": True,
            "soft_match_provider": top.get("provider") or "",
            "soft_match_reason": "strict_100",
            "confidence_tier": explanation.get("tier"),
            "confidence_reason": explanation.get("reason"),
            "match_explanation": explanation,
        }

    ranked = sorted(
        candidates,
        key=lambda c: (
            float(c.get("confidence") or 0.0),
            float(c.get("track_score") or 0.0),
            float(c.get("title_score") or 0.0),
            float(c.get("artist_score") or 0.0),
        ),
        reverse=True,
    )
    top = ranked[0]
    top_ok, top_reason = _provider_candidate_soft_identity_ok(top, min_confidence=min_score)
    runner = ranked[1] if len(ranked) > 1 else None
    runner_ok = False
    runner_reason = ""
    if runner is not None:
        runner_ok, runner_reason = _provider_candidate_soft_identity_ok(runner, min_confidence=min_score)
    classical_context = bool((resolved_local_context or {}).get("is_classical"))
    provider_consensus = bool(
        (not classical_context)
        and top_ok
        and runner_ok
        and _provider_candidates_support_consensus(top, runner)
    )
    ambiguous = bool(
        top_ok
        and runner is not None
        and runner_ok
        and abs(float(top.get("confidence") or 0.0) - float(runner.get("confidence") or 0.0)) <= score_margin
    )
    top_near_perfect = _provider_candidate_near_perfect_identity(top)
    if top_ok and (not ambiguous or top_near_perfect or provider_consensus):
        explanation = _provider_candidate_match_classification(top, min_confidence=min_score)
        logging.info(
            "[Providers] Matched %r - %r with %s using %s verification (confidence=%.2f).",
            artist_name,
            album_title,
            top.get("provider"),
            (
                "provider-consensus soft"
                if provider_consensus and ambiguous and not top_near_perfect
                else ("near-perfect soft" if top_near_perfect and ambiguous else "soft-safe")
            ),
            float(top.get("confidence") or 0.0),
        )
        return {
            "provider": top.get("provider"),
            "payload": top.get("payload"),
            "provider_id": top.get("provider_id") or "",
            "confidence": float(top.get("confidence") or 0.0),
            "confidence_source": "heuristic",
            "title_score": float(top.get("title_score") or 0.0),
            "artist_score": float(top.get("artist_score") or 0.0),
            "track_score": float(top.get("track_score") or 0.0),
            "strict_match_verified": False,
            "strict_reject_reason": str(top.get("strict_reject_reason") or ""),
            "strict_tracklist_score": float(top.get("strict_tracklist_score") or 0.0),
            "soft_match_verified": True,
            "soft_match_provider": top.get("provider") or "",
            "soft_match_reason": "soft_identity_ok",
            "confidence_tier": explanation.get("tier"),
            "confidence_reason": explanation.get("reason"),
            "match_explanation": explanation,
        }

    scan_inline_mode = _scan_inline_matching_active()
    scan_lifecycle_active = _ai_scan_lifecycle_phase_active()
    use_ai = bool(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_USE_AI", True)) and not classical_context and not scan_inline_mode and not scan_lifecycle_active
    if classical_context and bool(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_USE_AI", True)) and log_skips:
        logging.info(
            "[Providers] Skipping AI tiebreak for %r - %r because classical context uses deterministic-only arbitration.",
            artist_name,
            album_title,
        )
    elif scan_inline_mode and bool(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_USE_AI", True)) and log_skips:
        logging.info(
            "[Providers] Skipping AI tiebreak for %r - %r during scan inline matching; deterministic provider arbitration only.",
            artist_name,
            album_title,
        )
    elif scan_lifecycle_active and bool(getattr(sys.modules[__name__], "PROVIDER_IDENTITY_USE_AI", True)) and log_skips:
        logging.info(
            "[Providers] Skipping AI tiebreak for %r - %r during scan lifecycle; deterministic provider arbitration only.",
            artist_name,
            album_title,
        )
    if use_ai:
        ai_pool = ranked[:6]
        if ambiguous:
            top_conf = float(top.get("confidence") or 0.0)
            window = max(0.03, score_margin) + 0.05
            close = [c for c in ai_pool if (top_conf - float(c.get("confidence") or 0.0)) <= window]
            if len(close) >= 2:
                ai_pool = close
        ai_skip_reason = _provider_identity_ai_skip_reason(ai_pool, min_confidence=min_score)
        if ai_skip_reason:
            if log_skips:
                logging.info(
                    "[Providers] Skipping AI tiebreak for %r - %r because deterministic checks already disagree: %s",
                    artist_name,
                    album_title,
                    ai_skip_reason,
                )
        else:
            ai_choice, ai_conf = _ai_choose_provider_identity_candidate(
                artist_name=artist_name,
                album_title=album_title,
                local_track_titles=local_track_titles,
                candidates=ai_pool,
            )
            if ai_choice is not None:
                ai_min = max(0.62, min_score - 0.08)
                ai_ok, ai_reason = _provider_candidate_soft_identity_ok(ai_choice, min_confidence=ai_min)
                if ai_ok:
                    explanation = _provider_candidate_match_classification(
                        ai_choice,
                        min_confidence=ai_min,
                        ai_selected=True,
                    )
                    logging.info(
                        "[Providers] Matched %r - %r with %s after AI tiebreak (ai=%s, heuristic=%.2f).",
                        artist_name,
                        album_title,
                        ai_choice.get("provider"),
                        ai_conf if ai_conf is not None else "n/a",
                        float(ai_choice.get("confidence") or 0.0),
                    )
                    return {
                        "provider": ai_choice.get("provider"),
                        "payload": ai_choice.get("payload"),
                        "provider_id": ai_choice.get("provider_id") or "",
                        "confidence": float(ai_choice.get("confidence") or 0.0),
                        "confidence_source": "ai",
                        "title_score": float(ai_choice.get("title_score") or 0.0),
                        "artist_score": float(ai_choice.get("artist_score") or 0.0),
                        "track_score": float(ai_choice.get("track_score") or 0.0),
                        "strict_match_verified": False,
                        "strict_reject_reason": str(ai_choice.get("strict_reject_reason") or ""),
                        "strict_tracklist_score": float(ai_choice.get("strict_tracklist_score") or 0.0),
                        "soft_match_verified": True,
                        "soft_match_provider": ai_choice.get("provider") or "",
                        "soft_match_reason": f"ai_tiebreak:{ai_reason}",
                        "confidence_tier": explanation.get("tier"),
                        "confidence_reason": explanation.get("reason"),
                        "match_explanation": explanation,
                    }

    rejected = "; ".join(
        f"{c.get('provider')} -> strict={c.get('strict_reject_reason') or c.get('strict_reason') or 'strict failed'}"
        for c in ranked[:4]
    )
    soft_rejected = "; ".join(
        f"{c.get('provider')} -> {(_provider_candidate_soft_identity_ok(c, min_confidence=min_score)[1])}"
        for c in ranked[:4]
    )
    if log_negative:
        logging.info(
            "[Providers] Could not verify %r - %r. Strict checks: %s. Soft checks: %s",
            artist_name,
            album_title,
            rejected or "n/a",
            soft_rejected or "n/a",
        )
    return None


def _strict_discogs_payload_from_release_data(rel_data: dict) -> dict:
    title = str(rel_data.get("title") or "").strip()
    year_val = rel_data.get("year")
    year = str(year_val).strip() if year_val else ""
    cover_url = None
    images = rel_data.get("images") or []
    if isinstance(images, list) and images:
        img0 = images[0] if isinstance(images[0], dict) else None
        if img0:
            cover_url = (img0.get("uri") or img0.get("resource_url") or "").strip() or None
    artist_str = ""
    artists = rel_data.get("artists") or []
    if isinstance(artists, list) and artists:
        a0 = artists[0] if isinstance(artists[0], dict) else None
        if a0 and a0.get("name"):
            artist_str = str(a0.get("name") or "").strip()
    tracklist: list[str] = []
    for tr in rel_data.get("tracklist") or []:
        if not isinstance(tr, dict):
            continue
        t_title = (tr.get("title") or "").strip()
        if t_title:
            tracklist.append(t_title)
    labels: list[str] = []
    catalog_numbers: list[str] = []
    for label_info in rel_data.get("labels") or []:
        if not isinstance(label_info, dict):
            continue
        label_name = str(label_info.get("name") or "").strip()
        if label_name:
            labels.append(label_name)
        catno = str(label_info.get("catno") or "").strip()
        if catno:
            catalog_numbers.append(catno)
    release_id = str(rel_data.get("id") or "").strip()
    master_id = str(rel_data.get("master_id") or "").strip()
    if not master_id:
        master_val = rel_data.get("master")
        if isinstance(master_val, dict):
            master_id = str(master_val.get("id") or "").strip()
    return {
        "title": title,
        "year": year,
        "cover_url": cover_url,
        "artist_name": artist_str,
        "tracklist": tracklist,
        "label": labels,
        "catalog_number": catalog_numbers,
        "release_id": release_id,
        "master_id": master_id,
        "images": images if isinstance(images, list) else [],
        "identity_scope": "discogs_release",
    }


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}


def _provider_identity_text_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_identity_text_score(*args, **kwargs)

def _strict_reject_code_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_reject_code(*args, **kwargs)

def _norm_track_title_strict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _norm_track_title_strict(*args, **kwargs)

def _local_track_titles_for_strict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _local_track_titles_for_strict(*args, **kwargs)

def _provider_track_titles_for_strict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_track_titles_for_strict(*args, **kwargs)

def _provider_id_for_strict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_id_for_strict(*args, **kwargs)

def _strict_tracklist_match_details_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_tracklist_match_details(*args, **kwargs)

def _strict_candidate_artist_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_candidate_artist_text(*args, **kwargs)

def _strict_year_from_tags_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_year_from_tags(*args, **kwargs)

def _strict_year_from_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_year_from_payload(*args, **kwargs)

def _strict_flat_text_values_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_flat_text_values(*args, **kwargs)

def _strict_tag_text_tokens_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_tag_text_tokens(*args, **kwargs)

def _strict_payload_text_tokens_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_payload_text_tokens(*args, **kwargs)

def _strict_secondary_identity_signal_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_secondary_identity_signal(*args, **kwargs)

def _strict_tracklist_similarity_details_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_tracklist_similarity_details(*args, **kwargs)

def _strict_smart_provider_match_verdict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_smart_provider_match_verdict(*args, **kwargs)

def _strict_provider_match_100_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_provider_match_100(*args, **kwargs)

def _provider_candidate_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_candidate_id(*args, **kwargs)

def _build_provider_identity_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _build_provider_identity_candidates(*args, **kwargs)

def _provider_candidate_soft_identity_ok_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_candidate_soft_identity_ok(*args, **kwargs)

def _provider_candidate_match_classification_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_candidate_match_classification(*args, **kwargs)

def _annotate_provider_identity_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _annotate_provider_identity_candidates(*args, **kwargs)

def _provider_identity_ai_skip_reason_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_identity_ai_skip_reason(*args, **kwargs)

def _provider_candidate_near_perfect_identity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_candidate_near_perfect_identity(*args, **kwargs)

def _provider_candidates_support_consensus_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _provider_candidates_support_consensus(*args, **kwargs)

def _edition_soft_identity_survives_strict_reject_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _edition_soft_identity_survives_strict_reject(*args, **kwargs)

def _ai_choose_provider_identity_candidate_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ai_choose_provider_identity_candidate(*args, **kwargs)

def _arbitrate_provider_identity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _arbitrate_provider_identity(*args, **kwargs)

def _strict_discogs_payload_from_release_data_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _strict_discogs_payload_from_release_data(*args, **kwargs)
