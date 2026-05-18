"""Classical library display and duplicate grouping helpers."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals for classical-library helpers."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        '_classical_display_payload_for_runtime',
        '_files_collect_album_classical_payload_for_runtime',
        '_files_album_is_classical_like_for_browse_for_runtime',
        '_files_classical_composer_values_for_runtime',
        '_files_album_linked_composer_names_map_for_runtime',
        '_files_apply_canonical_composers_to_classical_payload_for_runtime',
        '_files_album_display_artist_name_for_runtime',
        '_files_browse_entity_kind_from_roles_for_runtime',
        '_classical_person_generated_aliases_for_runtime',
        '_artist_identity_primary_lookup_name_for_runtime',
        '_classical_track_title_set_for_edition_for_runtime',
        '_classical_same_recording_pair_details_for_runtime',
        '_classical_cluster_same_recording_for_runtime',
        '_classical_group_is_same_recording_confident_for_runtime',
        '_mark_classical_sibling_incompletes_for_runtime',
        '_classical_gap_anomaly_should_be_ignored_for_runtime',
        '_classical_preferred_title_composers_for_runtime',
        '_classical_has_explicit_signal_for_runtime',
        '_classical_work_tokens_from_texts_for_runtime',
        '_classical_person_alias_signature_for_runtime',
        '_classical_person_names_equivalent_for_runtime',
        '_classical_person_given_tokens_close_for_runtime',
        '_classical_person_name_looks_english_for_runtime',
        '_identity_case_quality_score_for_runtime',
        '_classical_person_display_preference_score_for_runtime',
        '_collapse_classical_person_aliases_for_runtime',
        '_select_classical_person_display_name_for_runtime',
        '_choose_preferred_person_identity_name_for_runtime',
        '_classical_person_signature_key_for_runtime',
        '_artist_role_hints_from_roles_json_for_runtime',
        '_artist_is_person_like_for_runtime',
        '_classical_gap_anomaly_should_be_ignored',
        '_classical_preferred_title_composers',
        '_classical_has_explicit_signal',
        '_classical_work_tokens_from_texts',
        '_classical_person_alias_signature',
        '_classical_person_names_equivalent',
        '_classical_person_given_tokens_close',
        '_classical_person_name_looks_english',
        '_identity_case_quality_score',
        '_classical_person_display_preference_score',
        '_collapse_classical_person_aliases',
        '_select_classical_person_display_name',
        '_choose_preferred_person_identity_name',
        '_classical_person_signature_key',
        '_artist_role_hints_from_roles_json',
        '_artist_is_person_like',
        "_bind_runtime",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _classical_display_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_display_payload`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_display_payload_impl(*args, **kwargs)

def _files_collect_album_classical_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_collect_album_classical_payload`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_collect_album_classical_payload_impl(*args, **kwargs)

def _files_album_is_classical_like_for_browse_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_album_is_classical_like_for_browse`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_album_is_classical_like_for_browse_impl(*args, **kwargs)

def _files_classical_composer_values_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_classical_composer_values`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_classical_composer_values_impl(*args, **kwargs)

def _files_album_linked_composer_names_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_album_linked_composer_names_map`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_album_linked_composer_names_map_impl(*args, **kwargs)

def _files_apply_canonical_composers_to_classical_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_apply_canonical_composers_to_classical_payload`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_apply_canonical_composers_to_classical_payload_impl(*args, **kwargs)

def _files_album_display_artist_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_album_display_artist_name`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_album_display_artist_name_impl(*args, **kwargs)

def _files_browse_entity_kind_from_roles_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_files_browse_entity_kind_from_roles`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _files_browse_entity_kind_from_roles_impl(*args, **kwargs)

def _classical_person_generated_aliases_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_generated_aliases`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_generated_aliases_impl(*args, **kwargs)

def _artist_identity_primary_lookup_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_artist_identity_primary_lookup_name`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _artist_identity_primary_lookup_name_impl(*args, **kwargs)

def _classical_track_title_set_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_track_title_set_for_edition`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_track_title_set_for_edition_impl(*args, **kwargs)

def _classical_same_recording_pair_details_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_same_recording_pair_details`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_same_recording_pair_details_impl(*args, **kwargs)

def _classical_cluster_same_recording_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_cluster_same_recording`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_cluster_same_recording_impl(*args, **kwargs)

def _classical_group_is_same_recording_confident_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_group_is_same_recording_confident`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_group_is_same_recording_confident_impl(*args, **kwargs)

def _mark_classical_sibling_incompletes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_mark_classical_sibling_incompletes`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _mark_classical_sibling_incompletes_impl(*args, **kwargs)

def _classical_gap_anomaly_should_be_ignored_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_gap_anomaly_should_be_ignored`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_gap_anomaly_should_be_ignored(*args, **kwargs)

def _classical_preferred_title_composers_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_preferred_title_composers`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_preferred_title_composers(*args, **kwargs)

def _classical_has_explicit_signal_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_has_explicit_signal`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_has_explicit_signal(*args, **kwargs)

def _classical_work_tokens_from_texts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_work_tokens_from_texts`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_work_tokens_from_texts(*args, **kwargs)

def _classical_person_alias_signature_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_alias_signature`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_alias_signature(*args, **kwargs)

def _classical_person_names_equivalent_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_names_equivalent`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_names_equivalent(*args, **kwargs)

def _classical_person_given_tokens_close_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_given_tokens_close`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_given_tokens_close(*args, **kwargs)

def _classical_person_name_looks_english_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_name_looks_english`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_name_looks_english(*args, **kwargs)

def _identity_case_quality_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_identity_case_quality_score`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _identity_case_quality_score(*args, **kwargs)

def _classical_person_display_preference_score_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_display_preference_score`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_display_preference_score(*args, **kwargs)

def _collapse_classical_person_aliases_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_collapse_classical_person_aliases`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _collapse_classical_person_aliases(*args, **kwargs)

def _select_classical_person_display_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_select_classical_person_display_name`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _select_classical_person_display_name(*args, **kwargs)

def _choose_preferred_person_identity_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_choose_preferred_person_identity_name`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _choose_preferred_person_identity_name(*args, **kwargs)

def _classical_person_signature_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_classical_person_signature_key`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _classical_person_signature_key(*args, **kwargs)

def _artist_role_hints_from_roles_json_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_artist_role_hints_from_roles_json`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _artist_role_hints_from_roles_json(*args, **kwargs)

def _artist_is_person_like_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_artist_is_person_like`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _artist_is_person_like(*args, **kwargs)


def _classical_gap_anomaly_should_be_ignored(
    tags: dict | None,
    *,
    actual_count: int,
    max_idx: int,
    gaps: list[tuple[int, int]] | None,
) -> bool:
    tag_map = tags if isinstance(tags, dict) else {}
    composer_values = _classical_collect_tag_values(tag_map, _CLASSICAL_COMPOSER_TAG_KEYS)
    performance_values = _classical_collect_tag_values(tag_map, _CLASSICAL_PERFORMANCE_TAG_KEYS)
    work_values = _classical_collect_tag_values(tag_map, _CLASSICAL_WORK_TAG_KEYS)
    genre_values = _classical_collect_tag_values(tag_map, ("genre", "style"))
    tag_override = _classical_tag_override(tag_map)
    looks_classical = _classical_has_explicit_signal(
        genre_values=genre_values,
        composer_values=composer_values,
        work_values=work_values,
        performer_values=performance_values,
    )
    if tag_override is False and not (
        _classical_genre_signal(genre_values)
        or bool(_classical_release_catalog_tokens_from_texts(_classical_collect_tag_values(tag_map, _CLASSICAL_RELEASE_CATALOG_TAG_KEYS)))
        or bool(_classical_collect_tag_values(tag_map, ("conductor", "orchestra", "ensemble", "choir", "chorus", "soloist", "soloists")))
    ):
        return False
    if not looks_classical:
        return False
    actual_total = max(0, int(actual_count or 0))
    highest_index = max(0, int(max_idx or 0))
    if actual_total < 12 or highest_index <= 0:
        return False
    coverage = (actual_total / highest_index) if highest_index else 1.0
    if coverage < 0.95:
        return False
    missing_total = 0
    non_leading_gaps: list[tuple[int, int]] = []
    for start_i, end_i in list(gaps or []):
        start = int(start_i or 0)
        end = int(end_i or 0)
        if end <= start + 1:
            continue
        if start <= 0:
            return False
        non_leading_gaps.append((start, end))
        missing_total += max(0, end - start - 1)
    if not non_leading_gaps:
        return False
    if len(non_leading_gaps) > 1:
        return False
    if missing_total > 1:
        return False
    return True


def _classical_preferred_title_composers(
    *,
    title_values: list[str] | tuple[str, ...] | None,
    tag_values: list[str] | tuple[str, ...] | None,
) -> list[str]:
    ordered_title_values = _classical_display_values(list(title_values or []))
    if not ordered_title_values:
        return _classical_display_values(list(tag_values or []))
    tag_candidates = _classical_display_values(list(tag_values or []))
    resolved: list[str] = []
    for title_value in ordered_title_values:
        title_norm = _classical_norm_text(title_value)
        title_token_count = len([tok for tok in re.findall(r"[a-z0-9]+", title_norm) if tok])
        equivalent_tags = [
            candidate
            for candidate in tag_candidates
            if _classical_person_names_equivalent(candidate, title_value)
            or title_norm == str((_classical_person_alias_signature(candidate) or {}).get("surname") or "").strip()
        ]
        if equivalent_tags and title_token_count <= 1:
            preferred = max(
                equivalent_tags,
                key=lambda candidate: (
                    _classical_person_display_preference_score(candidate),
                    len(candidate),
                ),
            )
        else:
            preferred = _select_classical_person_display_name(
                current_name=title_value,
                primary_name=equivalent_tags[0] if equivalent_tags else title_value,
                aliases=[*equivalent_tags, title_value],
            )
        if preferred:
            resolved.append(preferred)
    return _classical_display_values(_collapse_classical_person_aliases(resolved))


def _classical_has_explicit_signal(
    *,
    title: str = "",
    track_titles: list[str] | None = None,
    genre_values: list[str] | tuple[str, ...] | None = None,
    composer_values: list[str] | tuple[str, ...] | None = None,
    work_values: list[str] | tuple[str, ...] | None = None,
    conductor_values: list[str] | tuple[str, ...] | None = None,
    orchestra_values: list[str] | tuple[str, ...] | None = None,
    ensemble_values: list[str] | tuple[str, ...] | None = None,
    soloist_values: list[str] | tuple[str, ...] | None = None,
    performer_values: list[str] | tuple[str, ...] | None = None,
    catalog_values: list[str] | tuple[str, ...] | None = None,
) -> bool:
    work_tokens = _classical_work_tokens_from_texts(([title] if str(title or "").strip() else []) + list(work_values or []) + list(track_titles or [])[:20])
    title_signal = _classical_title_signal(title, work_tokens=work_tokens, track_titles=track_titles)
    genre_signal = _classical_genre_signal(genre_values)
    catalog_signal = bool(_classical_release_catalog_tokens_from_texts(list(catalog_values or [])))
    performance_signal = bool(
        list(conductor_values or [])
        or list(orchestra_values or [])
        or list(ensemble_values or [])
        or list(soloist_values or [])
    )
    composer_signal = bool(list(composer_values or []))
    performer_signal = bool(list(performer_values or []))
    return bool(
        title_signal
        or genre_signal
        or performance_signal
        or (catalog_signal and (title_signal or genre_signal or performance_signal))
        or (composer_signal and (title_signal or genre_signal or performance_signal))
        or (composer_signal and performer_signal and (title_signal or genre_signal))
    )


def _classical_person_alias_signature(name: str) -> dict[str, Any]:
    norm = _classical_norm_text(name)
    if not norm:
        return {}
    parts = [p for p in re.findall(r"[a-z0-9]+", norm) if p]
    if not parts:
        return {}
    cleaned: list[str] = []
    for idx, token in enumerate(parts):
        if idx == 0 and token in _CLASSICAL_PERSON_NAME_HONORIFICS:
            continue
        cleaned.append(token)
    if len(cleaned) < 2:
        return {}
    surname = ""
    surname_idx = len(cleaned) - 1
    for idx in range(len(cleaned) - 1, -1, -1):
        token = cleaned[idx]
        if token not in _CLASSICAL_PERSON_NAME_PARTICLES:
            surname = token
            surname_idx = idx
            break
    if not surname:
        surname = cleaned[-1]
        surname_idx = len(cleaned) - 1
    givens = [tok for tok in cleaned[:surname_idx] if tok and tok not in _CLASSICAL_PERSON_NAME_PARTICLES]
    long_givens = {tok for tok in givens if len(tok) >= 2}
    initials = {tok[0] for tok in givens if tok}
    return {
        "surname": surname,
        "givens": givens,
        "long_givens": long_givens,
        "initials": initials,
        "ordered_initials": [tok[0] for tok in givens if tok],
        "token_count": len(cleaned),
    }


def _classical_person_names_equivalent(left: str, right: str) -> bool:
    ls = _classical_person_alias_signature(left)
    rs = _classical_person_alias_signature(right)
    if not ls or not rs:
        left_norm = _classical_norm_text(left)
        right_norm = _classical_norm_text(right)
        if left_norm and right_norm and left_norm == right_norm:
            return True
        if left_norm and rs and left_norm == str(rs.get("surname") or ""):
            return True
        if right_norm and ls and right_norm == str(ls.get("surname") or ""):
            return True
        return False
    if str(ls.get("surname") or "") != str(rs.get("surname") or ""):
        return False

    def _given_variants(values: set[str]) -> set[str]:
        out: set[str] = set()
        for value in values or set():
            token = str(value or "").strip()
            if not token:
                continue
            variants = _CLASSICAL_PERSON_GIVEN_NAME_VARIANTS.get(token, {token})
            out.update({str(item or "").strip() for item in variants if str(item or "").strip()})
        return out

    left_long = set(ls.get("long_givens") or set())
    right_long = set(rs.get("long_givens") or set())
    left_long_variants = _given_variants(left_long)
    right_long_variants = _given_variants(right_long)
    if left_long and right_long and (left_long <= right_long or right_long <= left_long):
        return True
    if left_long_variants and right_long_variants and (
        left_long <= right_long_variants
        or right_long <= left_long_variants
        or left_long_variants <= right_long_variants
        or right_long_variants <= left_long_variants
    ):
        return True
    left_initials = set(ls.get("initials") or set())
    right_initials = set(rs.get("initials") or set())
    if left_initials and right_initials and (left_initials <= right_initials or right_initials <= left_initials):
        return True
    left_given_seq = [str(tok or "").strip() for tok in (ls.get("givens") or []) if str(tok or "").strip()]
    right_given_seq = [str(tok or "").strip() for tok in (rs.get("givens") or []) if str(tok or "").strip()]
    if left_given_seq and right_given_seq and len(left_given_seq) == len(right_given_seq):
        fuzzy_pairs = [
            _classical_person_given_tokens_close(a, b)
            for a, b in zip(left_given_seq, right_given_seq)
        ]
        if all(fuzzy_pairs):
            return True
    return False


def _classical_person_given_tokens_close(left: str, right: str) -> bool:
    left_txt = str(left or "").strip().lower()
    right_txt = str(right or "").strip().lower()
    if not left_txt or not right_txt:
        return False
    if left_txt == right_txt:
        return True
    if left_txt[:1] != right_txt[:1]:
        return False
    if left_txt in _CLASSICAL_PERSON_GIVEN_NAME_VARIANTS.get(right_txt, {right_txt}):
        return True
    if right_txt in _CLASSICAL_PERSON_GIVEN_NAME_VARIANTS.get(left_txt, {left_txt}):
        return True
    if min(len(left_txt), len(right_txt)) < 5:
        return False
    if len(left_txt) == len(right_txt):
        diffs = [idx for idx, pair in enumerate(zip(left_txt, right_txt)) if pair[0] != pair[1]]
        if len(diffs) == 2:
            first, second = diffs
            if second == first + 1 and left_txt[first] == right_txt[second] and left_txt[second] == right_txt[first]:
                return True
    if abs(len(left_txt) - len(right_txt)) > 1:
        return False
    if len(left_txt) > len(right_txt):
        left_txt, right_txt = right_txt, left_txt
    idx_left = 0
    idx_right = 0
    edits = 0
    while idx_left < len(left_txt) and idx_right < len(right_txt):
        if left_txt[idx_left] == right_txt[idx_right]:
            idx_left += 1
            idx_right += 1
            continue
        edits += 1
        if edits > 1:
            return False
        if len(left_txt) == len(right_txt):
            idx_left += 1
            idx_right += 1
        else:
            idx_right += 1
    if idx_left < len(left_txt) or idx_right < len(right_txt):
        edits += 1
    return edits <= 1


def _classical_person_name_looks_english(value: str) -> bool:
    tokens = {
        tok for tok in re.findall(r"[a-z0-9]+", _classical_norm_text(value))
        if tok
    }
    return bool(tokens.intersection(_CLASSICAL_PERSON_ENGLISH_GIVEN_NAMES))


def _identity_case_quality_score(value: str) -> tuple[int, int, int]:
    txt = " ".join(str(value or "").split()).strip()
    if not txt:
        return (-1, -1, -1)
    alpha_tokens = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'’.-]*", txt)
    title_case = 1 if alpha_tokens and all(
        token[:1].isupper() and any(ch.islower() for ch in token[1:])
        for token in alpha_tokens
    ) else 0
    mixed_case = 1 if any(ch.islower() for ch in txt) and any(ch.isupper() for ch in txt) else 0
    all_lower = 1 if txt == txt.lower() else 0
    return (title_case, mixed_case, 0 if all_lower else 1)


def _classical_person_display_preference_score(
    value: str,
    *,
    preference: str | None = None,
    primary: bool = False,
    current: bool = False,
) -> tuple[int, int, int, int, int, int, int, int]:
    txt = " ".join(str(value or "").split()).strip()
    if not txt:
        return (-1, -1, -1, -1, -1, -1, -1, -1)
    pref = _normalize_classical_name_preference(
        preference if preference is not None else globals().get("CLASSICAL_NAME_PREFERENCE", CLASSICAL_NAME_PREFERENCE)
    )
    quality = _identity_display_quality_score(txt)
    case_quality = _identity_case_quality_score(txt)
    ascii_only = 1 if all(ord(ch) < 128 for ch in txt) else 0
    looks_english = 1 if _classical_person_name_looks_english(txt) else 0
    if pref == "english":
        return (
            ascii_only,
            looks_english,
            case_quality[0],
            case_quality[1],
            case_quality[2],
            1 if current else 0,
            1 if primary and ascii_only else 0,
            quality[0],
            quality[2],
            quality[4],
        )
    return (
        0 if looks_english else 1,
        case_quality[0],
        case_quality[1],
        case_quality[2],
        quality[1],
        quality[0],
        1 if primary else 0,
        quality[2],
        quality[4],
        1 if current else 0,
    )


def _collapse_classical_person_aliases(
    values: list[str] | tuple[str, ...] | None,
    *,
    preference: str | None = None,
) -> list[str]:
    collapsed: list[str] = []
    best_scores: list[tuple[Any, ...]] = []
    for raw in values or []:
        clean = " ".join(str(raw or "").split()).strip()
        if not clean:
            continue
        norm = _norm_artist_key(clean)
        if not norm:
            continue
        score = _classical_person_display_preference_score(clean, preference=preference)
        replaced = False
        for idx, existing in enumerate(collapsed):
            if not _classical_person_names_equivalent(existing, clean):
                continue
            replaced = True
            if score > best_scores[idx]:
                collapsed[idx] = clean
                best_scores[idx] = score
            break
        if not replaced:
            collapsed.append(clean)
            best_scores.append(score)
    out: list[str] = []
    seen: set[str] = set()
    for clean in collapsed:
        norm = _norm_artist_key(clean)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(clean)
    return out


def _select_classical_person_display_name(
    *,
    current_name: str = "",
    primary_name: str = "",
    aliases: list[str] | tuple[str, ...] | None = None,
    preference: str | None = None,
) -> str:
    current_txt = " ".join(str(current_name or "").split()).strip()
    primary_txt = " ".join(str(primary_name or "").split()).strip()
    alias_values = [str(value or "").strip() for value in (aliases or []) if str(value or "").strip()]
    base_name = primary_txt or current_txt
    candidates = _files_merge_artist_alias_values([primary_txt, current_txt], alias_values)
    if not candidates:
        return primary_txt or current_txt
    filtered: list[str] = []
    if base_name:
        for candidate in candidates:
            if _classical_person_names_equivalent(base_name, candidate):
                filtered.append(candidate)
    pool = _collapse_classical_person_aliases(filtered or candidates, preference=preference)
    primary_norm = _norm_artist_key(primary_txt)
    current_norm = _norm_artist_key(current_txt)
    best = ""
    best_score: tuple[int, ...] | None = None
    for candidate in pool:
        candidate_norm = _norm_artist_key(candidate)
        score = _classical_person_display_preference_score(
            candidate,
            preference=preference,
            primary=bool(primary_norm and candidate_norm == primary_norm),
            current=bool(current_norm and candidate_norm == current_norm),
        )
        if best_score is None or score > best_score:
            best = candidate
            best_score = score
    return best or primary_txt or current_txt


def _choose_preferred_person_identity_name(current_value: str, candidate_value: str) -> str:
    current_txt = " ".join(str(current_value or "").split()).strip()
    candidate_txt = " ".join(str(candidate_value or "").split()).strip()
    if not candidate_txt:
        return current_txt
    if not current_txt:
        return candidate_txt
    if not _classical_person_names_equivalent(current_txt, candidate_txt):
        current_sig = _classical_person_alias_signature(current_txt)
        candidate_sig = _classical_person_alias_signature(candidate_txt)
        current_tokens = {tok for tok in re.findall(r"[a-z0-9]+", _classical_norm_text(current_txt)) if tok}
        candidate_tokens = {tok for tok in re.findall(r"[a-z0-9]+", _classical_norm_text(candidate_txt)) if tok}
        compatible = False
        if current_sig and candidate_sig:
            current_surname = str(current_sig.get("surname") or "").strip()
            candidate_surname = str(candidate_sig.get("surname") or "").strip()
            if current_surname and current_surname == candidate_surname:
                compatible = True
        elif current_tokens and candidate_tokens and (
            current_tokens <= candidate_tokens
            or candidate_tokens <= current_tokens
        ):
            compatible = True
        if not compatible:
            return current_txt
    return _select_classical_person_display_name(
        current_name=current_txt,
        primary_name=candidate_txt,
        aliases=[current_txt, candidate_txt],
    )


def _classical_person_signature_key(name: str) -> str:
    sig = _classical_person_alias_signature(name)
    surname = str(sig.get("surname") or "").strip()
    ordered_initials = "".join(str(ch or "").strip()[:1] for ch in (sig.get("ordered_initials") or []) if str(ch or "").strip())
    if surname and ordered_initials:
        return f"{surname}:{ordered_initials}"
    return ""


def _artist_role_hints_from_roles_json(roles_json: Any) -> list[str]:
    raw = roles_json
    if isinstance(raw, str):
        raw = _safe_json_load(raw, fallback=[])
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in raw:
        role = str(value or "").strip().lower()
        if not role or role in seen:
            continue
        seen.add(role)
        out.append(role)
    return out


def _artist_is_person_like(*, entity_kind: str = "", role_hints: list[str] | tuple[str, ...] | None = None) -> bool:
    kind = str(entity_kind or "").strip().lower()
    roles = {str(role or "").strip().lower() for role in (role_hints or []) if str(role or "").strip()}
    if kind in {"person", "composer", "conductor", "performer", "soloist"}:
        return True
    return bool(roles.intersection(_FILES_BROWSE_PERSON_ROLES))


def _classical_work_tokens_from_texts(texts: list[str]) -> set[str]:
    joined = " \n ".join([str(t or "").strip() for t in texts if str(t or "").strip()])
    if not joined:
        return set()
    norm = _classical_norm_text(joined)
    if not norm:
        return set()
    out: set[str] = set()
    for match in _CLASSICAL_CATALOG_RE.findall(joined):
        token = _classical_norm_text(match)
        if token:
            out.add(token)
    for keyword in _CLASSICAL_WORK_KEYWORDS:
        pattern = re.compile(
            rf"\b{re.escape(keyword)}(?:\s+(?:no|nr|n|n°|nº)\s*\.?\s*\d+)?(?:\s+in\s+[a-g](?:\s+(?:major|minor))?)?(?:\s+op\s*\d+)?\b",
            flags=re.IGNORECASE,
        )
        for match in pattern.findall(joined):
            token = _classical_norm_text(match)
            if token:
                out.add(token)
    canonical_patterns = (
        ("symphony", re.compile(r"\b(?:symphony|symphonie|sinfonia|sinfonie)\s*(?:no|nr|n|n°|nº)?\s*\.?\s*(\d+)\b", flags=re.IGNORECASE)),
        ("concerto", re.compile(r"\bconcerto\s*(?:no|nr|n|n°|nº)?\s*\.?\s*(\d+)\b", flags=re.IGNORECASE)),
        ("sonata", re.compile(r"\b(?:sonata|sonate)\s*(?:no|nr|n|n°|nº)?\s*\.?\s*(\d+)\b", flags=re.IGNORECASE)),
        ("quartet", re.compile(r"\bquartet\s*(?:no|nr|n|n°|nº)?\s*\.?\s*(\d+)\b", flags=re.IGNORECASE)),
        ("quintet", re.compile(r"\bquintet\s*(?:no|nr|n|n°|nº)?\s*\.?\s*(\d+)\b", flags=re.IGNORECASE)),
    )
    for canonical_name, pattern in canonical_patterns:
        for match in pattern.findall(joined):
            token = str(match or "").strip()
            if token:
                out.add(f"{canonical_name} {token}")
    explicit_signal = bool(out)
    split_patterns = re.compile(r"\s*(?:/|;|:|\||–|—)\s*")
    if explicit_signal:
        for raw_text in texts or []:
            raw = html.unescape(str(raw_text or "")).strip()
            if not raw:
                continue
            raw = re.sub(r"[\(\[][^)\]]*[\)\]]", " ", raw)
            for part in split_patterns.split(raw):
                norm_part = _classical_norm_text(part)
                if norm_part and len(norm_part) >= 4:
                    out.add(norm_part[:180])
    if not out and explicit_signal:
        title_line = _classical_norm_text(texts[0] if texts else "")
        if title_line:
            out.add(title_line[:180])
    return out


def _classical_display_payload_impl(
    tags: dict | None,
    *,
    fallback_title: str = "",
    fallback_artist: str = "",
) -> dict[str, Any] | None:
    tag_map = tags if isinstance(tags, dict) else {}
    preview_ctx = _classical_identity_context(
        local_artist=str(fallback_artist or "").strip(),
        local_title=str(fallback_title or "").strip(),
        local_tracks=[],
        local_tags=tag_map,
        local_paths=None,
    )
    if not bool(preview_ctx.get("is_classical")):
        return None
    composer = _classical_display_values(_classical_collect_tag_values(tag_map, _CLASSICAL_COMPOSER_TAG_KEYS))
    title_composer_values = _classical_display_values(_classical_title_composer_values(fallback_title))
    if title_composer_values:
        composer = _classical_preferred_title_composers(
            title_values=title_composer_values,
            tag_values=composer,
        )
    work = _classical_display_values(_classical_collect_tag_values(tag_map, _CLASSICAL_WORK_TAG_KEYS), limit=4)
    conductor = _classical_display_values(_classical_collect_tag_values(tag_map, ("conductor",)), limit=4)
    orchestra = _classical_display_values(_classical_collect_tag_values(tag_map, ("orchestra",)), limit=4)
    ensemble = _classical_display_values(_classical_collect_tag_values(tag_map, ("ensemble", "choir", "chorus")), limit=4)
    soloists = _classical_display_values(_classical_collect_tag_values(tag_map, ("soloist", "soloists")), limit=6)
    performers_raw = _classical_display_values(_classical_collect_tag_values(tag_map, ("performer", "performers", "artist", "albumartist", "album_artist")), limit=8)
    catalog_numbers = _classical_display_values(_classical_collect_tag_values(tag_map, _CLASSICAL_RELEASE_CATALOG_TAG_KEYS), limit=6)
    genre_values = _classical_collect_tag_values(tag_map, ("genre", "style"))
    tag_override = _classical_tag_override(tag_map)
    looks_classical = _classical_has_explicit_signal(
        title=fallback_title,
        genre_values=genre_values,
        composer_values=composer,
        work_values=work,
        conductor_values=conductor,
        orchestra_values=orchestra,
        ensemble_values=ensemble,
        soloist_values=soloists,
        performer_values=performers_raw,
        catalog_values=catalog_numbers,
    )
    if tag_override is False and not (
        _classical_title_signal(
            fallback_title,
            work_tokens=_classical_work_tokens_from_texts(
                ([fallback_title] if str(fallback_title or "").strip() else []) + list(work or [])
            ),
        )
        or _classical_genre_signal(genre_values)
        or bool(conductor or orchestra or ensemble or soloists)
        or bool(_classical_release_catalog_tokens_from_texts(catalog_numbers))
    ):
        return None
    if not looks_classical:
        return None
    composer_norms = {_classical_norm_text(v) for v in composer}
    conductor_norms = {_classical_norm_text(v) for v in conductor}
    orchestra_norms = {_classical_norm_text(v) for v in orchestra}
    ensemble_norms = {_classical_norm_text(v) for v in ensemble}
    soloist_norms = {_classical_norm_text(v) for v in soloists}
    filtered_performers = [
        value for value in performers_raw
        if _classical_norm_text(value) not in composer_norms
        and _classical_norm_text(value) not in conductor_norms
        and _classical_norm_text(value) not in orchestra_norms
        and _classical_norm_text(value) not in ensemble_norms
        and _classical_norm_text(value) not in soloist_norms
    ]
    if not work and str(fallback_title or "").strip():
        work = _classical_display_values([fallback_title], limit=2)
    fallback_artist_sig = _classical_person_alias_signature(fallback_artist)
    if (
        not composer
        and str(fallback_artist or "").strip()
        and not conductor
        and not orchestra
        and not ensemble
        and bool(fallback_artist_sig)
        and not any(
            token in _CLASSICAL_NON_COMPOSER_NAME_TOKENS
            for token in re.findall(r"[a-z0-9]+", _classical_norm_text(fallback_artist))
        )
    ):
        composer = _classical_display_values([fallback_artist], limit=2)
    return {
        "is_classical": True,
        "composer": composer,
        "work": work,
        "conductor": conductor,
        "orchestra": orchestra,
        "ensemble": ensemble,
        "soloists": soloists,
        "performers": filtered_performers[:6],
        "catalog_numbers": catalog_numbers,
    }


def _files_collect_album_classical_payload_impl(
    album: dict[str, Any],
    *,
    fallback_artist: str = "",
) -> dict[str, Any] | None:
    merged: dict[str, list[str]] = {
        "composer": [],
        "work": [],
        "conductor": [],
        "orchestra": [],
        "ensemble": [],
        "soloists": [],
        "performers": [],
        "catalog_numbers": [],
    }
    seen_per_key: dict[str, set[str]] = {key: set() for key in merged}
    any_payload = False
    seen_sources: set[str] = set()

    sources: list[tuple[str, str]] = []
    primary_tags_raw = str(album.get("primary_tags_json") or "{}").strip()
    if primary_tags_raw:
        sources.append((primary_tags_raw, str(album.get("title") or "").strip()))
    for track in (album.get("tracks") or []):
        raw = str((track or {}).get("primary_tags_json") or "").strip()
        if raw:
            sources.append((raw, str((track or {}).get("title") or album.get("title") or "").strip()))
    for raw in (album.get("track_primary_tags_jsons") or []):
        raw_txt = str(raw or "").strip()
        if raw_txt:
            sources.append((raw_txt, str(album.get("title") or "").strip()))

    for raw, fallback_title in sources:
        if raw in seen_sources:
            continue
        seen_sources.add(raw)
        tag_map = _safe_json_load(raw, fallback={})
        if not isinstance(tag_map, dict):
            continue
        payload = _classical_display_payload(
            tag_map,
            fallback_title=fallback_title,
            fallback_artist=fallback_artist,
        )
        if not isinstance(payload, dict):
            continue
        any_payload = True
        for key in merged:
            for value in (payload.get(key) or []):
                clean = re.sub(r"\s+", " ", str(value or "").strip(" -–—"))
                if not clean:
                    continue
                norm = _classical_norm_text(clean)
                if not norm or norm in seen_per_key[key]:
                    continue
                seen_per_key[key].add(norm)
                merged[key].append(clean)
    fallback_artist_norm = _classical_norm_text(fallback_artist)
    if fallback_artist_norm and len(merged["composer"]) > 1:
        merged["composer"] = [
            value
            for value in merged["composer"]
            if _classical_norm_text(value) != fallback_artist_norm
        ]
    if not any_payload:
        return None
    out: dict[str, Any] = dict(merged)
    out["is_classical"] = True
    return out


def _files_album_is_classical_like_for_browse_impl(
    album: dict[str, Any],
    *,
    fallback_artist: str = "",
) -> bool:
    title_value = str(album.get("title") or "").strip()
    tags_payloads: list[tuple[dict[str, Any], str]] = []

    primary_tags_raw = str(album.get("primary_tags_json") or "{}").strip()
    if primary_tags_raw:
        tag_map = _safe_json_load(primary_tags_raw, fallback={})
        if isinstance(tag_map, dict):
            tags_payloads.append((tag_map, title_value))

    for raw in (album.get("track_primary_tags_jsons") or [])[:6]:
        raw_txt = str(raw or "").strip()
        if not raw_txt:
            continue
        tag_map = _safe_json_load(raw_txt, fallback={})
        if isinstance(tag_map, dict):
            tags_payloads.append((tag_map, title_value))

    track_entries = list(album.get("tracks") or [])[:20]
    if not tags_payloads:
        tags_payloads.append(({}, title_value))

    for tag_map, fallback_title in tags_payloads:
        try:
            ctx = _classical_identity_context(
                local_artist=str(fallback_artist or "").strip(),
                local_title=str(fallback_title or "").strip(),
                local_tracks=track_entries,
                local_tags=tag_map,
                local_paths=None,
            )
        except Exception:
            continue
        if bool((ctx or {}).get("is_classical")):
            return True
    return False


def _files_classical_composer_values_impl(classical_payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(classical_payload, dict):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in list(classical_payload.get("composer") or []):
        clean = re.sub(r"\s+", " ", str(value or "").strip()).strip(" -–—")
        norm = _classical_norm_text(clean)
        if not clean or not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(clean)
    return out


def _files_album_linked_composer_names_map_impl(conn, album_ids: list[int] | tuple[int, ...] | None) -> dict[int, list[str]]:
    ids = sorted({int(aid) for aid in (album_ids or []) if int(aid or 0) > 0})
    if conn is None or not ids:
        return {}
    out: dict[int, list[str]] = {}
    seen_by_album: dict[int, set[str]] = defaultdict(set)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                link.album_id,
                COALESCE(ar.name, '') AS current_name,
                COALESCE(ar.canonical_name, '') AS canonical_name,
                COALESCE(ar.entity_kind, 'artist') AS entity_kind,
                COALESCE(ar.roles_json, '[]') AS roles_json,
                COALESCE(ar.aliases_json, '[]') AS aliases_json
            FROM files_artist_album_links link
            JOIN files_artists ar ON ar.id = link.artist_id
            WHERE link.album_id = ANY(%s)
              AND COALESCE(ar.entity_kind, 'artist') = 'composer'
            ORDER BY link.album_id ASC,
                     CASE WHEN link.is_primary THEN 0 ELSE 1 END ASC,
                     ar.id ASC
            """,
            (ids,),
        )
        rows = cur.fetchall()
    for album_id, current_name, canonical_name, entity_kind, roles_json, aliases_json in rows:
        aid = int(album_id or 0)
        display_name = _library_artist_display_name(
            current_name=str(current_name or "").strip(),
            canonical_name=str(canonical_name or "").strip(),
            entity_kind=str(entity_kind or "").strip(),
            roles_json=roles_json,
            aliases_json=aliases_json,
        )
        display_name = re.sub(r"\s+", " ", str(display_name or "").strip()).strip(" -–—")
        display_norm = _classical_norm_text(display_name)
        if aid <= 0 or not display_name or not display_norm or display_norm in seen_by_album[aid]:
            continue
        seen_by_album[aid].add(display_norm)
        out.setdefault(aid, []).append(display_name)
    return out


def _files_apply_canonical_composers_to_classical_payload_impl(
    classical_payload: dict[str, Any] | None,
    composer_names: list[str] | tuple[str, ...] | None,
) -> dict[str, Any] | None:
    payload = dict(classical_payload or {}) if isinstance(classical_payload, dict) else None
    canonical = _classical_display_values(
        [str(value or "").strip() for value in (composer_names or []) if str(value or "").strip()],
        limit=6,
    )
    if canonical:
        if not isinstance(payload, dict):
            payload = {"is_classical": True}
        payload["composer"] = canonical
    return payload


def _files_album_display_artist_name_impl(
    *,
    artist_name: str = "",
    classical_payload: dict[str, Any] | None = None,
    limit: int = 2,
) -> str:
    composers = _files_classical_composer_values(classical_payload)
    clean_artist = re.sub(r"\s+", " ", str(artist_name or "").strip()).strip(" -–—")
    safe_limit = max(1, int(limit or 2))
    if not composers:
        return clean_artist
    visible = composers[:safe_limit]
    if len(composers) > safe_limit:
        return f"{', '.join(visible)} +{len(composers) - safe_limit} more"
    return ", ".join(visible)


def _files_browse_entity_kind_from_roles_impl(roles: set[str]) -> str:
    ordered = {str(r or "").strip().lower() for r in (roles or set()) if str(r or "").strip()}
    if not ordered:
        return "artist"
    if ordered <= {"artist", "featured", "appearance"}:
        return "artist"
    if ordered <= _FILES_BROWSE_ENSEMBLE_ROLES:
        return "ensemble"
    if ordered <= {"composer"}:
        return "composer"
    if ordered <= {"conductor"}:
        return "conductor"
    if ordered <= {"soloist", "performer"}:
        return "performer"
    if ordered == {"artist"}:
        return "artist"
    return "mixed"


def _classical_person_generated_aliases_impl(name: str) -> list[str]:
    clean = " ".join(str(name or "").split()).strip(" -–—")
    if not clean:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        txt = " ".join(str(value or "").split()).strip(" -–—")
        norm = _classical_norm_text(txt)
        if not txt or not norm or norm in seen:
            return
        seen.add(norm)
        out.append(txt)

    if "," in clean:
        _push(_classical_sort_name_to_display(clean))
    sig = _classical_person_alias_signature(clean)
    surname = str(sig.get("surname") or "").strip()
    givens = [str(tok or "").strip() for tok in (sig.get("givens") or []) if str(tok or "").strip()]
    ordered_initials = [str(ch or "").strip().upper()[:1] for ch in (sig.get("ordered_initials") or []) if str(ch or "").strip()]
    if surname and ordered_initials:
        joined = " ".join(ordered_initials)
        compact = "".join(ordered_initials)
        dotted = ". ".join(ordered_initials) + "."
        _push(f"{joined} {surname}".strip())
        _push(f"{compact} {surname}".strip())
        _push(f"{dotted} {surname}".strip())
        _push(f"{'.'.join(ordered_initials)}. {surname}".strip())
    if surname and givens:
        variant_choices: list[list[str]] = []
        for token in givens:
            variant_choices.append(sorted(_CLASSICAL_PERSON_GIVEN_NAME_VARIANTS.get(token, {token}))[:4])
        generated_sequences: list[list[str]] = []
        seen_sequences: set[tuple[str, ...]] = set()
        max_variants = 12
        queue: deque[tuple[int, list[str]]] = deque([(0, [])])
        while queue and len(generated_sequences) < max_variants:
            idx, prefix = queue.popleft()
            if idx >= len(variant_choices):
                key = tuple(prefix)
                if key not in seen_sequences:
                    seen_sequences.add(key)
                    generated_sequences.append(prefix)
                continue
            for variant in variant_choices[idx]:
                queue.append((idx + 1, [*prefix, variant]))
                if len(queue) > (max_variants * 4):
                    break
        original_key = tuple(givens)
        for seq in generated_sequences:
            key = tuple(seq)
            if not seq or key == original_key:
                continue
            _push(" ".join(seq + [surname]).strip())
            if "," in clean:
                _push(f"{surname}, {' '.join(seq)}".strip())
        for explicit in sorted(_CLASSICAL_PERSON_FULLNAME_VARIANTS.get((surname, tuple(givens)), set())):
            _push(explicit)
    return out


def _artist_identity_primary_lookup_name_impl(
    artist_name: str,
    *,
    entity_kind: str = "",
    role_hints: list[str] | tuple[str, ...] | None = None,
    candidate_names: list[str] | tuple[str, ...] | None = None,
) -> str:
    primary = " ".join(str(artist_name or "").split()).strip()
    if not primary:
        return ""
    explicit_candidates: list[str] = []
    seen: set[str] = set()
    for raw in candidate_names or []:
        clean = " ".join(str(raw or "").split()).strip()
        norm = _norm_artist_key(clean)
        if not clean or not norm or norm in seen:
            continue
        if not _artist_image_alias_candidate_is_compatible(
            primary,
            clean,
            entity_kind=entity_kind,
            role_hints=role_hints,
        ):
            continue
        seen.add(norm)
        explicit_candidates.append(clean)
    if not explicit_candidates:
        return primary
    if _artist_is_person_like(entity_kind=entity_kind, role_hints=role_hints) or str(entity_kind or "").strip().lower() in {"composer", "conductor"}:
        ranked = sorted(
            list(enumerate(explicit_candidates)),
            key=lambda item: (
                1 if "," in str(item[1] or "") else 0,
                -_classical_person_alias_signature(str(item[1] or "")).get("token_count", 0),
                -_identity_display_quality_score(str(item[1] or ""))[0],
                item[0],
            ),
        )
        best = " ".join(str((ranked[0][1] if ranked else primary) or "").split()).strip()
        return best or primary
    ranked = sorted(
        explicit_candidates,
        key=lambda value: (
            1 if "," in str(value or "") else 0,
            -_identity_display_quality_score(str(value or ""))[0],
            -len(str(value or "")),
        ),
    )
    best = " ".join(str(ranked[0] or "").split()).strip()
    return best or primary


def _classical_track_title_set_for_edition_impl(edition: dict | None) -> set[str]:
    e = edition if isinstance(edition, dict) else {}
    cached = e.get("_dupe_track_title_set")
    if isinstance(cached, set):
        return cached
    ts = _dupe_track_title_set(list(e.get("tracks") or []))
    e["_dupe_track_title_set"] = ts
    return ts


def _classical_same_recording_pair_details_impl(a: dict | None, b: dict | None) -> tuple[bool, str]:
    ctx_a = _classical_context_for_edition(a)
    ctx_b = _classical_context_for_edition(b)
    if not bool(ctx_a.get("is_classical") or ctx_b.get("is_classical")):
        return (False, "not_classical")

    composer_a = set(ctx_a.get("composer_tokens") or set())
    composer_b = set(ctx_b.get("composer_tokens") or set())
    if composer_a and composer_b and not (composer_a & composer_b):
        return (False, "composer_mismatch")

    work_a = set(ctx_a.get("work_tokens") or set())
    work_b = set(ctx_b.get("work_tokens") or set())
    title_a = str(ctx_a.get("title_norm") or "")
    title_b = str(ctx_b.get("title_norm") or "")
    if work_a and work_b and not (work_a & work_b) and title_a != title_b:
        return (False, "work_mismatch")

    perf_a = set(ctx_a.get("performance_tokens") or set())
    perf_b = set(ctx_b.get("performance_tokens") or set())
    if perf_a and perf_b and not (perf_a & perf_b):
        return (False, "performance_mismatch")

    disc_a = int(ctx_a.get("disc_count") or 0)
    disc_b = int(ctx_b.get("disc_count") or 0)
    if disc_a > 0 and disc_b > 0 and disc_a != disc_b:
        return (False, "disc_count_mismatch")

    labels_a = set(ctx_a.get("label_tokens") or set())
    labels_b = set(ctx_b.get("label_tokens") or set())
    catalogs_a = set(ctx_a.get("catalog_tokens") or set())
    catalogs_b = set(ctx_b.get("catalog_tokens") or set())
    years_a = _mb_extract_year(ctx_a.get("year"))
    years_b = _mb_extract_year(ctx_b.get("year"))

    tracks_a = _classical_track_title_set_for_edition(a)
    tracks_b = _classical_track_title_set_for_edition(b)
    jac = _dupe_jaccard(tracks_a, tracks_b)
    contain = _dupe_track_title_containment(tracks_a, tracks_b)
    count_a = max(0, int(ctx_a.get("track_count") or 0))
    count_b = max(0, int(ctx_b.get("track_count") or 0))
    ratio = (float(min(count_a, count_b)) / float(max(count_a, count_b))) if count_a > 0 and count_b > 0 else 0.0

    total_a = int(ctx_a.get("total_duration_ms") or 0)
    total_b = int(ctx_b.get("total_duration_ms") or 0)
    dur_ratio = 0.0
    if total_a > 0 and total_b > 0:
        hi = max(total_a, total_b)
        dur_ratio = (float(min(total_a, total_b)) / float(hi)) if hi > 0 else 0.0

    label_ok = (not labels_a) or (not labels_b) or bool(labels_a & labels_b)
    catalog_ok = (not catalogs_a) or (not catalogs_b) or bool(catalogs_a & catalogs_b)
    year_ok = True
    if years_a and years_b:
        try:
            year_ok = abs(int(years_a) - int(years_b)) <= 3
        except Exception:
            year_ok = True

    if count_a > 0 and count_b > 0 and count_a == count_b:
        if contain >= 0.98 and (dur_ratio == 0.0 or dur_ratio >= 0.94) and catalog_ok:
            return (True, "same_recording_exact_structure")
        if (not perf_a or not perf_b) and contain >= 0.98 and label_ok and catalog_ok and year_ok and (dur_ratio == 0.0 or dur_ratio >= 0.92):
            return (True, "same_recording_missing_performance_context")

    if count_a > 0 and count_b > 0 and abs(count_a - count_b) <= 2:
        if contain >= 0.98 and label_ok and catalog_ok and year_ok:
            # Tail-truncated siblings should stay in the same recording family so they can be
            # marked incomplete later, but different interpretations remain blocked above.
            return (True, "same_recording_subset_structure")

    if (not perf_a or not perf_b) and jac >= 0.82 and ratio >= 0.75 and label_ok and catalog_ok and year_ok:
        if dur_ratio == 0.0 or dur_ratio >= 0.90:
            return (True, "same_recording_similarity")

    return (False, "recording_context_insufficient")


def _classical_cluster_same_recording_impl(editions: list[dict]) -> list[list[dict]]:
    if not editions:
        return []

    coarse_buckets: dict[tuple, list[dict]] = {}
    for e in editions:
        ctx = _classical_context_for_edition(e)
        composer_sig = tuple(sorted(ctx.get("composer_tokens") or set()))[:4]
        work_sig = tuple(sorted(ctx.get("work_tokens") or set()))[:12]
        key = (
            composer_sig or tuple(str(x) for x in (ctx.get("artist_norms") or [])[:2]) or ("__no_composer__",),
            work_sig or (str(ctx.get("title_norm") or "__no_work__"),),
        )
        coarse_buckets.setdefault(key, []).append(e)

    clusters: list[list[dict]] = []
    for bucket in coarse_buckets.values():
        if len(bucket) <= 1:
            clusters.append(bucket)
            continue
        n = len(bucket)
        parent = list(range(n))
        rank = [0] * n

        def find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def union(i: int, j: int) -> None:
            ri = find(i)
            rj = find(j)
            if ri == rj:
                return
            if rank[ri] < rank[rj]:
                parent[ri] = rj
            elif rank[ri] > rank[rj]:
                parent[rj] = ri
            else:
                parent[rj] = ri
                rank[ri] += 1

        for i in range(n):
            for j in range(i + 1, n):
                ok, _reason = _classical_same_recording_pair_details(bucket[i], bucket[j])
                if ok:
                    union(i, j)
        grouped: dict[int, list[dict]] = defaultdict(list)
        for i, edition in enumerate(bucket):
            grouped[find(i)].append(edition)
        clusters.extend([items for items in grouped.values() if items])
    return clusters


def _classical_group_is_same_recording_confident_impl(editions: list[dict]) -> bool:
    ed_list = [e for e in (editions or []) if isinstance(e, dict)]
    if len(ed_list) < 2:
        return False
    allowed_reasons = {
        "same_recording_exact_structure",
        "same_recording_missing_performance_context",
        "same_recording_subset_structure",
        "same_recording_similarity",
    }
    for i in range(len(ed_list)):
        for j in range(i + 1, len(ed_list)):
            ok, reason = _classical_same_recording_pair_details(ed_list[i], ed_list[j])
            if not ok or reason not in allowed_reasons:
                return False
    return True


def _mark_classical_sibling_incompletes_impl(editions: list[dict], artist_name: str = "") -> None:
    classical_editions = [e for e in (editions or []) if bool(_classical_context_for_edition(e).get("is_classical"))]
    if len(classical_editions) < 2:
        return

    for cluster in _classical_cluster_same_recording(classical_editions):
        if len(cluster) < 2:
            continue
        max_count = max((_edition_exact_expected_track_count(e) or int(_classical_context_for_edition(e).get("track_count") or 0)) for e in cluster)
        if max_count <= 0:
            max_count = max(int(_classical_context_for_edition(e).get("track_count") or 0) for e in cluster)
        if max_count <= 0:
            continue
        leaders = [e for e in cluster if int(_classical_context_for_edition(e).get("track_count") or 0) == max_count]
        if not leaders:
            continue
        leader_title_norms = {str(_classical_context_for_edition(e).get("title_norm") or "") for e in leaders}
        for e in cluster:
            if bool(e.get("is_broken")):
                continue
            ctx = _classical_context_for_edition(e)
            actual_count = int(ctx.get("track_count") or 0)
            if actual_count <= 0 or actual_count >= max_count or (max_count - actual_count) > 2:
                continue
            e_tracks = _classical_track_title_set_for_edition(e)
            if not e_tracks:
                continue
            sibling_match = False
            for leader in leaders:
                leader_ctx = _classical_context_for_edition(leader)
                same_recording, same_recording_reason = _classical_same_recording_pair_details(e, leader)
                if not same_recording:
                    continue
                title_norm_match = str(leader_ctx.get("title_norm") or "") == str(ctx.get("title_norm") or "")
                work_overlap = bool(
                    set(leader_ctx.get("work_tokens") or set())
                    & set(ctx.get("work_tokens") or set())
                )
                if leader_title_norms and not title_norm_match and not work_overlap:
                    continue
                contain = _dupe_track_title_containment(e_tracks, _classical_track_title_set_for_edition(leader))
                if contain >= 0.98 and same_recording_reason in {
                    "same_recording_exact_structure",
                    "same_recording_missing_performance_context",
                    "same_recording_subset_structure",
                    "same_recording_similarity",
                }:
                    sibling_match = True
                    break
            if not sibling_match:
                continue
            e["is_broken"] = True
            e["expected_track_count"] = max_count
            e["actual_track_count"] = actual_count
            e["missing_indices"] = _edition_missing_indices_exact(e, max_count, actual_count)
            e["_classical_sibling_incomplete"] = True
            logging.warning(
                "[Artist %s] Album %s (%s) forced incomplete from classical sibling cluster: actual=%s expected=%s missing=%s",
                artist_name or str(e.get("artist_name") or ""),
                e.get("album_id"),
                e.get("title_raw") or e.get("plex_title") or "",
                actual_count,
                max_count,
                list(e.get("missing_indices") or [])[:24],
            )
