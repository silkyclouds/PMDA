"""Loose numeric parsing helpers shared by PMDA runtime modules."""

from __future__ import annotations

import re


def parse_int_loose(value, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return default
    if "/" in text:
        text = text.split("/", 1)[0]
    match = re.search(r"-?\d+", text)
    if not match:
        return default
    try:
        return int(match.group(0))
    except Exception:
        return default


def clamp_int(value, default: int = 0, min_value: int | None = None, max_value: int | None = None) -> int:
    out = parse_int_loose(value, default)
    if min_value is not None and out < min_value:
        return min_value
    if max_value is not None and out > max_value:
        return max_value
    return out


def parse_float_loose(value, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return default
    try:
        return float(text)
    except Exception:
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return default
        try:
            return float(match.group(0))
        except Exception:
            return default


def parse_duration_seconds_loose(value, default: float = 0.0) -> float:
    """Parse seconds or clock-style duration strings into seconds."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return default

    try:
        return float(text)
    except Exception:
        pass

    if ":" in text:
        parts = [part.strip() for part in text.split(":")]
        try:
            numbers = [float(part) for part in parts]
        except Exception:
            numbers = []
        if numbers:
            if len(numbers) == 3:
                hours, minutes, seconds = numbers
                return (hours * 3600.0) + (minutes * 60.0) + seconds
            if len(numbers) == 2:
                minutes, seconds = numbers
                return (minutes * 60.0) + seconds
            if len(numbers) == 1:
                return numbers[0]

    return parse_float_loose(text, default)


def parse_disc_track_loose(
    tags: dict | None,
    fallback_disc: int = 1,
    fallback_track: int = 0,
) -> tuple[int, int]:
    """Parse disc/track numbers while tolerating vinyl-style values."""
    tags = tags or {}
    raw_disc = tags.get("disc") or tags.get("discnumber")
    raw_track = tags.get("track") or tags.get("tracknumber")

    disc = parse_int_loose(raw_disc, 0)
    track = parse_int_loose(raw_track, 0)

    track_text = str(raw_track or "").strip().upper()
    if track_text:
        match = re.match(r"^([A-Z])(?:\s*[-_.]?\s*(\d+))?$", track_text)
        if match:
            if disc <= 0:
                disc = (ord(match.group(1)) - ord("A")) + 1
            if track <= 0:
                track = parse_int_loose(match.group(2), 1)

    if disc <= 0:
        disc = fallback_disc
    if track <= 0:
        track = fallback_track

    return disc, track
