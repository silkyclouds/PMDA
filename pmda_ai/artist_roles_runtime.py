"""Runtime-bound AI artist role classification helper."""
from __future__ import annotations

from typing import Any, Optional
import json
import logging

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {"ai_suggest_artist_roles"}
_ORIGINAL_EXTRACTED_FUNCTIONS: dict[str, Any] = {}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
            if original is not None:
                globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def ai_suggest_artist_roles(
    album_artist_name: str,
    track_credits: list[dict],
    album_title: str | None,
    release_group_id: str | None,
    use_ai: bool,
) -> Optional[dict]:
    """
    Optional AI helper to classify main vs featuring artists per track from MusicBrainz credits.

    Parameters
    ----------
    album_artist_name: canonical album artist name (from MB release-group or Plex).
    track_credits: list of {index, title, credit} where credit is a string from MB artist-credit.
    album_title: album title (for context in the prompt).
    release_group_id: MusicBrainz release-group ID (for logging / diagnostics only).
    use_ai: whether AI is enabled in settings; when False, this returns None immediately.

    Returns
    -------
    A dict like:
      {
        "main_album_artist": "Ochre",
        "featuring_by_track": {
          "3": ["Keef Baker"],
          "5": ["Global Goon"]
        }
      }
    or None when AI is disabled or an error occurs.
    """
    if not use_ai or not track_credits:
        return None

    try:
        # Build a compact but explicit prompt for the provider
        credits_lines = []
        for tc in track_credits[:40]:
            idx = tc.get("index")
            title = tc.get("title") or ""
            credit = tc.get("credit") or ""
            credits_lines.append(f"Track {idx}: {title} — {credit}")
        credits_text = "\n".join(credits_lines)

        system_prompt = (
            "You are a tagging assistant for a music library manager. "
            "Your job is to identify the main album artist versus featuring/guest artists per track. "
            "Never split one album into multiple albums: there is always exactly one main album artist."
        )
        user_prompt = (
            f"Album title: {album_title or ''}\n"
            f"Album artist (from user / Plex): {album_artist_name}\n"
            f"Release group ID (MusicBrainz): {release_group_id or ''}\n\n"
            "Here are the per-track artist credits:\n"
            f"{credits_text}\n\n"
            "Decide:\n"
            "1) The single main album artist (string).\n"
            "2) For each track index, a list of featuring/guest artists only (exclude the main album artist).\n"
            "Respond strictly as JSON with keys 'main_album_artist' (string) and 'featuring_by_track' (object mapping track index to list of strings)."
        )

        reply = _call_ai_provider_bounded(
            provider=AI_PROVIDER,
            model=RESOLVED_MODEL or OPENAI_MODEL,
            system_msg=system_prompt,
            user_msg=user_prompt,
            max_tokens=400,
            analysis_type="other",
            timeout_sec=AI_SCAN_HARD_TIMEOUT_SEC,
            log_prefix="[AI Artist Roles]",
        )
        if not reply:
            return None
        try:
            data = json.loads(reply)
        except Exception:
            # Try to extract JSON substring if the model wrapped it
            start = reply.find("{")
            end = reply.rfind("}")
            if start != -1 and end != -1 and end > start:
                data = json.loads(reply[start : end + 1])
            else:
                raise

        main_album_artist = str(data.get("main_album_artist") or album_artist_name or "").strip()
        featuring_by_track = data.get("featuring_by_track") or {}
        if not isinstance(featuring_by_track, dict):
            featuring_by_track = {}

        # Normalise track indices to strings and values to list[str]
        norm_map: dict[str, list[str]] = {}
        for k, v in featuring_by_track.items():
            key = str(k)
            if isinstance(v, str):
                vals = [v]
            elif isinstance(v, list):
                vals = [str(x) for x in v if str(x).strip()]
            else:
                continue
            vals = [s.strip() for s in vals if s.strip()]
            if vals:
                norm_map[key] = vals

        return {
            "main_album_artist": main_album_artist or album_artist_name,
            "featuring_by_track": norm_map,
        }
    except Exception as e:
        # Log to scan_ai_errors but never fail the scan
        msg = f"ai_suggest_artist_roles failed for RG {release_group_id or '?'}: {e}"
        logging.debug(msg)
        try:
            state.setdefault("scan_ai_errors", []).append({"message": msg, "group": f"artist_roles:{release_group_id or ''}"})
            if len(state["scan_ai_errors"]) > 100:
                state["scan_ai_errors"] = state["scan_ai_errors"][-80:]
        except Exception:
            pass
        return None


def ai_suggest_artist_roles_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return ai_suggest_artist_roles(*args, **kwargs)


_ORIGINAL_EXTRACTED_FUNCTIONS.update({
    "ai_suggest_artist_roles": ai_suggest_artist_roles,
})
