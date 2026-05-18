#!/usr/bin/env python3
"""PMDA local stdio MCP bridge.

This process intentionally opens no listener. MCP clients launch it over stdio;
it forwards allowed tool calls to PMDA's authenticated `/api/mcp/tool` endpoint.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any


PMDA_BASE_URL = os.getenv("PMDA_MCP_BASE_URL", "http://127.0.0.1:5005").rstrip("/")
PMDA_TOKEN = os.getenv("PMDA_MCP_TOKEN", "").strip()


TOOLS: list[dict[str, Any]] = [
    {
        "name": "pmda.status",
        "description": "Read PMDA scan/runtime/MCP status.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.scan.current",
        "description": "Read current scan progress and active workers.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.scan.analytics",
        "description": "Read structured live scan analytics: progress percentages, ETA, match rates, provider hits, cache counters, moves, active artists, trace summary, review stats, and enrichment coverage.",
        "inputSchema": {"type": "object", "properties": {"scan_id": {"type": "integer"}}},
    },
    {
        "name": "pmda.scan.results",
        "description": "Read the full scan result bundle: history, pipeline trace summary/items, moves, duplicates, incompletes, and review proposals.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scan_id": {"type": "integer"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                "q": {"type": "string"},
                "provider": {"type": "string"},
                "outcome": {"type": "string"},
            },
        },
    },
    {
        "name": "pmda.scan.pipeline_trace",
        "description": "Read paginated per-album pipeline results with provider identity, match status, duplicates, incompletes, moves, and AI metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scan_id": {"type": "integer"},
                "page": {"type": "integer", "minimum": 1},
                "page_size": {"type": "integer", "minimum": 1, "maximum": 1000},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                "q": {"type": "string"},
                "provider": {"type": "string"},
                "outcome": {"type": "string"},
            },
        },
    },
    {
        "name": "pmda.scan.moves",
        "description": "Read moved duplicate/incomplete items plus moved count and total size summaries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scan_id": {"type": "integer"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                "reason": {"type": "string"},
                "status": {"type": "string", "enum": ["all", "active", "restored"]},
            },
        },
    },
    {
        "name": "pmda.scan.resume_state",
        "description": "Read interrupted/resumable scan state, artist statuses, resume plan counts, and discovery snapshot counts.",
        "inputSchema": {"type": "object", "properties": {"run_id": {"type": "string"}}},
    },
    {
        "name": "pmda.scan.history",
        "description": "Read recent scan history.",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}}},
    },
    {
        "name": "pmda.logs.tail",
        "description": "Read recent PMDA logs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "lines": {"type": "integer", "minimum": 20, "maximum": 1000},
                "scan_mode": {"type": "boolean"},
            },
        },
    },
    {
        "name": "pmda.providers.stats",
        "description": "Read provider gateway/cache statistics.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.providers.cache",
        "description": "Read provider album lookup cache summaries and recent cached rows by provider/status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "provider": {"type": "string"},
                "status": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500},
            },
        },
    },
    {
        "name": "pmda.cache.stats",
        "description": "Read all PMDA cache statistics: audio cache, MusicBrainz cache, provider cache, state cache tables, and live provider gateway counters.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.runtime.status",
        "description": "Read managed runtime status for MusicBrainz and Ollama.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.jobs.status",
        "description": "Read durable PMDA job status for scan, publication, materialization, library index, media cache, profile backfill, embeddings, and runtime repair.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.pipeline.jobs",
        "description": "Read the current scan pipeline job snapshot, including active phase, measurable progress, heartbeat, blockers, and post-scan jobs.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.storage.current",
        "description": "Read current disk-aware storage power-saver state: active disk, bucket progress, active device limits, and mapping errors.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.storage.plan",
        "description": "Read the disk-aware storage plan by bucket/device, including resume counts and bucket history.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.musicbrainz.health",
        "description": "Read MusicBrainz target and managed mirror health.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.musicbrainz.cache",
        "description": "Read MusicBrainz album lookup and release-group cache summaries plus recent found/not-found rows.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["found", "not_found", "miss"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500},
            },
        },
    },
    {
        "name": "pmda.ollama.health",
        "description": "Read Ollama runtime and configured model status.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.library.stats",
        "description": "Read published library counts, provider source distribution, artwork/profile coverage, and strict export backlog safety stats.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.enrichment.stats",
        "description": "Read artwork/profile enrichment coverage: album covers, artist images, artist bios, album descriptions/reviews, public ratings, provider source counts, and newest enrichment timestamps.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.library.search",
        "description": "Search PMDA's published library.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50},
            },
            "required": ["query"],
        },
    },
    {
        "name": "pmda.duplicates.list",
        "description": "Read duplicate groups awaiting review.",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 200}}},
    },
    {
        "name": "pmda.incompletes.list",
        "description": "Read incomplete albums awaiting review.",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 200}}},
    },
    {
        "name": "pmda.review.stats",
        "description": "Read duplicate/incomplete/review-proposal rollups: review counts, no-move/same-folder blockers, loser sizes, incomplete classifications, and scan trace issue counts.",
        "inputSchema": {"type": "object", "properties": {"scan_id": {"type": "integer"}}},
    },
    {
        "name": "pmda.scan.start",
        "description": "Start a PMDA scan. Requires scan_control scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scan_type": {"type": "string", "enum": ["full", "changed_only", "incomplete_only"]},
                "run_improve_after": {"type": "boolean"},
            },
        },
    },
    {
        "name": "pmda.scan.pause",
        "description": "Pause the current scan. Requires scan_control scope.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.scan.resume",
        "description": "Resume the current or interrupted scan. Requires scan_control scope.",
        "inputSchema": {"type": "object", "properties": {"scan_type": {"type": "string", "enum": ["full", "changed_only"]}}},
    },
    {
        "name": "pmda.scan.stop",
        "description": "Stop the current scan and preserve resume state. Requires scan_control scope.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.index.rebuild",
        "description": "Start a published library index rebuild. Requires runtime_repair scope.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.musicbrainz.repair",
        "description": "Start MusicBrainz search-index repair. Requires runtime_repair scope.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "pmda.ollama.pull",
        "description": "Ask PMDA to pull/check an Ollama model. Requires runtime_repair scope.",
        "inputSchema": {"type": "object", "properties": {"model": {"type": "string"}}},
    },
    {
        "name": "pmda.review.propose",
        "description": "Create a duplicate/incomplete review proposal without moving files. Requires review_propose scope.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": ["duplicate", "incomplete", "batch"]},
                "scan_id": {"type": "integer"},
                "target_key": {"type": "string"},
                "title": {"type": "string"},
                "recommendation": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "evidence": {"type": "object"},
                "proposed_actions": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["kind", "title", "recommendation"],
        },
    },
    {
        "name": "pmda.review.proposals",
        "description": "Read MCP-created duplicate/incomplete review proposals awaiting human validation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scan_id": {"type": "integer"},
                "status": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500},
            },
        },
    },
]


def _mcp_text(payload: Any, *, is_error: bool = False) -> dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            }
        ],
        "isError": bool(is_error),
    }


def _pmda_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if not PMDA_TOKEN:
        return _mcp_text({"error": "PMDA_MCP_TOKEN is required"}, is_error=True)
    body = json.dumps({"tool": name, "args": arguments or {}}).encode("utf-8")
    request = urllib.request.Request(
        f"{PMDA_BASE_URL}/api/mcp/tool",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {PMDA_TOKEN}",
            "Content-Type": "application/json",
            "User-Agent": "pmda-mcp-stdio/1",
            "X-PMDA-MCP-Transport": "stdio",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = json.loads(response.read().decode("utf-8"))
            return _mcp_text(data)
    except urllib.error.HTTPError as exc:
        try:
            data = json.loads(exc.read().decode("utf-8"))
        except Exception:
            data = {"error": str(exc), "status": exc.code}
        return _mcp_text(data, is_error=True)
    except Exception as exc:
        return _mcp_text({"error": str(exc), "base_url": PMDA_BASE_URL}, is_error=True)


def _handle(request: dict[str, Any]) -> dict[str, Any] | None:
    request_id = request.get("id")
    method = str(request.get("method") or "")
    params = request.get("params") if isinstance(request.get("params"), dict) else {}

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "pmda-local", "version": "1.0.0"},
            },
        }
    if method == "notifications/initialized":
        return None
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}
    if method == "tools/call":
        name = str(params.get("name") or "")
        arguments = params.get("arguments") if isinstance(params.get("arguments"), dict) else {}
        return {"jsonrpc": "2.0", "id": request_id, "result": _pmda_tool_call(name, arguments)}
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": f"Unknown method: {method}"},
    }


def main() -> int:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
            response = _handle(request)
        except Exception as exc:
            response = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32603, "message": str(exc)},
            }
        if response is not None:
            sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
            sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
