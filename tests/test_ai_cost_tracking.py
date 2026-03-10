import inspect
import ast
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from queue import Empty

import pmda


class AICostTrackingTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-ai-cost-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
            "AUTH_DISABLE": pmda.AUTH_DISABLE,
            "AI_MAX_CALLS_PER_SCAN": pmda.AI_MAX_CALLS_PER_SCAN,
            "AI_CALL_COOLDOWN_SEC": pmda.AI_CALL_COOLDOWN_SEC,
        }
        pmda._stop_ai_usage_worker()
        try:
            while True:
                pmda._ai_usage_queue.get_nowait()
        except Empty:
            pass

        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.AUTH_DISABLE = True
        pmda.init_state_db()
        pmda.init_settings_db()
        pmda.init_cache_db()
        self.client = pmda.app.test_client()

    def tearDown(self):
        pmda._stop_ai_usage_worker()
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def _create_scan_row(self) -> int:
        con = sqlite3.connect(str(pmda.STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO scan_history(start_time, status, entry_type) VALUES (?, 'completed', 'scan')",
            (time.time(),),
        )
        scan_id = int(cur.lastrowid or 0)
        con.commit()
        con.close()
        return scan_id

    def test_record_ai_usage_persists_tokens_and_cost(self):
        scan_id = self._create_scan_row()
        prev_ctx = pmda._ai_usage_context_push(
            scan_id=scan_id,
            phase="scan",
            source="interactive",
            job_type="scan_full",
            scope="full",
        )
        try:
            pmda.record_ai_usage(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="text",
                analysis_type="mb_match_verify",
                started_at=time.time() - 0.1,
                status="completed",
                response_obj={
                    "id": "resp_test_1",
                    "usage": {
                        "prompt_tokens": 1000,
                        "completion_tokens": 200,
                        "total_tokens": 1200,
                    },
                },
            )
        finally:
            pmda._ai_usage_context_restore(prev_ctx)

        pmda._ai_usage_wait_for_idle(max_wait_sec=2.0)
        time.sleep(0.2)

        resp = self.client.get(f"/api/scans/{scan_id}/ai-costs?include_lifecycle=false&group_by=analysis_type")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}
        totals = payload.get("totals") or {}
        self.assertEqual(int(totals.get("calls") or 0), 1)
        self.assertEqual(int(totals.get("total_tokens") or 0), 1200)
        self.assertAlmostEqual(float(totals.get("cost_usd") or 0.0), 0.00027, places=8)

        rows = payload.get("breakdown") or []
        self.assertTrue(rows)
        self.assertEqual(str(rows[0].get("analysis_type") or ""), "mb_match_verify")

        con = sqlite3.connect(str(pmda.STATE_DB_FILE), timeout=10)
        cur = con.cursor()
        cur.execute(
            "SELECT ai_tokens_total, ai_unpriced_calls FROM scan_history WHERE scan_id = ?",
            (scan_id,),
        )
        row = cur.fetchone()
        con.close()
        self.assertIsNotNone(row)
        self.assertEqual(int(row[0] or 0), 1200)
        self.assertEqual(int(row[1] or 0), 0)

    def test_ai_cost_summary_group_by_album(self):
        scan_id = self._create_scan_row()
        prev_ctx = pmda._ai_usage_context_push(
            scan_id=scan_id,
            phase="scan",
            source="interactive",
            job_type="scan_full",
            scope="full",
        )
        try:
            pmda._ai_usage_set_album_context(album_id=101, album_artist="Artist A", album_title="Album A")
            for _ in range(2):
                pmda.record_ai_usage(
                    provider="openai",
                    model="gpt-4o-mini",
                    endpoint_kind="text",
                    analysis_type="provider_identity",
                    started_at=time.time() - 0.05,
                    status="completed",
                    response_obj={
                        "id": "resp_album_a",
                        "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
                    },
                )
            pmda._ai_usage_set_album_context(album_id=202, album_artist="Artist B", album_title="Album B")
            pmda.record_ai_usage(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="text",
                analysis_type="mb_match_verify",
                started_at=time.time() - 0.05,
                status="completed",
                response_obj={
                    "id": "resp_album_b",
                    "usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60},
                },
            )
            pmda._ai_usage_set_album_context(album_id=None, album_artist="", album_title="")
        finally:
            pmda._ai_usage_context_restore(prev_ctx)

        pmda._ai_usage_wait_for_idle(max_wait_sec=2.0)

        resp = self.client.get(
            f"/api/scans/{scan_id}/ai-costs?include_lifecycle=false&group_by=album&limit=1"
        )
        self.assertEqual(resp.status_code, 200, resp.get_json())
        payload = resp.get_json() or {}
        self.assertEqual(str(payload.get("group_by") or ""), "album")
        self.assertEqual(int(payload.get("limit") or 0), 1)

        rows = payload.get("breakdown") or []
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(int(row.get("album_id") or 0), 101)
        self.assertEqual(str(row.get("album_artist") or ""), "Artist A")
        self.assertEqual(str(row.get("album_title") or ""), "Album A")
        self.assertEqual(int(row.get("calls") or 0), 2)
        self.assertEqual(int(row.get("total_tokens") or 0), 240)

    def test_web_search_tool_call_fee_is_included(self):
        scan_id = self._create_scan_row()
        prev_ctx = pmda._ai_usage_context_push(
            scan_id=scan_id,
            phase="scan",
            source="interactive",
            job_type="scan_full",
            scope="full",
        )
        try:
            pmda.record_ai_usage(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="web_search",
                analysis_type="web_search",
                started_at=time.time() - 0.05,
                status="completed",
                response_obj={
                    "id": "resp_web_search_fee",
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 20,
                        "total_tokens": 120,
                    },
                },
                image_inputs=1,
            )
        finally:
            pmda._ai_usage_context_restore(prev_ctx)

        pmda._ai_usage_wait_for_idle(max_wait_sec=2.0)
        resp = self.client.get(
            f"/api/scans/{scan_id}/ai-costs?include_lifecycle=false&group_by=analysis_type"
        )
        self.assertEqual(resp.status_code, 200, resp.get_json())
        totals = (resp.get_json() or {}).get("totals") or {}
        # 4o-mini web-search call:
        # input: 100 * 0.15 / 1M = 0.000015
        # output: 20 * 0.60 / 1M = 0.000012
        # tool call: 0.025
        expected = 0.025027
        self.assertAlmostEqual(float(totals.get("cost_usd") or 0.0), expected, places=6)

    def test_web_search_preview_non_reasoning_fee_is_25_per_1k(self):
        scan_id = self._create_scan_row()
        prev_ctx = pmda._ai_usage_context_push(
            scan_id=scan_id,
            phase="scan",
            source="interactive",
            job_type="scan_full",
            scope="full",
        )
        try:
            pmda.record_ai_usage(
                provider="openai",
                model="gpt-4o",
                endpoint_kind="web_search",
                analysis_type="web_search",
                started_at=time.time() - 0.05,
                status="completed",
                response_obj={
                    "id": "resp_web_search_fee_4o",
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 20,
                        "total_tokens": 120,
                    },
                },
                image_inputs=1,
            )
        finally:
            pmda._ai_usage_context_restore(prev_ctx)

        pmda._ai_usage_wait_for_idle(max_wait_sec=2.0)
        resp = self.client.get(
            f"/api/scans/{scan_id}/ai-costs?include_lifecycle=false&group_by=analysis_type"
        )
        self.assertEqual(resp.status_code, 200, resp.get_json())
        totals = (resp.get_json() or {}).get("totals") or {}
        # gpt-4o web_search_preview non-reasoning:
        # input: 100 * 2.5 / 1M = 0.00025
        # output: 20 * 10 / 1M = 0.0002
        # tool call: 0.025
        expected = 0.02545
        self.assertAlmostEqual(float(totals.get("cost_usd") or 0.0), expected, places=6)

    def test_ai_guardrail_enforces_cooldown_and_cap(self):
        scan_id = self._create_scan_row()
        pmda.AI_MAX_CALLS_PER_SCAN = 2
        pmda.AI_CALL_COOLDOWN_SEC = 0.2
        pmda._ai_guard_reset_scan(scan_id)
        with pmda.lock:
            pmda.state["scan_id"] = scan_id

        prev_ctx = pmda._ai_usage_context_push(
            scan_id=scan_id,
            phase="scan",
            source="interactive",
            job_type="scan_full",
            scope="full",
        )
        try:
            ok1, reason1, _meta1 = pmda._ai_guardrail_precheck(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="text",
                analysis_type="mb_match_verify",
                requested_tokens=200,
            )
            self.assertTrue(ok1, reason1)
            self.assertEqual(reason1, "")

            ok2, reason2, _meta2 = pmda._ai_guardrail_precheck(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="text",
                analysis_type="mb_match_verify",
                requested_tokens=200,
            )
            self.assertFalse(ok2)
            self.assertIn("cooldown_active", reason2)

            time.sleep(0.22)
            ok3, reason3, _meta3 = pmda._ai_guardrail_precheck(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="text",
                analysis_type="mb_match_verify",
                requested_tokens=200,
            )
            self.assertTrue(ok3, reason3)

            time.sleep(0.22)
            ok4, reason4, _meta4 = pmda._ai_guardrail_precheck(
                provider="openai",
                model="gpt-4o-mini",
                endpoint_kind="text",
                analysis_type="mb_match_verify",
                requested_tokens=200,
            )
            self.assertFalse(ok4)
            self.assertIn("cap_reached", reason4)
        finally:
            pmda._ai_usage_context_restore(prev_ctx)
            with pmda.lock:
                pmda.state["scan_id"] = None

        with pmda.lock:
            used = int(pmda.state.get("scan_ai_guard_calls_used") or 0)
            blocked = int(pmda.state.get("scan_ai_guard_calls_blocked") or 0)
        self.assertEqual(used, 2)
        self.assertEqual(blocked, 2)

    def test_wrappers_require_analysis_type_keyword(self):
        sig_text = inspect.signature(pmda.call_ai_provider)
        sig_vision = inspect.signature(pmda.call_ai_provider_vision)
        sig_longform = inspect.signature(pmda.call_ai_provider_longform)

        self.assertIn("analysis_type", sig_text.parameters)
        self.assertIn("analysis_type", sig_vision.parameters)
        self.assertIn("analysis_type", sig_longform.parameters)

        self.assertEqual(sig_text.parameters["analysis_type"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertEqual(sig_vision.parameters["analysis_type"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertEqual(sig_longform.parameters["analysis_type"].kind, inspect.Parameter.KEYWORD_ONLY)

    def test_all_wrapper_calls_pass_analysis_type_keyword(self):
        source = Path(pmda.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        missing: list[tuple[str, int]] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn_name = None
            if isinstance(node.func, ast.Name):
                fn_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                fn_name = node.func.attr
            if fn_name not in {"call_ai_provider", "call_ai_provider_vision", "call_ai_provider_longform"}:
                continue
            kw = {k.arg for k in node.keywords if k.arg}
            if "analysis_type" not in kw:
                missing.append((fn_name, int(getattr(node, "lineno", 0) or 0)))
        self.assertEqual(missing, [], f"Missing analysis_type keyword in calls: {missing}")


if __name__ == "__main__":
    unittest.main()
