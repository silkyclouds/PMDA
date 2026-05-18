import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

import pmda


class AIOverviewTests(unittest.TestCase):
    def test_latest_ai_benchmark_for_domain_parses_normalized_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "dedupe_ai_benchmark_2026-04-03.json"
            report.write_text(
                json.dumps(
                    {
                        "sample_size": 12,
                        "deterministic_accuracy": 0.75,
                        "assisted_accuracy": 0.83,
                        "ai_completed": 5,
                        "ai_skipped": 4,
                        "ai_failed": 3,
                        "avg_latency_sec": 12.4,
                    }
                ),
                encoding="utf-8",
            )
            old_dir = pmda.PMDA_ANALYSIS_REPORTS_DIR
            try:
                pmda.PMDA_ANALYSIS_REPORTS_DIR = str(tmp)
                out = pmda._latest_ai_benchmark_for_domain("dedupe")
            finally:
                pmda.PMDA_ANALYSIS_REPORTS_DIR = old_dir
            self.assertIsNotNone(out)
            self.assertTrue(bool(out.get("available")))
            self.assertEqual(out.get("sample_size"), 12)
            self.assertAlmostEqual(float(out.get("baseline_score") or 0.0), 0.75, places=3)
            self.assertAlmostEqual(float(out.get("assisted_score") or 0.0), 0.83, places=3)

    def test_ai_queue_status_snapshot_contains_all_domains(self):
        snap = pmda._ai_queue_status_snapshot()
        self.assertIn("matching", snap)
        self.assertIn("dedupe", snap)
        self.assertIn("incomplete", snap)
        self.assertIn("review", snap)

    def test_assistant_runtime_status_uses_resolved_ollama_model(self):
        original_provider = pmda.AI_PROVIDER
        original_openai_model = pmda.OPENAI_MODEL
        try:
            pmda.AI_PROVIDER = "ollama"
            pmda.OPENAI_MODEL = "gpt-5-nano"
            with mock.patch.object(pmda, "_resolve_ai_runtime_availability", return_value=(True, "ollama", "none", "")), \
                mock.patch.object(pmda, "_resolve_model_for_runtime", return_value="qwen3:14b") as model_resolver, \
                mock.patch.object(pmda, "_ollama_available_models_cached", return_value={"qwen3:14b"}):
                status = pmda._assistant_runtime_status(user_id=0)
        finally:
            pmda.AI_PROVIDER = original_provider
            pmda.OPENAI_MODEL = original_openai_model

        self.assertTrue(bool(status.get("ai_ready")))
        self.assertEqual(status.get("ai_provider"), "ollama")
        self.assertEqual(status.get("ai_model"), "qwen3:14b")
        model_resolver.assert_called_once()

    def test_assistant_runtime_status_marks_ollama_unready_when_required_model_missing(self):
        original_provider = pmda.AI_PROVIDER
        original_ollama_url = pmda.OLLAMA_URL
        try:
            pmda.AI_PROVIDER = "ollama"
            pmda.OLLAMA_URL = "http://ollama.local:11434"
            with mock.patch.object(pmda, "_resolve_ai_runtime_availability", return_value=(True, "ollama", "none", "")), \
                mock.patch.object(pmda, "_resolve_model_for_runtime", return_value="qwen3:14b"), \
                mock.patch.object(pmda, "_ollama_available_models_cached", return_value={"qwen3:4b"}):
                status = pmda._assistant_runtime_status(user_id=0)
        finally:
            pmda.AI_PROVIDER = original_provider
            pmda.OLLAMA_URL = original_ollama_url

        self.assertFalse(bool(status.get("ai_ready")))
        self.assertIn("qwen3:14b", str(status.get("ai_error") or ""))

    def test_assistant_runtime_status_uses_text_routing_for_passive_status_poll(self):
        with mock.patch.object(pmda, "_resolve_ai_runtime_availability", return_value=(True, "ollama", "none", "")), \
            mock.patch.object(pmda, "_resolve_model_for_runtime", return_value="qwen3:4b") as model_resolver, \
            mock.patch.object(pmda, "_ollama_available_models_cached", return_value={"qwen3:4b", "qwen3:14b"}):
            status = pmda._assistant_runtime_status(user_id=0)

        self.assertEqual(status.get("ai_model"), "qwen3:4b")
        self.assertEqual(model_resolver.call_args.kwargs.get("endpoint_kind"), "text")

    def test_assistant_runtime_status_keeps_longform_routing_when_user_message_exists(self):
        with mock.patch.object(pmda, "_resolve_ai_runtime_availability", return_value=(True, "ollama", "none", "")), \
            mock.patch.object(pmda, "_resolve_model_for_runtime", return_value="qwen3:14b") as model_resolver, \
            mock.patch.object(pmda, "_ollama_available_models_cached", return_value={"qwen3:4b", "qwen3:14b"}):
            status = pmda._assistant_runtime_status(user_id=0, user_msg="Tell me more about this library")

        self.assertEqual(status.get("ai_model"), "qwen3:14b")
        self.assertEqual(model_resolver.call_args.kwargs.get("endpoint_kind"), "longform")

    def test_api_assistant_chat_uses_runtime_resolved_provider_model_and_analysis_type(self):
        class _DummyConn:
            def close(self):
                return None

        conn = _DummyConn()
        user_row = {"id": 1, "role": "user", "content": "salut"}
        assistant_row = {"id": 2, "role": "assistant", "content": "bonjour"}

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(pmda, "_get_library_mode", return_value="files"))
            stack.enter_context(mock.patch.object(pmda, "_auth_bootstrap_required", return_value=False))
            stack.enter_context(mock.patch.object(pmda, "_auth_user_can_use_ai", return_value=True))
            stack.enter_context(mock.patch.object(pmda, "_current_user_or_empty", return_value={}))
            stack.enter_context(mock.patch.object(pmda, "_current_user_id_or_zero", return_value=0))
            stack.enter_context(mock.patch.object(pmda, "_ensure_files_index_ready", return_value=(True, None)))
            stack.enter_context(mock.patch.object(pmda, "_files_pg_connect", return_value=conn))
            stack.enter_context(mock.patch.object(pmda, "_assistant_maybe_gc", return_value=None))
            stack.enter_context(mock.patch.object(pmda, "_assistant_ensure_session", return_value="sess-1"))
            stack.enter_context(mock.patch.object(pmda, "_assistant_fetch_session_messages", return_value=[]))
            stack.enter_context(mock.patch.object(pmda, "_assistant_ingest_library_rag", return_value={}))
            stack.enter_context(mock.patch.object(pmda, "_assistant_find_artist_ids_for_query", return_value=[]))
            stack.enter_context(mock.patch.object(pmda, "_assistant_insert_message", side_effect=[user_row, assistant_row]))
            stack.enter_context(
                mock.patch.object(
                    pmda,
                    "_assistant_runtime_status",
                    side_effect=[
                        {"ai_ready": True, "ai_provider": "ollama", "ai_model": "qwen3:14b", "ai_error": None},
                        {"ai_ready": True, "ai_provider": "ollama", "ai_model": "qwen3:14b", "ai_error": None},
                    ],
                )
            )
            stack.enter_context(mock.patch.object(pmda, "_assistant_should_force_llm_rag", return_value=True))
            stack.enter_context(mock.patch.object(pmda, "_assistant_try_handle_tool_query", return_value={"handled": False}))
            stack.enter_context(mock.patch.object(pmda, "_assistant_try_handle_sql_agent_query", return_value={"handled": False}))
            stack.enter_context(mock.patch.object(pmda, "_assistant_retrieve_chunks", return_value={"chunks": [], "citations": []}))
            stack.enter_context(mock.patch.object(pmda, "_assistant_should_include_web_discovery", return_value=False))
            stack.enter_context(mock.patch.object(pmda, "_assistant_build_prompt", return_value=("system prompt", "user prompt")))
            stack.enter_context(mock.patch.object(pmda, "_assistant_links_from_citations", return_value=[]))
            stack.enter_context(mock.patch.object(pmda, "_assistant_links_from_web_results", return_value=[]))
            ai_call = stack.enter_context(mock.patch.object(pmda, "call_ai_provider_longform", return_value="bonjour"))
            with pmda.app.test_request_context("/api/assistant/chat", method="POST", json={"message": "salut"}):
                response = pmda.api_assistant_chat()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(ai_call.called)
        self.assertEqual(ai_call.call_args.args[0], "ollama")
        self.assertEqual(ai_call.call_args.args[1], "qwen3:14b")
        self.assertEqual(ai_call.call_args.kwargs["analysis_type"], "assistant_chat")

    def test_assistant_sql_agent_uses_runtime_resolved_provider_model(self):
        with mock.patch.object(pmda, "_assistant_should_try_sql_agent", return_value=True), \
            mock.patch.object(pmda, "_assistant_runtime_status", return_value={"ai_ready": True, "ai_provider": "ollama", "ai_model": "qwen3:14b", "ai_error": None}), \
            mock.patch.object(pmda, "_assistant_lang_for_message", return_value="en"), \
            mock.patch.object(pmda, "_assistant_sql_agent_generate_query", return_value={"sql": "SELECT 1 AS artist_id, 'Orbital' AS artist_name", "params": [], "title": "Top artists"}) as plan_mock, \
            mock.patch.object(pmda, "_assistant_validate_readonly_sql", return_value=(True, "")), \
            mock.patch.object(pmda, "_assistant_sql_agent_execute", return_value=(["artist_id", "artist_name"], [(1, "Orbital")])), \
            mock.patch.object(pmda, "_assistant_sql_agent_format_result", return_value="Orbital"), \
            mock.patch.object(pmda, "_assistant_sql_agent_links_from_result", return_value=[]):
            out = pmda._assistant_try_handle_sql_agent_query(object(), user_message="Quels sont mes artistes les plus presents ?", context_artist_id=0, base_url="http://pmda.local")

        self.assertTrue(bool(out.get("handled")))
        self.assertEqual(plan_mock.call_args.kwargs["provider"], "ollama")
        self.assertEqual(plan_mock.call_args.kwargs["model"], "qwen3:14b")


if __name__ == "__main__":
    unittest.main()
