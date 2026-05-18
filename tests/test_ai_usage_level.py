import unittest
import sqlite3
import tempfile
from pathlib import Path

import pmda


class AIUsageLevelTests(unittest.TestCase):
    def test_auto_usage_level_enables_mb_candidate_choice(self):
        overrides = pmda._ai_usage_level_overrides("auto")
        self.assertTrue(bool(overrides.get("USE_AI_FOR_MB_MATCH")))
        self.assertTrue(bool(overrides.get("USE_WEB_SEARCH_FOR_MB")))

    def test_reload_ai_config_reapplies_usage_level_overrides(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "settings.db"
            con = sqlite3.connect(str(db_path))
            con.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)")
            con.executemany(
                "INSERT INTO settings(key, value) VALUES (?, ?)",
                [
                    ("AI_USAGE_LEVEL", "auto"),
                    ("AI_PROVIDER", "ollama"),
                    ("OLLAMA_URL", "http://127.0.0.1:11434"),
                    ("OLLAMA_MODEL", "qwen3:4b"),
                ],
            )
            con.commit()
            con.close()

            original_settings_db = pmda.SETTINGS_DB_FILE
            original_reinit = pmda._reinit_ai_from_globals
            original_level = pmda.AI_USAGE_LEVEL
            original_mb_match = pmda.USE_AI_FOR_MB_MATCH
            original_mb_verify = pmda.USE_AI_FOR_MB_VERIFY
            original_dedupe = pmda.USE_AI_FOR_DEDUPE
            original_provider_ai = pmda.PROVIDER_IDENTITY_USE_AI
            try:
                pmda.SETTINGS_DB_FILE = db_path
                pmda.AI_USAGE_LEVEL = "limited"
                pmda.USE_AI_FOR_MB_MATCH = False
                pmda.USE_AI_FOR_MB_VERIFY = False
                pmda.USE_AI_FOR_DEDUPE = False
                pmda.PROVIDER_IDENTITY_USE_AI = False
                pmda._reinit_ai_from_globals = lambda: None

                pmda._reload_ai_config_and_reinit()

                self.assertEqual(pmda.AI_USAGE_LEVEL, "auto")
                self.assertTrue(pmda.USE_AI_FOR_MB_MATCH)
                self.assertTrue(pmda.USE_AI_FOR_MB_VERIFY)
                self.assertTrue(pmda.USE_AI_FOR_DEDUPE)
                self.assertTrue(pmda.PROVIDER_IDENTITY_USE_AI)
            finally:
                pmda.SETTINGS_DB_FILE = original_settings_db
                pmda._reinit_ai_from_globals = original_reinit
                pmda.AI_USAGE_LEVEL = original_level
                pmda.USE_AI_FOR_MB_MATCH = original_mb_match
                pmda.USE_AI_FOR_MB_VERIFY = original_mb_verify
                pmda.USE_AI_FOR_DEDUPE = original_dedupe
                pmda.PROVIDER_IDENTITY_USE_AI = original_provider_ai


if __name__ == "__main__":
    unittest.main()
