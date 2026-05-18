import tempfile
import unittest
from pathlib import Path

import pmda


class McpAdminBlueprintTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pmda-mcp-admin-blueprint-")
        tmp_path = Path(self._tmp.name)
        self._orig = {
            "CONFIG_DIR": pmda.CONFIG_DIR,
            "STATE_DB_FILE": pmda.STATE_DB_FILE,
            "SETTINGS_DB_FILE": pmda.SETTINGS_DB_FILE,
            "CACHE_DB_FILE": pmda.CACHE_DB_FILE,
        }
        pmda.CONFIG_DIR = tmp_path
        pmda.STATE_DB_FILE = tmp_path / "state.db"
        pmda.SETTINGS_DB_FILE = tmp_path / "settings.db"
        pmda.CACHE_DB_FILE = tmp_path / "cache.db"
        pmda.init_settings_db()
        self.client = pmda.app.test_client()

    def tearDown(self):
        for key, value in self._orig.items():
            setattr(pmda, key, value)
        self._tmp.cleanup()

    def test_mcp_admin_routes_are_registered_from_blueprint(self):
        endpoints = set(pmda.app.view_functions)
        self.assertIn("pmda_mcp_admin.api_admin_mcp_status", endpoints)
        self.assertIn("pmda_mcp_admin.api_admin_mcp_audit", endpoints)
        self.assertIn("pmda_mcp_admin.api_admin_mcp_token_rotate", endpoints)
        self.assertIn("pmda_mcp_admin.api_admin_mcp_token_revoke", endpoints)
        self.assertIn("pmda_mcp_admin.api_mcp_tool_call", endpoints)

    def test_disabled_mcp_tool_route_still_blocks_before_token_auth(self):
        resp = self.client.post("/api/mcp/tool", json={"tool": "pmda.status"})
        self.assertEqual(resp.status_code, 403, resp.get_json())
        self.assertEqual((resp.get_json() or {}).get("code"), "mcp_disabled")


if __name__ == "__main__":
    unittest.main()
