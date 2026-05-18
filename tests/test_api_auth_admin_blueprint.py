import unittest

import pmda


class AuthAdminBlueprintTests(unittest.TestCase):
    def test_auth_and_admin_user_routes_are_registered_from_blueprint(self):
        endpoints = set(pmda.app.view_functions)
        expected = {
            "pmda_auth_admin.api_auth_bootstrap_status",
            "pmda_auth_admin.api_auth_bootstrap",
            "pmda_auth_admin.api_auth_login",
            "pmda_auth_admin.api_auth_me",
            "pmda_auth_admin.api_auth_profile_update",
            "pmda_auth_admin.api_auth_password_update",
            "pmda_auth_admin.api_auth_logout",
            "pmda_auth_admin.api_admin_users_get",
            "pmda_auth_admin.api_admin_users_create",
            "pmda_auth_admin.api_admin_users_update",
            "pmda_auth_admin.api_admin_users_delete",
        }
        self.assertTrue(expected.issubset(endpoints), expected - endpoints)


if __name__ == "__main__":
    unittest.main()
