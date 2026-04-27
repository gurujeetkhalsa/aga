import base64
import json
import unittest

from bayrate.auth import authorize_bayrate_admin, extract_bayrate_principal


class FakeAuthAdapter:
    def __init__(self, rows=None, error=None) -> None:
        self.rows = list(rows or [])
        self.error = error
        self.queries = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        if self.error is not None:
            raise self.error
        return list(self.rows)


def encoded_principal(**claims):
    payload = {
        "auth_typ": "aad",
        "identityProvider": "aad",
        "userDetails": claims.get("preferred_username"),
        "claims": [
            {"typ": key, "val": value}
            for key, value in claims.items()
            if value is not None
        ],
    }
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


class BayRateAuthTest(unittest.TestCase):
    def test_extracts_easy_auth_principal_claims(self) -> None:
        principal = extract_bayrate_principal(
            {
                "X-MS-CLIENT-PRINCIPAL": encoded_principal(
                    preferred_username="admin@example.org",
                    oid="object-123",
                    name="Example Admin",
                ),
                "X-MS-CLIENT-PRINCIPAL-NAME": "fallback@example.org",
                "X-MS-CLIENT-PRINCIPAL-IDP": "aad",
            },
            environ={"BAYRATE_TRUST_EASY_AUTH": "true"},
        )

        self.assertIsNotNone(principal)
        self.assertEqual(principal.principal_name, "admin@example.org")
        self.assertEqual(principal.principal_id, "object-123")
        self.assertEqual(principal.display_name, "Example Admin")
        self.assertEqual(principal.identity_provider, "aad")
        self.assertIn("admin@example.org", principal.lookup_names)

    def test_azure_runtime_requires_explicit_easy_auth_trust_setting(self) -> None:
        result = authorize_bayrate_admin(
            {
                "X-MS-CLIENT-PRINCIPAL-NAME": "admin@example.org",
            },
            FakeAuthAdapter(rows=[{"AdminID": 1}]),
            environ={"WEBSITE_SITE_NAME": "aga-ratings-explorer"},
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.status_code, 503)
        self.assertFalse(result.principal)

    def test_missing_principal_requires_sign_in(self) -> None:
        result = authorize_bayrate_admin(
            {},
            FakeAuthAdapter(rows=[{"AdminID": 1}]),
            environ={"BAYRATE_TRUST_EASY_AUTH": "true"},
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.status_code, 401)

    def test_active_sql_admin_is_authorized(self) -> None:
        adapter = FakeAuthAdapter(rows=[{"AdminID": 7, "Principal_Name": "admin@example.org"}])

        result = authorize_bayrate_admin(
            {
                "X-MS-CLIENT-PRINCIPAL-NAME": "admin@example.org",
            },
            adapter,
            environ={"BAYRATE_TRUST_EASY_AUTH": "true"},
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.admin_row["AdminID"], 7)
        self.assertEqual(adapter.queries[0][1], ("admin@example.org",))

    def test_non_admin_is_forbidden(self) -> None:
        result = authorize_bayrate_admin(
            {
                "X-MS-CLIENT-PRINCIPAL-NAME": "viewer@example.org",
            },
            FakeAuthAdapter(rows=[]),
            environ={"BAYRATE_TRUST_EASY_AUTH": "true"},
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.status_code, 403)

    def test_missing_admin_table_is_configuration_error(self) -> None:
        result = authorize_bayrate_admin(
            {
                "X-MS-CLIENT-PRINCIPAL-NAME": "admin@example.org",
            },
            FakeAuthAdapter(error=RuntimeError("Invalid object name 'ratings.bayrate_admins'.")),
            environ={"BAYRATE_TRUST_EASY_AUTH": "true"},
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.status_code, 503)
        self.assertIn("bayrate_authorization_schema.sql", result.error)

    def test_local_dev_email_can_supply_principal(self) -> None:
        result = authorize_bayrate_admin(
            {},
            FakeAuthAdapter(rows=[{"AdminID": 9}]),
            environ={"BAYRATE_DEV_AUTH_EMAIL": "local-admin@example.org"},
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.principal.principal_name, "local-admin@example.org")


if __name__ == "__main__":
    unittest.main()
