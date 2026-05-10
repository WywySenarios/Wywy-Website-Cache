"""
Tests for the index login endpoint.

Assumptions:
  - check_creds(username, password) and create_session(username) are
    already unit-tested and are mocked throughout these tests.
  - The view is mounted at "/" in a URLconf named "index".
  - Django settings are configured (e.g. via pytest-django or
    a setUpClass / override_settings decorator).
"""

from unittest.mock import MagicMock, patch
from unittest import TestCase, skip
from django.test import TestCase
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool
from psycopg.errors import CheckViolation
from constants import CONN_CONFIG
from auth.sessions import create_session, validate_session
from auth.creds import check_creds
from typing import cast, Any
import json


class TestSessionCreation(TestCase):
    pool = ConnectionPool(
        conninfo=make_conninfo(
            **CONN_CONFIG,
            dbname="info",
        ),
        min_size=0,
        max_size=10,
    )

    def setUp(self):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET tokens_remaining=1000 WHERE username='admin'"
                )

    def tearDown(self):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE sessions")
                cur.execute("""
                    INSERT INTO sessions (id, user_id, secret_hash)
                    VALUES (
                        'aaaaaaaaaaaaaaaaaaaaaaaa',
                        (SELECT id FROM users WHERE username = 'admin'),
                        encode(sha256('aaaaaaaaaaaaaaaaaaaaaaaa'::bytea), 'hex')
                    )
                    ON CONFLICT (id) DO NOTHING
                    """)

    def testSessionCreationAndValidation(self):
        """Test whether or not session creation runs to completion and passes validation when intended."""

        token = cast(str, create_session("admin"))

        self.assertIsNotNone(token, "A valid user login did not pass validation.")

        tokenized_token = token.split(".")

        self.assertEqual(len(token), 24 + 1 + 24)
        self.assertEqual(
            len(tokenized_token), 2, "The token does not contain exactly one period."
        )
        id, secret = tokenized_token
        self.assertEqual(len(id), 24, "Invalid session ID string length.")
        self.assertEqual(len(secret), 24, "Invalid session secret string length.")

        valid, username = validate_session(token)
        self.assertTrue(
            valid, f"A valid token ({token}) did not pass token validation."
        )
        self.assertEqual(
            username,
            "admin",
            f'The session username is incorrect. Expected "admin", received "{username}".',
        )

        self.assertFalse(
            validate_session(token[:-1])[0],
            "Invalid token (wrong secret) passed validation.",
        )

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE sessions SET last_seen=NOW() - INTERVAL '1000 hours' WHERE id=%s",
                    (id,),
                )

        self.assertFalse(
            validate_session(token)[0], "Invalid token (expired) passed validation."
        )

    def testNoTokensSessionCreation(self):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET tokens_remaining=0 WHERE username='admin'"
                )

        with self.assertRaises(CheckViolation):
            create_session("admin")

    def testTokenBucketRefill(self):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET tokens_remaining = 0, last_seen = NOW() - INTERVAL '1000 seconds' WHERE username = 'admin'"
                )

        self.assertIsNotNone(create_session("admin"))

    def testTokenBucketOverfill(self):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET tokens_remaining=0, last_seen=NOW() - INTERVAL '10000 seconds' WHERE username='admin'"
                )
                cur.execute("SELECT tokens_remaining FROM users WHERE username='admin'")
                self.assertLessEqual(cur.fetchall()[0][0], 1000.01)

    def testBadUserSessionCreation(self):
        self.assertIsNone(create_session("not_admin"))

    def testNegativeSessionValidation(self):
        token = "xxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxx"
        self.assertFalse(
            validate_session(token)[0],
            f"An invalid token ({token}) erroneously passed token validation.",
        )

    def testCredentialValidation(self):
        with open("/run/secrets/admin", "r") as f:
            admin_password = f.read()

            self.assertFalse(check_creds("notadmin", admin_password))
            self.assertFalse(check_creds("admin", admin_password + "lol"))
            self.assertTrue(check_creds("admin", admin_password))


ENDPOINT = "/auth"  # adjust if your URLconf differs
VALID_USER = "admin"
with open("/run/secrets/admin", "r") as f:
    VALID_PASSWORD = f.read()

# ---------------------------------------------------------------------------
# Method / content-type guard
# ---------------------------------------------------------------------------


class TestRequestGuards(TestCase):

    def test_get_request_returns_405(self):
        response = self.client.get(ENDPOINT)
        self.assertEqual(response.status_code, 405)

    def test_put_request_returns_405(self):
        response = self.client.put(ENDPOINT)
        self.assertEqual(response.status_code, 405)

    def test_post_with_form_content_type_returns_400(self):
        response = self.client.post(
            ENDPOINT,
            data=json.dumps({"username": VALID_USER, "password": VALID_PASSWORD}),
            # Client defaults to multipart/form-data – explicitly set it
            content_type="application/x-www-form-urlencoded",
        )
        self.assertEqual(response.status_code, 400)

    def test_post_with_no_content_type_returns_400(self):
        response = self.client.post(ENDPOINT, data="", content_type="text/plain")
        self.assertEqual(response.status_code, 400)


# ---------------------------------------------------------------------------
# JSON validation
# ---------------------------------------------------------------------------


class TestJsonValidation(TestCase):
    def test_malformed_json_returns_400(self):
        response = self.client.post(
            ENDPOINT, data="{not valid json", content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_empty_body_returns_400(self):
        response = self.client.post(ENDPOINT, data="", content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def test_json_array_instead_of_object_returns_400(self):
        """Top-level JSON arrays are not valid request bodies."""
        response = self.client.post(
            ENDPOINT,
            data=json.dumps([VALID_USER, VALID_PASSWORD]),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_json_string_instead_of_object_returns_400(self):
        response = self.client.post(
            ENDPOINT, data='"just a string"', content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_json_null_returns_400(self):
        response = self.client.post(
            ENDPOINT, data="null", content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)


# ---------------------------------------------------------------------------
# Field validation — username and password required
# ---------------------------------------------------------------------------


class TestFieldValidation(TestCase):
    def test_missing_username_returns_400(self):
        response = self.client.post(
            ENDPOINT,
            data=json.dumps({"password": VALID_PASSWORD}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_missing_password_returns_400(self):
        response = self.client.post(
            ENDPOINT,
            data=json.dumps({"username": VALID_USER}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_missing_both_fields_returns_400(self):
        response = self.client.post(
            ENDPOINT, data=json.dumps({}), content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_extra_fields_alongside_valid_creds_are_ignored(self):
        """Unexpected extra keys must not cause a 500."""
        with patch("auth.creds.check_creds", return_value=False):
            response = self.client.post(
                ENDPOINT,
                data=json.dumps(
                    {
                        "username": VALID_USER,
                        "password": VALID_PASSWORD,
                        "extra": "data",
                    }
                ),
                content_type="application/json",
            )
        self.assertNotEqual(response.status_code, 500)


# ---------------------------------------------------------------------------
# Field type safety — no runtime errors for weird but valid JSON values
# ---------------------------------------------------------------------------


class TestFieldTypeSafety(TestCase):
    """
    Valid JSON can contain non-string values for any key.
    The view must return 400 rather than raise a TypeError or AttributeError.
    """

    BAD_TYPE_CASES: list[dict[str, Any]] = [
        {"username": 123, "password": VALID_PASSWORD},
        {"username": VALID_USER, "password": 456},
        {"username": None, "password": VALID_PASSWORD},
        {"username": VALID_USER, "password": None},
        {"username": True, "password": VALID_PASSWORD},
        {"username": [], "password": VALID_PASSWORD},
        {"username": {}, "password": VALID_PASSWORD},
        {"username": VALID_USER, "password": []},
    ]

    def test_non_string_types_return_400(self):
        for payload in self.BAD_TYPE_CASES:
            with self.subTest(payload=payload):
                response = self.client.post(
                    ENDPOINT, data=json.dumps(payload), content_type="application/json"
                )
                self.assertEqual(
                    response.status_code,
                    400,
                    msg=f"Expected 400 for payload {payload}, got {response.status_code}",
                )


# ---------------------------------------------------------------------------
# Authentication outcomes
# ---------------------------------------------------------------------------


class TestAuthentication(TestCase):

    # @patch("auth.sessions.create_session", return_value=VALID_TOKEN)
    # @patch("auth.creds.check_creds", return_value=True)
    def test_valid_credentials_return_200(self):
        response = self.client.post(
            ENDPOINT,
            data=json.dumps({"username": VALID_USER, "password": VALID_PASSWORD}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)

    # @patch("auth.creds.check_creds", return_value=False)
    def test_invalid_credentials_return_401(self):
        response = self.client.post(
            ENDPOINT,
            data=json.dumps({"username": VALID_USER, "password": "wrong"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 401)

    @skip(
        "This test doesn't work. I suspect MagicMock doesn't go through to server-side."
    )
    @patch("auth.creds.check_creds", return_value=False)
    def test_check_creds_called_with_correct_arguments(self, mock_check: MagicMock):
        self.client.post(
            ENDPOINT,
            data=json.dumps(
                {"username": VALID_USER, "password": VALID_PASSWORD, "extra": "data"}
            ),
            content_type="application/json",
        )
        mock_check.assert_called_once_with(VALID_USER, VALID_PASSWORD)


# ---------------------------------------------------------------------------
# Cookie validation
# ---------------------------------------------------------------------------


class TestSessionCookie(TestCase):
    """After a successful login the response must set a Lax, HttpOnly cookie."""

    # @patch("auth.sessions.create_session", return_value=VALID_TOKEN)
    # @patch("auth.creds.check_creds", return_value=True)
    def _login(self):
        return self.client.post(
            ENDPOINT,
            data=json.dumps({"username": VALID_USER, "password": VALID_PASSWORD}),
            content_type="application/json",
        )

    def test_cookie_is_set_after_successful_login(self):
        response = self._login()
        self.assertIn("token", response.cookies)

    def test_cookie_is_http_only(self):
        response = self._login()
        self.assertTrue(
            response.cookies["token"]["httponly"],
            msg="session_token cookie must be HttpOnly",
        )

    def test_cookie_samesite_is_lax(self):
        response = self._login()
        samesite = response.cookies["token"].get("samesite", "")
        self.assertEqual(
            samesite.lower(),
            "lax",
            msg="session_token cookie SameSite must be Lax",
        )

    def test_no_cookie_set_on_failed_login(self):
        with patch("auth.creds.check_creds", return_value=False):
            response = self.client.post(
                ENDPOINT,
                data=json.dumps({"username": VALID_USER, "password": "wrong"}),
                content_type="application/json",
            )
        self.assertNotIn("token", response.cookies)
