from unittest import TestCase
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool
from psycopg.errors import CheckViolation, NotNullViolation
from constants import CONN_CONFIG
from .sessions import create_session, validate_session
from .creds import check_creds
from typing import cast


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
