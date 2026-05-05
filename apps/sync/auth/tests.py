from unittest import TestCase
from psycopg import connect
from constants import CONN_CONFIG
from .sessions import create_session, validate_session
from .creds import check_creds


class TestSessionCreation(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        with connect(**CONN_CONFIG, dbname="info") as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE sessions")

    def testSessionCreationAndValidation(self):
        """Test whether or not session creation runs to completion and passes validation"""

        token = create_session("admin")

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
