from unittest import TestCase
from psycopg import connect
from constants import CONN_CONFIG
from .sessions import create_session


class TestSessionCreation(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        with connect(**CONN_CONFIG, dbname="info") as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE sessions")

    def testSessionCreation(self):
        """Test whether or not session creation runs to completion and contains a potentially valid token."""

        token = create_session("admin")

        self.assertEqual(len(token), 24 + 1 + 24)
