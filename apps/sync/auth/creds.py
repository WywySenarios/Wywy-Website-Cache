from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from constants import CONN_CONFIG
from psycopg import connect

ph = PasswordHasher()


def check_creds(username: str, password: str) -> bool:
    with connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT password_hash FROM users WHERE username=%s", (username,)
            )
            result = cur.fetchall()
            if len(result) == 0:
                return False

            password_hash = result[0][0]

            try:
                ph.verify(password_hash, password)
                return True
            except VerifyMismatchError:
                return False
