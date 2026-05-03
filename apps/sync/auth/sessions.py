from psycopg import connect
from constants import CONN_CONFIG
import secrets
import hashlib


def create_session(username: str) -> str:
    """Attempts to create a new user session.

    A successful creation involves creating a secure token and registering it with the database.

    Args:
        username (str): The username to create a session for.

    Returns:
        str | None: None on failure, the token on success.
    """
    id = secrets.token_urlsafe(24)[:24]
    secret = secrets.token_urlsafe(24)[:24]

    secret_hash = hashlib.sha256(secret.encode()).hexdigest()

    with connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sessions (id, user_id, secret_hash) VALUES (%s, (SELECT id FROM users where USERNAME=%s), %s)",
                (
                    id,
                    username,
                    secret_hash,
                ),
            )

    return f"{id}.{secret}"
