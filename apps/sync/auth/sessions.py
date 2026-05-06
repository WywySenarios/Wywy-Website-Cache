from psycopg import connect
from constants import CONN_CONFIG
import secrets
import hashlib


def create_session(username: str) -> str | None:
    """Attempts to create a new user session.

    A successful creation involves creating a secure token and registering it with the database.

    Args:
        username (str): The username to create a session for.

    Returns:
        str | None: None on failure, the token on success.

    Raises:
        psycopg.errors.CheckViolation: When the user does not have enough tokens to open a new session.
        psycopg.errors.NotNullViolation: When the user does not exist.
    """
    id = secrets.token_urlsafe(24)[:24]
    secret = secrets.token_urlsafe(24)[:24]

    secret_hash = hashlib.sha256(secret.encode()).hexdigest()

    with connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH relevant_user AS (
                    UPDATE users SET tokens_remaining = LEAST (
                        1000,
                        tokens_remaining + EXTRACT(EPOCH FROM (now() - last_seen)) - 1
                    ), last_seen = NOW() WHERE username=%s RETURNING id
                ) INSERT INTO sessions (id, user_id, secret_hash) SELECT %s, id, %s FROM relevant_user
                """,
                (
                    username,
                    id,
                    secret_hash,
                ),
            )

            if cur.rowcount == 0:
                return None

    return f"{id}.{secret}"


def validate_session(token: str) -> tuple[bool, str]:
    """Validates a session and finds the username associated with the session.

    Args:
        token (str): The token to validate.

    Returns:
        tuple[bool, str]: Whether or not the session is valid and a username string if the session is valid. The string will be empty if the session is invalid.
    """
    id, secret = token.split(".")

    with connect(**CONN_CONFIG, dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sessions.secret_hash, users.username FROM sessions INNER JOIN users ON sessions.user_id = users.id WHERE sessions.id=%s",
                (id,),
            )

            rows = cur.fetchall()

            if len(rows) != 1:
                return (False, "")

            if secrets.compare_digest(
                rows[0][0], hashlib.sha256(secret.encode()).hexdigest()
            ):
                return (True, rows[0][1])
            else:
                return (False, "")
