from psycopg import connect, Error
from constants import CONN_CONFIG
import secrets
import hashlib
from logging import getLogger

logger = getLogger("auth")


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
                "SELECT sessions.secret_hash, users.username, FLOOR(EXTRACT(EPOCH FROM (NOW() - sessions.last_seen))/60/60)::INT FROM sessions INNER JOIN users ON users.id=sessions.user_id WHERE sessions.id=%s",
                (id,),
            )

            rows = cur.fetchall()

            if len(rows) != 1:
                return (False, "")

            valid = secrets.compare_digest(
                rows[0][0], hashlib.sha256(secret.encode()).hexdigest()
            )

            valid &= rows[0][2] < 1000

            if rows[0][2] >= 1:
                # try to update last_seen. It usually doesn't matter if this actually goes through.
                try:
                    cur.execute(
                        """
                        WITH session AS (
                            UPDATE sessions SET last_seen=NOW() WHERE id=%s RETURNING user_id
                        ) UPDATE users SET last_seen=NOW() WHERE id=(SELECT user_id FROM session);
                        """,
                        (id,),
                    )
                except Error as e:
                    logger.error(e, exc_info=True)

            if valid:
                logger.info("Session %s validated.", id)
                return (True, rows[0][1])
            else:
                return (False, "")
