from typing import Literal
from os import environ

# Username, password, host, and port constant
CONN_CONFIG: dict[Literal["user", "password", "host", "port"], str | int] = {
    "user": environ.get("POSTGRES_USER", "postgres"),
    "password": environ.get("POSTGRES_PASSWORD", "password"),
    "host": environ.get("DATABASE_HOST", "postgres"),
    "port": environ.get("POSTGRES_PORT", 5433),
}
