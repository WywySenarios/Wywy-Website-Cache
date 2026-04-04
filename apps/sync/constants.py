from requests import get as GET
from typing import Literal, Any
from os import environ
from string import Template

CACHE_URL = f"http://{environ["SYNC_HOST"]}:{environ["SYNC_PORT"]}"
DATA_ENDPOINT: Template = Template(CACHE_URL + "/main/${database_name}/${table_name}")
DESCRIPTOR_ENDPOINT: Template = Template(
    CACHE_URL + "/main/${database_name}/${table_name}/descriptors/${descriptor_name}"
)
TAG_ENDPOINT: Template = Template(
    CACHE_URL + "/tags/${database_name}/${table_name}/${table_type}"
)

GENERIC_REQUEST_PARAMS: dict[str, Any] = {"headers": {}}
AUTH_COOKIES: dict[str, str] = {}

if environ.get("TEST", "FALSE") == "TRUE":
    with open("/run/secrets/admin", "r") as f:
        AUTH_COOKIES["username"] = "admin"
        AUTH_COOKIES["password"] = f.read()

    csrf_response = GET(url=f"{CACHE_URL}/cache/csrf", **GENERIC_REQUEST_PARAMS)
    csrf_json = csrf_response.json()
    if csrf_json is None or "csrfToken" not in csrf_json:
        raise RuntimeError(
            f"Failed to find CSRF token; {csrf_response.status_code}: {csrf_response.text}"
        )
    GENERIC_REQUEST_PARAMS["headers"]["X-CSRFToken"] = csrf_json["csrfToken"]
    AUTH_COOKIES["csrftoken"] = csrf_json["csrfToken"]

    GENERIC_REQUEST_PARAMS["headers"]["Origin"] = environ["MAIN_URL"]
    GENERIC_REQUEST_PARAMS["cookies"] = AUTH_COOKIES

# Username, password, host, and port constant
CONN_CONFIG: dict[Literal["user", "password", "host", "port"], str | int] = {
    "user": environ.get("POSTGRES_USER", "postgres"),
    "password": environ.get("POSTGRES_PASSWORD", "password"),
    "host": environ.get("DATABASE_HOST", "postgres"),
    "port": environ.get("POSTGRES_PORT", 5433),
}
