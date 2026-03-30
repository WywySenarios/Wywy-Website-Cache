import json
import logging
from typing import List
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest, JsonResponse
from django.views.decorators.csrf import ensure_csrf_cookie, get_token
from schema import databases
from typing import Any
from Wywy_Website_Types import DictTableInfo
from config import CONFIG

from schema import check_item
from utils import to_lower_snake_case

logger = logging.getLogger("cache")

with open("/var/lib/Wywy-Website/cache/cache.json", "r+") as cache_file:
    stored_values = json.loads(cache_file.read())

    cache_values: dict[str, dict[str, dict[str, Any] | None]] = {
        to_lower_snake_case(db["dbname"]): {
            to_lower_snake_case(table["tableName"]): None for table in db["tables"]
        }
        for db in CONFIG["data"]
    }

    if stored_values is not None:
        cache_values = {**cache_values, **stored_values}

    logger.debug(f"Loaded cache values: {str(cache_values)}")


@ensure_csrf_cookie
def index(request: HttpRequest) -> HttpResponse:
    if request.method == "POST" and request.content_type == "application/json":
        # look for the target table
        url_chunks: List[str] = request.path.split("/")
        if len(url_chunks) != 4:
            return HttpResponseBadRequest()
        database_name = to_lower_snake_case(url_chunks[2])
        table_name = to_lower_snake_case(url_chunks[3])
        if not database_name in databases or not table_name in databases[database_name]:
            return HttpResponseBadRequest()
        table: DictTableInfo = databases[database_name][table_name]

        # load in body
        data = json.loads(request.body)

        if data == None:
            return HttpResponseBadRequest()

        if not check_item(data, table["schema"], require_inclusion=False):
            return HttpResponseBadRequest()

        # store the input into the cache
        logger.debug(f"Storing new value to {database_name}/{table_name}: {data}")
        cache_values[database_name][table_name] = data

        with open("/var/lib/Wywy-Website/cache/cache.json", "w+") as cache_file:
            cache_file.write(json.dumps(cache_values))

        return HttpResponse()
    elif request.method == "GET":
        # look for the target table
        url_chunks: List[str] = request.path.split("/")
        if len(url_chunks) != 4:
            return HttpResponseBadRequest()
        database_name = to_lower_snake_case(url_chunks[2])
        table_name = to_lower_snake_case(url_chunks[3])
        if not database_name in databases or not table_name in databases[database_name]:
            return HttpResponseBadRequest()

        # return the cache or an empty dictionary
        if cache_values[database_name][table_name] is None:
            return JsonResponse({})
        else:
            return JsonResponse(cache_values[database_name][table_name])
    return HttpResponseBadRequest()


@ensure_csrf_cookie
def csrf(request: HttpRequest):
    csrf_token = get_token(request)  # Generates/gets the CSRF token
    return JsonResponse(
        {
            "csrfToken": csrf_token,  # Include the token in JSON
            "detail": "CSRF cookie set",
        }
    )
