import json
from typing import List
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest, JsonResponse
from django.views.decorators.csrf import ensure_csrf_cookie, get_token
import yaml

from schema import check_item
from utils import to_lower_snake_case

# peak at config
with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

cache_values: dict[str, dict[str, dict | None]] = {
    to_lower_snake_case(db["dbname"]): {
        to_lower_snake_case(table["tableName"]): None
        for table in db["tables"]
    }
    for db in config["data"]
}

# convert all the table schemas into dictionaries with snake_case keys
databases: dict = {
    to_lower_snake_case(db["dbname"]): {
        to_lower_snake_case(table["tableName"]): {
            "read": table["read"],
            "write": table["write"],
            "entrytype": table["entrytype"],
            "schema": {
                to_lower_snake_case(item["name"]): item
                for item in table["schema"]
            }
        }
        for table in db["tables"]
    }
    for db in config["data"]
}

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
        table: dict = databases[database_name][table_name]

        
        # load in body
        data = json.loads(request.body)
        
        if data == None:
            return HttpResponseBadRequest()
        
        if not check_item(data, table["schema"], require_inclusion=False):
            return HttpResponseBadRequest()
        
        # store the input into the cache
        cache_values[database_name][table_name] = data
        
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
    return JsonResponse({
        "csrfToken": csrf_token,      # Include the token in JSON
        "detail": "CSRF cookie set"
    })
