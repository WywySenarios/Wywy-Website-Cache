from django.shortcuts import render
from django.http import HttpResponse, HttpRequest, HttpResponseBadRequest, HttpResponseServerError, HttpResponseForbidden
from requests import HTTPError
from typing import List
import psycopg

from utils import to_lower_snake_case, chunkify_url
from schema import databases
from sync.sync import pull

def index(request: HttpRequest) -> HttpResponse:
    # client requests
    if request.method == "POST":
        # look for the target table
        url_chunks: List[str] = chunkify_url(request.path)
        if len(url_chunks) != 4:
            return HttpResponseBadRequest("Bad POST URL.")
        database_name = to_lower_snake_case(url_chunks[1])
        table_name = to_lower_snake_case(url_chunks[2])
        if not database_name in databases or not table_name in databases[database_name]:
            return HttpResponseBadRequest(f"Database \"{database_name}\" was not found.")
        table: dict = databases[database_name][table_name]
        
        # check for write permissions
        if "write" not in table or table["write"] != True:
            return HttpResponseForbidden(f"Write is not enabled on table {table_name}")
        
        table_type = to_lower_snake_case(url_chunks[3])
        
        try:
            pull(database_name, table_name, table_type=table_type)
        except ValueError as e:
            # @TODO fix ValueError implying both 400 and 500
            return HttpResponseBadRequest(str(e))
        except (HTTPError, psycopg.Error, RuntimeError) as e:
            return HttpResponseServerError(str(e))

        return HttpResponse()
        
    return HttpResponseBadRequest("POST requests only.")