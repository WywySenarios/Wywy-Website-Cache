from django.shortcuts import render
from django.http import HttpResponse, JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseServerError
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from os import environ as env

from schema import databases
from utils import chunkify_url, to_lower_snake_case

# Create your views here.
def index(request: HttpRequest) -> HttpResponse:
    # only accept GET requests
    if (request.method == "GET"):
        url_chunks = chunkify_url(request.path, 3)

        # expect "tags/[database_name]/[table_name]"
        if len(url_chunks) < 3:
            return HttpResponseBadRequest()
        
        database_name = to_lower_snake_case(url_chunks[1])
        table_name = to_lower_snake_case(url_chunks[2])

        # lf the requested table
        if not database_name in databases or not table_name in databases[database_name]:
            return HttpResponseBadRequest()

        # check if this table has tagging enabled
        if "tagging" not in databases[database_name][table_name] or not databases[database_name][table_name]:
            return HttpResponseBadRequest()

        # get results
        with psycopg.connect(
            dbname=database_name,
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "password"),
            host="wywywebsite-cache_database",
            port=env.get("POSTGRES_PORT", 5433),
            row_factory=dict_row
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT * FROM {table_name};").format(table_name=sql.Identifier(f"{table_name}_tag_names")))
                return JsonResponse({"tags": cur.fetchall()})

    return HttpResponseBadRequest()