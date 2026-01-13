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
    url_chunks = chunkify_url(request.path)

    if len(url_chunks < 3):
        return HttpResponseBadRequest("Bad URL. Expecting URL in the form tags/[databaseName]/[tableName]/...")

    database_name = to_lower_snake_case(url_chunks[1])
    table_name = to_lower_snake_case(url_chunks[2])

    if database_name not in databases:
        return HttpResponseBadRequest(f"Database \"{database_name}\" was not found.")
    if table_name not in databases[database_name]:
        return HttpResponseBadRequest(f"Table \"{database_name}/{table_name}\" was not found.")

    if "tagging" not in databases[database_name][table_name] or not databases[database_name][table_name]["tagging"] == True:
        return HttpResponseBadRequest(f"Tagging is not enabled on table \"{database_name}/{table_name}\"")
    
    # only accept GET requests
    if (request.method == "GET"):
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