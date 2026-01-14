from django.shortcuts import render
from django.http import HttpResponse, JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseServerError
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from os import environ as env

from schema import databases
from utils import chunkify_url, to_lower_snake_case
from db import store_raw_entry

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
                # @TODO change to tag_aliases
                cur.execute(sql.SQL("SELECT * FROM {table_name};").format(table_name=sql.Identifier(f"{table_name}_tag_names")))
                return JsonResponse({"tags": cur.fetchall()})
    elif (request.method == "POST"):
        if len(url_chunks) < 4:
            return HttpResponseBadRequest("Invalid URL. Expecting tags/[databaseName]/[tableName]/[tag_names/tag_aliases].")
        
        command = url_chunks[3]

        # make sure the body has content
        data = json.loads(request.body)
        if not data:
            return HttpResponseBadRequest("Empty or invalid body.")

        match command:
            case "tags":
                # validate input
                if "entry_id" not in data:
                    return HttpResponseBadRequest("The entry ID was not provided.")
                if not isinstance(data["entry_id"], int) or data["entry_id"] <= 0:
                    return HttpResponseBadRequest("The ID of the entry that is being tagged must be a postivie integer.")
                if "tag_id" not in data:
                    return HttpResponseBadRequest("Related ID not provided.")
                if not isinstance(data["tag_id"], int) or data["tag_id"] <= 0:
                    return HttpResponseBadRequest("The related tag ID must be a positive integer.")
                if len(list(data)) > 2:
                    return HttpResponseBadRequest("Erroneous information was provided.")

                # store data
                store_raw_entry(database_name, f"{table_name}_tags", data)
            case "tag_names":
                # validate input
                if "tag_name" not in data:
                    return HttpResponseBadRequest("New tag name not provided.")
                if not isinstance(data["tag_name"], str):
                    return HttpResponseBadRequest("The new tag name must be a string.")
                if len(list(data)) > 1:
                    return HttpResponseBadRequest("Erroneous information was provided.")

                # store data
                next_id: int | None = get_local_next_id()

                if next_id is None:
                    return HttpResponseServerError("Could not find the next ID. Database anomaly?")

                # @TODO atomicity
                store_raw_entry(database_name, f"{table_name}_tag_names", data)

                # automatically add the related alias
                store_raw_entry(database_name, f"{table_name}_tag_aliases", {
                    "alias": data["tag_name"],
                    "tag_id": next_id
                })
            case "tag_aliases":
                # validate input
                if "alias" not in data:
                    return HttpResponseBadRequest("New alias name not provided.")
                if not isinstance(data["alias"], str):
                    return HttpResponseBadRequest("The new alias name must be a string.")
                if "tag_id" not in data:
                    return HttpResponseBadRequest("Related ID not provided.")
                if not isinstance(data["tag_id"], int) or data["tag_id"] <= 0:
                    return HttpResponseBadRequest("The related tag ID must be a positive integer.")
                if len(list(data)) > 2:
                    return HttpResponseBadRequest("Erroneous information was provided.")
                store_raw_entry(database_name, f"{table_name}_tag_aliases", data)
            case "tag_groups":
                # validate input
                if "group_name" not in data:
                    return HttpResponseBadRequest("Related group name not provided.")
                if not isinstance(data["alias"], str):
                    return HttpResponseBadRequest("The related group name must be a string.")
                if "tag_id" not in data:
                    return HttpResponseBadRequest("The ID of the tag being grouped was not provided.")
                if not isinstance(data["tag_id"], int) or data["tag_id"] <= 0:
                    return HttpResponseBadRequest("The ID of the tag being grouped must be a positive integer.")
                if len(list(data)) > 2:
                    return HttpResponseBadRequest("Erroneous information was provided.")
                store_raw_entry(database_name, f"{table_name}_tag_groups", data)
            case _:
                return HttpResponseBadRequest("Invalid URL. Expecting tags/[databaseName]/[tableName]/[tag_names/tag_aliases].")


    return HttpResponseBadRequest("Bad HTTP method. Expects GET or POST.")