from django.http import (
    HttpResponse,
    JsonResponse,
    HttpRequest,
    HttpResponseBadRequest,
    HttpResponseServerError,
    HttpResponseNotAllowed,
)
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from wywy_website_types.data import EntryTableData
from constants import CONN_CONFIG
import json

from database.schema import databases
from utils import chunkify_url, to_lower_snake_case, remove_quotation
from sync.sync import queue_sync
from database.db import store_entry


def handle_select_request(request: HttpRequest) -> HttpResponse:
    url_chunks = chunkify_url(request.path)

    if len(url_chunks) != 4:
        return HttpResponseBadRequest(
            "Bad URL. Expecting URL in the form [...]/[database name]/[table name]/[table type]"
        )

    database_name = to_lower_snake_case(url_chunks[1])
    table_name = to_lower_snake_case(url_chunks[2])
    table_type = to_lower_snake_case(url_chunks[3])

    if database_name not in databases:
        return HttpResponseBadRequest(f'Database "{database_name}" was not found.')
    if table_name not in databases[database_name]:
        return HttpResponseBadRequest(
            f'Table "{database_name}/{table_name}" was not found.'
        )

    if databases[database_name][table_name].get("tagging", False) is not True:
        return HttpResponseBadRequest(
            f'Tagging is not enabled on table "{database_name}/{table_name}"'
        )

    match table_type:
        case "tags" | "tag_names" | "tag_aliases" | "tag_groups":
            pass
        case _:
            return HttpResponseBadRequest(f'"{table_type}" is not a valid table type.')

    # get results
    with psycopg.connect(
        **CONN_CONFIG,
        dbname=database_name,
        row_factory=dict_row,  # pyright: ignore[reportArgumentType]
    ) as conn:
        with conn.cursor() as cur:
            # @TODO change to tag_aliases
            cur.execute(
                sql.SQL("SELECT * FROM {table_name};").format(
                    table_name=sql.Identifier(f"{table_name}_{table_type}")
                )
            )

            if cur.description is None:
                return HttpResponseServerError(
                    "Could not fetch the column schema from the database."
                )

            output: EntryTableData = {
                "columns": [column.name for column in cur.description],
                "data": [list(row) for row in cur.fetchall()],
            }
        return JsonResponse(output)


def handle_insert_request(request: HttpRequest) -> HttpResponse:
    url_chunks = chunkify_url(request.path)

    if len(url_chunks) != 4:
        return HttpResponseBadRequest(
            "Bad URL. Expecting URL in the form [...]/[database name]/[table name]/[table type]"
        )

    database_name = to_lower_snake_case(url_chunks[1])
    table_name = to_lower_snake_case(url_chunks[2])
    table_type = to_lower_snake_case(url_chunks[3])

    if database_name not in databases:
        return HttpResponseBadRequest(f'Database "{database_name}" was not found.')
    if table_name not in databases[database_name]:
        return HttpResponseBadRequest(
            f'Table "{database_name}/{table_name}" was not found.'
        )

    if databases[database_name][table_name].get("tagging", False) is not True:
        return HttpResponseBadRequest(
            f'Tagging is not enabled on table "{database_name}/{table_name}"'
        )

    # make sure the body has content
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError as e:
        return HttpResponseBadRequest(f"Invalid JSON: {e}")
    if not data:
        return HttpResponseBadRequest("Empty or invalid body.")

    with (
        psycopg.connect(**CONN_CONFIG, dbname=database_name) as data_conn,
        psycopg.connect(**CONN_CONFIG, dbname="info") as info_conn,
    ):
        try:
            match table_type:
                case "tags":
                    # validate input
                    if "entry_id" not in data:
                        return HttpResponseBadRequest("The entry ID was not provided.")
                    if not isinstance(data["entry_id"], int) or data["entry_id"] <= 0:
                        return HttpResponseBadRequest(
                            "The ID of the entry that is being tagged must be a positive integer."
                        )
                    if "tag_id" not in data:
                        return HttpResponseBadRequest("Related ID not provided.")
                    if not isinstance(data["tag_id"], int) or data["tag_id"] <= 0:
                        return HttpResponseBadRequest(
                            "The related tag ID must be a positive integer."
                        )
                    if len(list(data)) > 2:
                        return HttpResponseBadRequest(
                            "Erroneous information was provided."
                        )

                    # store data
                    store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        f"{table_name}_tags",
                        table_name,
                        "tags",
                        list(data.keys()),
                        list(data.values()),
                    )
                case "tag_names":
                    # validate input
                    if "tag_name" not in data:
                        return HttpResponseBadRequest("New tag name not provided.")
                    if not isinstance(data["tag_name"], str):
                        return HttpResponseBadRequest(
                            "The new tag name must be a string."
                        )
                    if len(list(data)) > 1:
                        return HttpResponseBadRequest(
                            "Erroneous information was provided."
                        )

                    # unquote necessary fields
                    data["tag_name"] = remove_quotation(data["tag_name"])

                    # store data
                    # @TODO atomicity
                    next_id = store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        f"{table_name}_tag_names",
                        table_name,
                        "tag_names",
                        list(data.keys()),
                        list(data.values()),
                    )

                    # automatically add the related alias
                    store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        f"{table_name}_tag_aliases",
                        table_name,
                        "tag_aliases",
                        ["alias", "tag_id"],
                        [data["tag_name"], next_id],
                        id_column_name="alias",
                    )
                case "tag_aliases":
                    # validate input
                    if "alias" not in data:
                        return HttpResponseBadRequest("New alias name not provided.")
                    if not isinstance(data["alias"], str):
                        return HttpResponseBadRequest(
                            "The new alias name must be a string."
                        )
                    if "tag_id" not in data:
                        return HttpResponseBadRequest("Related ID not provided.")
                    if not isinstance(data["tag_id"], int) or data["tag_id"] <= 0:
                        return HttpResponseBadRequest(
                            "The related tag ID must be a positive integer."
                        )
                    if len(list(data)) > 2:
                        return HttpResponseBadRequest(
                            "Erroneous information was provided."
                        )

                    # unquote necessary fields
                    data["alias"] = remove_quotation(data["alias"])

                    store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        f"{table_name}_tag_aliases",
                        table_name,
                        "tag_aliases",
                        list(data.keys()),
                        list(data.values()),
                        id_column_name="alias",
                    )
                case "tag_groups":
                    # validate input
                    if "group_name" not in data:
                        return HttpResponseBadRequest(
                            "Related group name not provided."
                        )
                    if not isinstance(data["alias"], str):
                        return HttpResponseBadRequest(
                            "The related group name must be a string."
                        )
                    if "tag_id" not in data:
                        return HttpResponseBadRequest(
                            "The ID of the tag being grouped was not provided."
                        )
                    if not isinstance(data["tag_id"], int) or data["tag_id"] <= 0:
                        return HttpResponseBadRequest(
                            "The ID of the tag being grouped must be a positive integer."
                        )
                    if len(list(data)) > 2:
                        return HttpResponseBadRequest(
                            "Erroneous information was provided."
                        )

                    # unquote necessary fields
                    data["group_name"] = remove_quotation(data["group_name"])

                    store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        f"{table_name}_tag_groups",
                        table_name,
                        "tag_groups",
                        list(data.keys()),
                        list(data.values()),
                    )
                case _:
                    return HttpResponseBadRequest(
                        "Invalid URL. Expecting tags/[databaseName]/[tableName]/[tag_names/tag_aliases]."
                    )
        except psycopg.Error:
            data_conn.rollback()
            info_conn.rollback()
            return HttpResponseServerError(
                "Database/schema check faliure. Contact the website administrator and dev for a fix."
            )

    queue_sync()
    return HttpResponse()


# Create your views here.
def index(request: HttpRequest) -> HttpResponse:
    if request.method == "GET":
        return handle_select_request(request)
    elif request.method == "POST":
        return handle_insert_request(request)

    return HttpResponseNotAllowed(permitted_methods=["GET", "POST"])
