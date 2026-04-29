import logging
from django.http import (
    HttpResponse,
    JsonResponse,
    HttpRequest,
    HttpResponseBadRequest,
    HttpResponseServerError,
    HttpResponseForbidden,
    HttpResponseNotAllowed,
)
from typing import List, Any, cast
from wywy_website_types import (
    DictTableInfo,
    DictDescriptorInfo,
    Entry,
    EntryTableData,
    DictSchema,
)
import json
import psycopg
from psycopg import sql
from constants import CONN_CONFIG
import datetime

from utils import to_lower_snake_case, chunkify_url
from database.schema import check_entry, check_item, check_tags, databases
from sync.sync import queue_sync
from database.db import construct_select_all_query, store_entry, decompose_entry

from tags.views import handle_select_request as handle_tags_select_request

logger = logging.getLogger("database")


def handle_select_request(request: HttpRequest) -> HttpResponse:
    """Handle a database SELECT request.

    Args:
        request (HttpRequest): The request to handle.

    Returns:
        HttpResponse: The response to the client.
    """
    # .../main/[database_name]/[table_name]/[table_type]/[descriptor_name]?
    url_chunks: List[str] = chunkify_url(request.path)

    if len(url_chunks) < 4:
        return HttpResponseBadRequest("Bad GET URL.")

    database_name = to_lower_snake_case(url_chunks[1])
    table_name = to_lower_snake_case(url_chunks[2])
    table_type = to_lower_snake_case(url_chunks[3])

    # validate database name
    if database_name not in databases:
        return HttpResponseBadRequest(f'Database "{database_name}" was not found.')
    # validate table name
    if table_name not in databases[database_name]:
        return HttpResponseBadRequest(
            f'Table "{table_name}" was not found inside database "{database_name}"'
        )
    table_info = databases[database_name][table_name]

    select_query: sql.SQL | sql.Composed
    target_schema: DictSchema
    target_table_name: str
    tagging: bool
    match (table_type):
        case "data":  # .../main/[database_name]/[table_name]/data
            if len(url_chunks) != 4:
                return HttpResponseBadRequest("Bad GET URL.")

            target_table_name = table_name
            target_schema = table_info["schema"]
            tagging = table_info.get("tagging", False)
        case (
            "tags" | "tag_aliases" | "tag_groups" | "tag_names"
        ):  # .../main/[database_name]/[table_name]/[table_type]
            return handle_tags_select_request(request)
        case (
            "descriptors"
        ):  # .../main/[database_name]/[table_name]/descriptors/[descriptor_name]
            if len(url_chunks) != 5:
                return HttpResponseBadRequest("Bad GET URL.")

            if "descriptors" not in table_info:
                return HttpResponseBadRequest("This table has no descriptors.")

            descriptor_name = url_chunks[4]

            if descriptor_name not in table_info["descriptors"]:
                return HttpResponseBadRequest(
                    f"Could not find descriptor {descriptor_name}"
                )

            descriptor_info = table_info["descriptors"][descriptor_name]

            target_table_name = f"{table_name}_{descriptor_name}_descriptors"
            target_schema = descriptor_info["schema"]
            tagging = False
        case _:
            return HttpResponseBadRequest(f'"{table_type}" is not a valid table type.')

    conditions: list[sql.Composable | sql.Composed] = []

    select_id = request.GET.get("id")
    parent_id = request.GET.get("parent_id")
    if select_id is not None:
        conditions.append(sql.SQL("WHERE id={id}").format(id=sql.Literal(select_id)))
    elif parent_id is not None:
        match (table_type):
            case "data":
                return HttpResponseBadRequest(f"Main tables do not have parent tables.")
            case "descriptors":
                return HttpResponseBadRequest(
                    "Not implemented yet. Descriptors currently do not have relationships with their original entries."
                )
            case _:
                logger.critical(
                    f"Unexpected table type found while constructing SELECT query: {table_type}"
                )
        conditions.append(sql.SQL("WHERE "))

    select_query = construct_select_all_query(
        target_table_name,
        target_schema,
        values=[sql.Identifier("id")],
        conditions=sql.SQL(" ").join(conditions),
        tagging=tagging,
    )

    # check if the target table has read permissions
    if not databases[database_name][table_name]["read"]:
        return HttpResponseForbidden(
            f"{database_name}/{table_name} does not have read permissions."
        )

    # fetch data
    with psycopg.connect(**CONN_CONFIG, dbname=database_name) as conn:
        with conn.cursor() as cur:
            # @TODO change to tag_aliases
            # @TODO LIMIT
            cur.execute(select_query)

            if cur.description is None:
                return HttpResponseServerError(
                    "Could not fetch the column schema from the database."
                )

            output: EntryTableData = {
                "columns": [column.name for column in cur.description],
                "data": [list(row) for row in cur.fetchall()],
            }

    # format the data
    date_columns: List[int] = []
    # string_columns: List[int] = []
    if len(output["data"]) > 0:
        for i in range(len(output["data"][0])):
            item = output["data"][0][i]
            if (
                isinstance(item, datetime.datetime)
                or isinstance(item, datetime.date)
                or isinstance(item, datetime.time)
            ):
                date_columns.append(i)
            # elif isinstance(item, str):
            # string_columns.append(i)

        for row in output["data"]:
            for date_index in date_columns:
                row[date_index] = row[date_index].isoformat()

    # return the data
    return JsonResponse(output)


def handle_insert_request(request: HttpRequest) -> HttpResponse:
    """Handle an INSERT request, assuming that the content of the request is JSON.

    Args:
        request (HttpRequest): The request to handle.

    Raises:
        ValueError: On database INSERT severe failure (i.e. anomalous).

    Returns:
        HttpResponse: The response to the client.
    """
    # look for the target table
    url_chunks: List[str] = chunkify_url(request.path)
    entry_info: DictTableInfo | DictDescriptorInfo
    match (len(url_chunks)):
        case 3 | 4:  # .../main/[database_name]/[table_name]/[data]?
            database_name = to_lower_snake_case(url_chunks[1])
            table_name = to_lower_snake_case(url_chunks[2])
            target_table_name = table_name
            if (
                not database_name in databases
                or not table_name in databases[database_name]
            ):
                return HttpResponseBadRequest(
                    f'Table "{database_name}/{table_name}" was not found.'
                )
            if len(url_chunks) == 4 and url_chunks[3] != "data":
                return HttpResponseBadRequest(f"Bad target: {url_chunks[3]}")
            table: DictTableInfo = databases[database_name][table_name]
            entry_info = databases[database_name][table_name]
        case 5:  # .../main/[database_name]/[table_name]/descriptors/[descriptor_name]
            database_name = to_lower_snake_case(url_chunks[1])
            table_name = to_lower_snake_case(url_chunks[2])
            if (
                not database_name in databases
                or not table_name in databases[database_name]
            ):
                return HttpResponseBadRequest(
                    f'Table "{database_name}/{table_name}" was not found.'
                )
            table: DictTableInfo = databases[database_name][table_name]

            if url_chunks[3] != "descriptors":
                return HttpResponseBadRequest("Bad POST URL.")

            descriptor_name = to_lower_snake_case(url_chunks[4])

            if "descriptors" not in table:
                return HttpResponseBadRequest("This table has no descriptors.")

            if descriptor_name not in table["descriptors"]:
                return HttpResponseBadRequest(
                    f"Descriptor {descriptor_name} not found in table {table_name}."
                )

            target_table_name = f"{table_name}_{descriptor_name}_descriptors"
            entry_info = table["descriptors"][descriptor_name]
        case _:
            return HttpResponseBadRequest("Bad POST URL.")

    # load in body
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError as e:
        return HttpResponseBadRequest(f"Invalid JSON: {e}")

    if not data:
        return HttpResponseBadRequest("No data supplied.")

    if not isinstance(data, dict):
        return HttpResponseBadRequest(
            "The data section must be composed of JSON objects."
        )

    data = cast(dict[str, Any], data)
    entry: Entry
    tags: list[str] | None = None
    descriptors: dict[str, list[dict[str, Any]]] | None = None

    data_data = data.get("data", None)

    # determine if we are doing a batch insert (i.e. form submission) or a single entry insert (e.g. editing a record)
    # remember to cast keys to snake case
    if isinstance(data_data, dict):
        data_data = cast(Entry, data_data)
        entry = {}
        for key, value in data_data.items():
            entry[to_lower_snake_case(key)] = value

        if "tags" in data:
            if not table.get("tagging", False):
                return HttpResponseBadRequest("Tagging is disabled.")

            if not isinstance(data["tags"], list):
                return HttpResponseBadRequest("Tags must be supplied in an array.")

            if not check_tags(cast(list[Any], data["tags"]), database_name, table_name):
                return HttpResponseBadRequest("Invalid tags.")

            tags = cast(list[str], data["tags"])

            if "descriptors" in data:
                descriptors = {}
                data_descriptors = data["descriptors"]
                if not "descriptors" in table:
                    return HttpResponseBadRequest("This table has no descriptors.")

                if not isinstance(data_descriptors, dict):
                    return HttpResponseBadRequest(
                        "Descriptors must be supplied in a JSON object with the key as the descriptor type and the value as an array of descriptors of the corresponding type. The arrays may be empty."
                    )

                data_descriptors = cast(dict[str, Any], data_descriptors)
                for descriptor_type, descriptor_array in data_descriptors.items():
                    if not isinstance(descriptor_array, list):
                        return HttpResponseBadRequest(
                            "Descriptors must be supplied in arrays."
                        )

                    descriptor_name = to_lower_snake_case(descriptor_type)

                    if descriptor_name not in table["descriptors"]:
                        return HttpResponseBadRequest(
                            f"Descriptor type {descriptor_name} was not found."
                        )

                    for descriptor_entry in cast(list[Any], descriptor_array):
                        if not isinstance(descriptor_array, dict):
                            return HttpResponseBadRequest(
                                "Descriptor entries must be JSON objects."
                            )

                        check_item(
                            cast(dict[str, Any], descriptor_entry),
                            table["descriptors"][descriptor_name]["schema"],
                        )

                    descriptors[to_lower_snake_case(descriptor_type)] = cast(
                        list[dict[str, Any]], descriptor_array
                    )
    else:
        entry = data

    if not check_entry(entry, database_name, entry_info):
        return HttpResponseBadRequest("The given entry does not conform to the schema.")
    # END - validate schema

    # store data
    # @TODO https://en.wikipedia.org/wiki/Two-phase_commit_protocol
    with (
        psycopg.connect(**CONN_CONFIG, dbname=database_name) as data_conn,
        psycopg.connect(**CONN_CONFIG, dbname="info") as info_conn,
    ):
        try:
            # @TODO atomicity
            # main entry
            entry_id = store_entry(
                data_conn,
                info_conn,
                database_name,
                target_table_name,
                table_name,
                "data",
                **decompose_entry(
                    entry,
                    entry_info["schema"],
                    tagging=entry_info.get("tagging", False),
                    id_column_name="id",
                ),
            )

            # tags
            if tags is not None:
                for tag_id in tags:
                    store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        f"{table_name}_tags",
                        table_name,
                        "tags",
                        ["entry_id", "tag_id"],
                        [entry_id, tag_id],
                    )

            # descriptors
            if descriptors is not None:
                for descriptor_name, descriptor_array in descriptors.items():
                    if "descriptors" not in table:
                        raise ValueError("Descriptors not in table?")
                    for descriptor_entry in descriptor_array:
                        store_entry(
                            data_conn,
                            info_conn,
                            database_name,
                            f"{table_name}_{descriptor_name}_descriptors",
                            table_name,
                            "descriptors",
                            **decompose_entry(
                                descriptor_entry,
                                table["descriptors"][descriptor_name]["schema"],
                            ),
                        )
        except (psycopg.Error, ValueError) as e:
            logger.error(e)
            data_conn.rollback()
            info_conn.rollback()
            return HttpResponseServerError(
                "Database/schema check faliure. Contact the website administrator and dev for a fix."
            )

    # @TODO recovery

    # queue a sync
    queue_sync()

    return HttpResponse(entry_id)


def index(request: HttpRequest) -> HttpResponse:
    if request.method == "GET":
        return handle_select_request(request)

    if request.method == "POST":
        if not request.content_type == "application/json":
            return HttpResponseBadRequest("Expected JSON data.")

        return handle_insert_request(request)

    return HttpResponseNotAllowed(permitted_methods=["GET", "POST"])
