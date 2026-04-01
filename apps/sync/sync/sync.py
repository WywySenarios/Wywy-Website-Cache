# @TODO fix multithreading database transaction issues >:(
import logging
import threading
from django.http import HttpRequest, HttpResponse
import requests
from requests import Response, HTTPError
import datetime
from os import environ
from constants import CONN_CONFIG
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from typing import List, Literal, Any, Tuple, cast

from schema import databases
from db import update_foreign_key, store_entry, construct_select_all_query

logger = logging.getLogger("sync")


def auto_sync(sync_event: threading.Event) -> None:
    # automatic sync interval in minutes
    auto_sync_interval: float = float(environ.get("AUTOSYNC_INTERVAL", 5))

    if not auto_sync_interval > 0:
        auto_sync_interval = 5

    logger.info(f"Auto-sync is set to sync every {auto_sync_interval} minutes.")

    while True:
        # wait until automatic sync interval or until interupted
        if sync_event.wait(timeout=(auto_sync_interval * 60)):
            logger.debug("Starting automatic sync.")
            sync_event.clear()
        else:
            logger.debug("Starting automatic sync.")

        sync()


def enable_autosync():
    AUTO_SYNC_THREAD.start()


def prepare_payload(
    target_id: str,
    database_name: str,
    table_name: str,
    parent_table_name: str,
    table_type: str,
    remote_id: str | None = None,
) -> Tuple[str, dict[str, Any]] | None:
    id_column_name: str = "id"
    tagging: bool = False
    if table_type == "data" and table_name == parent_table_name:
        tagging = databases[database_name][parent_table_name].get("tagging", False)
    # update the ID column information
    match (table_type):
        case "tag_aliases":
            id_column_name = "alias"
        case _:
            pass

    # find the correct endpoint to POST to & construct the relevant select query
    endpoint: str = ""
    select_query: sql.Composed
    id_column = sql.SQL("{table_name}.{id_column_name}").format(
        table_name=sql.Identifier(table_name),
        id_column_name=sql.Identifier(id_column_name),
    )
    match (table_type):
        case "data":
            values: List[sql.Composable] = []
            conditions: List[sql.Composable] = []
            if tagging:
                conditions.append(
                    sql.SQL(
                        "INNER JOIN sync_status ON sync_status.database_name={database_name} AND sync_status.table_name={tagging_table_name} AND {primary_tag_column}=sync_status.entry_id::integer "
                    ).format(
                        database_name=sql.Literal(database_name),
                        tagging_table_name=sql.Literal(f"{table_name}_tag_names"),
                        primary_tag_column=sql.SQL("{table_name}.primary_tag").format(
                            table_name=sql.Identifier(parent_table_name),
                        ),
                        id_column_name=sql.Identifier(id_column_name),
                    )
                )
                values.append(sql.SQL("sync_status.remote_id::integer AS primary_tag"))
                tagging = False

            conditions.append(
                sql.SQL("WHERE {id_column_name}=%s").format(id_column_name=id_column)
            )

            endpoint = f"{environ["DATABASE_URL"]}/{database_name}/{parent_table_name}/{table_type}"
            select_query = construct_select_all_query(
                table_name,
                databases[database_name][table_name]["schema"],
                values=values,
                conditions=sql.Composed(conditions),
                tagging=tagging,
            )
            # LEFT JOIN sync_status ON entry_id = this.primary_tag
        case _:
            # @TODO test if tag_names works.
            endpoint = f"{environ["DATABASE_URL"]}/{database_name}/{parent_table_name}/{table_type}/{table_name.removeprefix(f"{parent_table_name}_").removesuffix(f"_{table_type}")}"
            select_query = sql.SQL(
                "SELECT * FROM {table_name} WHERE {id_column_name}=%s"
            ).format(
                table_name=sql.Identifier(table_name),
                id_column_name=id_column,
            )

    # get the information relating to the target
    target_record_conn = psycopg.connect(
        **CONN_CONFIG,
        dbname=database_name,
        row_factory=dict_row,  # type: ignore[arg-type]
    )
    target_record_cur = target_record_conn.execute(
        select_query,
        (target_id,),
    )

    # contact sql-receptionist and ask for a record addition
    # @TODO sql-receptionist should reject problematic id keys
    payload = target_record_cur.fetchone()
    if payload is None:
        return
    payload = dict(payload)
    target_record_cur.close()
    target_record_conn.close()
    for k, v in payload.items():
        if (
            isinstance(v, datetime.datetime)
            or isinstance(v, datetime.date)
            or isinstance(v, datetime.time)
        ):
            payload[k] = f"'{v.isoformat()}'"
        elif isinstance(v, str):
            payload[k] = f"'{v.removeprefix("'").removesuffix("'")}'"

    # remove numerical id because the sql-receptionist will take care of that.
    if remote_id is None:
        del payload[id_column_name]
    else:
        payload[id_column_name] = remote_id

    return (endpoint, payload)


def sync() -> None:
    with psycopg.connect(**CONN_CONFIG, dbname="info") as info_conn:
        # select all targets that need syncing (failed, not synced yet (NULL)) (do not select mismatch for now)
        targets_cur = info_conn.execute(
            "SELECT id, table_name, parent_table_name, table_type, database_name, entry_id, remote_id FROM sync_status WHERE status NOT IN ('updated', 'anomalous');"
        )

        num_successes = 0
        num_failures = 0
        for target in targets_cur.fetchall():
            sync_status_id = target[0]
            table_name: str = target[1]
            parent_table_name: str = target[2]
            table_type: str = target[3]
            database_name: str = target[4]
            target_id: str = target[5]

            # @TODO check if the target already exists

            status: None | Literal["modified", "updated", "failed", "anomalous"] = None
            endpoint = None
            payload = None
            remote_id: int | str | None = target[6]

            data = prepare_payload(
                target_id,
                database_name,
                table_name,
                parent_table_name,
                table_type,
                remote_id=target[6],
            )

            if data is None:
                status = "failed"
            else:
                endpoint, payload = data

                # correct the foreign key to the master database's key ids. If those key IDs are not yet available, abort.
                try:
                    # @TODO move logic to JOIN queries
                    match (table_type):
                        case "tag_aliases":
                            update_foreign_key(
                                payload,
                                database_name,
                                f"{parent_table_name}_tag_names",
                                "tag_id",
                                target_type=int,
                            )
                        case "tag_groups":
                            update_foreign_key(
                                payload,
                                database_name,
                                f"{parent_table_name}_tag_names",
                                "tag_id",
                                target_type=int,
                            )
                        case "tags":
                            update_foreign_key(
                                payload,
                                database_name,
                                f"{parent_table_name}_tag_names",
                                "tag_id",
                                target_type=int,
                            )
                            update_foreign_key(
                                payload,
                                database_name,
                                parent_table_name,
                                "entry_id",
                                target_type=int,
                            )
                        case _:
                            pass

                    with open("/run/secrets/admin", "r") as f:
                        response = requests.post(
                            endpoint,
                            timeout=5,
                            headers={"Origin": environ["CACHE_URL"]},
                            cookies={"username": "admin", "password": f.read()},
                            json=payload,
                        )
                        response.raise_for_status()

                        remote_id = response.text

                        if not remote_id:
                            raise ValueError("remote_id is not valid.")
                        status = "updated"
                except RuntimeError:
                    status = "failed"
                except (requests.HTTPError, requests.exceptions.RequestException):
                    status = "failed"
                except ValueError:
                    status = "anomalous"
                else:
                    status = "updated"

            match (status):
                case "updated":
                    num_successes += 1
                case _:
                    num_failures += 1
            info_conn.execute(
                """
                UPDATE sync_status
                SET status=%s, sync_timestamp=%s, remote_id=%s
                WHERE "id"=%s;
                """,
                (
                    status,
                    datetime.datetime.now().isoformat(),
                    remote_id,
                    sync_status_id,
                ),
            ).close()
        targets_cur.close()

        logger.info(
            f"Successfully synced {num_successes} entries and failed to sync {num_failures} entries."
        )


def pull(database_name: str, parent_table_name: str, table_type: str = "data") -> None:
    """Pulls in entries from the master database.

    Args:
        database_name (str): The database containing the respective table.
        parent_table_name (str): The parent table name, or the table name if the table has no parent.
        table_type (str, optional): The target table type. Defaults to "data".

    Raises:
        HTTPError: When the master database cannot be contacted.
        ValueError: When a schema column is missing from an entry to record.
        Psycopg.Error: When storing an entry fails.
        RuntimeError: When the data the master database returned is invalid, or when table_type is invalid.
    """
    if table_type not in {"tags", "tag_names", "tag_aliases", "tag_groups"}:
        raise ValueError(f"Table type {table_type} not supported for pulling.")

    with (
        psycopg.connect(**CONN_CONFIG, dbname=database_name) as data_conn,
        psycopg.connect(**CONN_CONFIG, dbname="info") as info_conn,
    ):
        try:
            # @TODO tables with many entries

            # find the correct endpoint to GET from
            endpoint: str = ""
            match (table_type):
                case "data":
                    endpoint = f"{environ}/{database_name}/{parent_table_name}"
                case _:
                    endpoint = f"{environ["DATABASE_URL"]}/{database_name}/{parent_table_name}/{table_type}"

            response: Response
            # get all data
            with open("/run/secrets/admin", "r") as f:
                response = requests.get(
                    endpoint,
                    timeout=5,
                    headers={"Origin": environ["CACHE_URL"]},
                    cookies={"username": "admin", "password": f.read()},
                )
                response.raise_for_status()

                data = response.json()
                if (
                    data is None
                    or "data" not in data
                    or not isinstance(data["data"], list)
                    or "columns" not in data
                    or not isinstance(data["columns"], list)
                ):
                    raise RuntimeError("Invalid data received.")

                # check if the schema matches
                # check to see that the column names are valid
                columns = set(data["columns"])
                id_column_name: str = "id"
                remove_id_column: bool = True
                table_name: str = parent_table_name
                match (table_type):
                    # case "data": @TODO
                    case "tags":
                        if {"id", "entry_id", "tag_id"} != columns:
                            raise RuntimeError("Malformed column names.")

                        table_name = f"{parent_table_name}_tags"
                    case "tag_names":
                        if {"id", "tag_name"} != columns:
                            raise RuntimeError("Malformed column names.")

                        table_name = f"{parent_table_name}_tag_names"
                    case "tag_aliases":
                        if {"alias", "tag_id"} != columns:
                            raise RuntimeError("Malformed column names.")

                        id_column_name = "alias"
                        remove_id_column = False
                        table_name = f"{parent_table_name}_tag_aliases"
                    case "tag_groups":
                        if {"id", "tag_id", "group_name"} != columns:
                            raise RuntimeError("Malformed column names.")

                        table_name = f"{parent_table_name}_tag_groups"
                    case _:
                        raise RuntimeError(f'Invalid table type "{table_type}"')

                num_columns = len(data["columns"])

                for row in data["data"]:
                    if not isinstance(row, list):
                        raise RuntimeError("Malformed row type.")
                    row = cast(list[Any], row)

                    if len(row) != num_columns:
                        raise RuntimeError("Malformed row size.")

                if remove_id_column:
                    id_column_index = data["columns"].index(id_column_name)
                    for row in data["data"]:
                        row.pop(id_column_index)
                    data["columns"].pop(id_column_index)

                for row in data["data"]:
                    store_entry(
                        data_conn,
                        info_conn,
                        database_name,
                        table_name,
                        parent_table_name,
                        table_type,
                        data["columns"],
                        row,
                        id_column_name=id_column_name,
                    )

        except (psycopg.Error, ValueError, HTTPError):
            data_conn.rollback()
            info_conn.rollback()
            raise


def queue_sync() -> None:
    SYNC_EVENT.set()


def request_sync(request: HttpRequest) -> HttpResponse:
    queue_sync()
    return HttpResponse("Queued sync.")


SYNC_EVENT: threading.Event = threading.Event()

AUTO_SYNC_THREAD: threading.Thread = threading.Thread(
    target=auto_sync, args=(SYNC_EVENT,)
)
