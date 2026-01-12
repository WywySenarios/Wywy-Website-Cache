from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import HttpResponse, JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseServerError
from typing import List, Literal
import yaml
import json
import re
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
import threading
from os import environ as env
import requests
import datetime

from utils import to_lower_snake_case, get_env_int
from schema import check_entry, databases

SYNC_VERBOSITY = get_env_int("SYNC_VERBOSITY", 0)

def auto_sync(sync_event: threading.Event) -> None:
    # automatic sync interval in minutes
    auto_sync_interval: float = float(env.get("AUTOSYNC_INTERVAL", 5))
    
    if not auto_sync_interval > 0:
        auto_sync_interval = 5
    
    print(f"Auto-sync is set to sync every {auto_sync_interval} minutes.")
    
    while True:
        # wait until automatic sync interval or until interupted
        if sync_event.wait(timeout=(auto_sync_interval*60)):
            # print("Sync requested via interupt...")
            sync_event.clear()
        else:
            pass
            # print("Automatically syncing...")
        
        sync()

def sync() -> None:
    with psycopg.connect(
            dbname="info",
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "password"),
            host="wywywebsite-cache_database",
            port=env.get("POSTGRES_PORT", 5433)
        ) as info_conn:
        # select all targets that need syncing (failed, not synced yet (NULL)) (do not select mismatch for now)
        targets_cur = info_conn.execute("SELECT id, table_name, parent_table_name, table_type, db_name, entry_id, status FROM sync_status WHERE status IN ('failed') OR status IS NULL;")
        
        num_successes = 0
        num_failures = 0
        for target in targets_cur.fetchall():
            sync_status_id = target[0]
            table_name: str = target[1]
            parent_table_name: str = target[2]
            table_type: str = target[3]
            db_name: str = target[4]
            target_id: str = target[5]
            
            # get the information relating to the target
            target_record_conn = psycopg.connect(
                dbname=db_name,
                user=env.get("POSTGRES_USER", "postgres"),
                password=env.get("POSTGRES_PASSWORD", "password"),
                host="wywywebsite-cache_database",
                port=env.get("POSTGRES_PORT", 5433), row_factory=dict_row
            )
            target_record_cur = target_record_conn.execute(sql.SQL("""
                                    SELECT * FROM {table} WHERE "id"=%s;
                                    """).format(table=sql.Identifier(table_name)), (target_id,))
            
            # @TODO check if the target already exists
            
            # contact sql-receptionist and ask for a record addition
            # @TODO sql-receptionist should reject problematic id keys
            payload = dict(next(target_record_cur))
            for k, v in payload.items():
                if v is None:
                    payload[k] = f"''"
                elif isinstance(v, datetime.datetime) or isinstance(v, datetime.date) or isinstance(v, datetime.time):
                    payload[k] =  f"'{v.isoformat()}'"
                elif isinstance(v, str):
                    payload[k] = f"'{v}'"
            # remove id because the sql-receptionist will take care of that.
            del payload["id"]
            status: None | Literal['updated', 'failed'] = None
            try:
                with open("/run/secrets/admin", "r") as f:
                    response = requests.post(f"{config["referenceUrls"]["db"]}/{db_name}/{parent_table_name}/{table_type}{f"/{table_name[len(parent_table_name) + 1:][:1 + len(table_type)]}" if table_type != "data" else ""}", timeout=5, cookies={
                        "username": "admin",
                        "password": f.read()
                    }, json=payload)
                    response.raise_for_status()
            except (requests.HTTPError, requests.exceptions.RequestException) as e:
                status = "failed"
                num_failures += 1
            else:
                status="updated"
                num_successes += 1
            finally:
                info_conn.execute("""
                                  UPDATE sync_status
                                  SET status=%s, sync_timestamp=%s
                                  WHERE "id"=%s
                                  """, (status, datetime.datetime.now().isoformat(), sync_status_id,))
            target_record_cur.close()
        targets_cur.close()
        
        if SYNC_VERBOSITY > 0:
            print(f"Successfully synced {num_successes} entries and failed to sync {num_failures} entries.")

        if SYNC_VERBOSITY > 1:
            summary_cur = info_conn.execute("SELECT * FROM sync_status;")
            print(summary_cur.fetchall())
            summary_cur.close()
    
    # print("Sync complete.")

def get_next_id(db_name: str, table_name: str) -> int:
    with open("/run/secrets/admin", "r") as f:
        response = requests.get(config["referenceUrls"]["db"] + "/" + db_name + "/" + table_name + "/get_next_id", cookies={
            "username": "admin",
            "password": f.read()
        }, timeout=5)
        response.raise_for_status()
        
        return int(response.text)

# START - Global variables
# datatype checking functions
DATATYPE_CHECK: dict = {
    "int": lambda x: isinstance(x, int),
    "integer": lambda x: isinstance(x, int),
    "float": lambda x: isinstance(x, (int, float)),
    "number": lambda x: isinstance(x, (int, float)),
    "string": lambda x: x is not None,
    "str": lambda x: x is not None,
    "text": lambda x: x is not None,
    "bool": lambda x: str(x).capitalize() in ("TRUE", "FALSE"),
    "boolean": lambda x: str(x).capitalize() in ("TRUE", "FALSE"),

    # yyyy-mm-dd (1–4 digit year)
    "date": lambda x: isinstance(x, str) and re.fullmatch(r"\'[0-9]{1,4}-[0-9]{2}-[0-9]{2}\'", x) is not None,

    # hh:mm:ss | hh:mm:ss.ssssss | Thhmmss | Thhmmss.ssssss
    "time": lambda x: isinstance(x, str) and re.fullmatch(
        r"\'([0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"|[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,6}"
        r"|T[0-9]{6}"
        r"|T[0-9]{6}\.[0-9]{1,6})\'", x
    ) is not None,

    # yyyy-mm-ddThh:mm:ss | yyyy-mm-ddThh:mm:ss.ssssss | yyyy-mm-ddTThhmmss | yyyy-mm-ddTThhmmss.ssssss
    "timestamp": lambda x: isinstance(x, str) and re.fullmatch(
        r"\'[0-9]{1,4}-[0-9]{2}-[0-9]{2}T("
        r"[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"|[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,6}"
        r"|[0-9]{6}"
        r"|[0-9]{6}\.[0-9]{1,6})\'", x
    ) is not None
}

DEFAULT_VALUES = {
    "int": 0,
    "integer": 0,
    "float": 0.0,
    "number": 0.0,
    "string": "",
    "str": "",
    "text": "",
    "bool": False,
    "boolean": False,
    "date": "'0001-01-01'",
    "time": "'01:00:00'",
    "timestamp": "'0001-01-01T01:00:00'"
}

REQUIRES_QUOTATION = {
    "int": False,
    "integer": False,
    "float": False,
    "number": False,
    "string": True,
    "str": True,
    "text": True,
    "bool": False,
    "boolean": False,
    "date": True,
    "time": True,
    "timestamp": True
}

# peak at config
with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

SYNC_EVENT: threading.Event = threading.Event()

AUTO_SYNC_THREAD: threading.Thread = threading.Thread(target=auto_sync, args=(SYNC_EVENT,))
AUTO_SYNC_THREAD.start()

# END - Global variables

def store_entry(data_conn, info_conn, item: dict, schema: dict, target_database_name: str, target_table_name: str, target_parent_table_name: str, target_table_type: str) -> None:
    """Stores an entry in both the respective data table and the info/sync table.

    Args:
        data_conn (_type_): Connection to the target database.
        info_conn (_type_): Connection to the info database.
        item (dict): The item whose data will be entere.
        schema (dict): The column schema corresponding to the entry.
        taregt_database_name (str): The name of the target database.
        target_table_name (str): The name of the target table.
        target_parent_table_name (str): The name of the target table's parent.
        target_table_type (str): The target table's type.
    """
    # we need our ID to match the production db's ID.
    # if our DB currently does not have any entries, we need to copy the production DB's next ID. Assume, since there is only one user who can commit data, that this ID is accurate.
    # fetch the production DB's next ID.
    # apparently ts is unsafe (with incrementation being different behind the scenes for everyone)
    next_id_cur = data_conn.execute(sql.SQL("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {table};").format(table=sql.Identifier(target_table_name)))
    next_id_in_house: int | None = next(next_id_cur)[0]
    next_id: int
    if not next_id_in_house:
        next_id = get_next_id(target_database_name, target_table_name)
        if not next_id:
            return HttpResponseServerError()
    else:
        next_id = next_id_in_house
    next_id_cur.close()
    
    cols: List[str] = ["id"]
    values: List = [next_id]
    
    # populate column names & insert values
    for col_name in schema:
        cols.append(col_name)
        
        if col_name in item:
            # if REQUIRES_QUOTATION[table["schema"][col_name]["datatype"]] and :
            #     values_string += f"'{item[col_name]}'"
            # match (table["schema"][col_name]["datatype"]):
            #     # case "str", "string", "text":
            #     #     values_string += f"'{item[col_name]}'"
            #     case "bool", "boolean":
            #         values_string += str(item[col_name]).capitalize()
            #     case _:
            #         values_string += str(item[col_name])
            values.append(item[col_name])
        else:
            match(schema[col_name]["datatype"]):
                case "str", "string", "text":
                    values.append("")
                case _:
                    values.append(None)
    
    # record the main entry
    data_conn.execute(sql.SQL("INSERT INTO {table} ({fields}) VALUES({placeholders});").format(table=sql.Identifier(target_table_name), fields=sql.SQL(', ').join(map(sql.Identifier, cols)),placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))), values).close()
    info_conn.execute("INSERT INTO sync_status (table_name, parent_table_name, table_type, db_name, entry_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL);", (target_table_name, target_parent_table_name, target_table_type, target_database_name, next_id)).close()

@ensure_csrf_cookie
def index(request: HttpRequest) -> HttpResponse:
    if request.method == "POST" and request.content_type == "application/json":
        # look for the target table
        url_chunks: List[str] = request.path.split("/")
        if len(url_chunks) != 4:
            return HttpResponseBadRequest("Bad POST URL.")
        database_name = to_lower_snake_case(url_chunks[2])
        table_name = to_lower_snake_case(url_chunks[3])
        if not database_name in databases or not table_name in databases[database_name]:
            return HttpResponseBadRequest(f"Database \"{database_name}\" was not found.")
        table: dict = databases[database_name][table_name]

        
        # load in body
        data = json.loads(request.body)

        if not data or "data" not in data:
            return HttpResponseBadRequest("No data supplied.")

        # make a copy of data with snake_cased keys
        f_data = {
            to_lower_snake_case(section_name): (
                {to_lower_snake_case(key): value for key, value in section_data.items()} if isinstance(section_data, dict) else
                [to_lower_snake_case(item) if isinstance(item, str) else item for item in section_data]
            )
            for section_name, section_data in data.items()
        }
        
        
        if not check_entry(f_data, table):
            return HttpResponseBadRequest("The given entry does not conform to the schema.")
        # END - validate schema
        
        # store data
        # @TODO https://en.wikipedia.org/wiki/Two-phase_commit_protocol
        with psycopg.connect(
            dbname=database_name,
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "password"),
            host="wywywebsite-cache_database",
            port=env.get("POSTGRES_PORT", 5433)
        ) as data_conn, psycopg.connect(
            dbname="info",
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "password"),
            host="wywywebsite-cache_database",
            port=env.get("POSTGRES_PORT", 5433)
        ) as info_conn:
            # main entry
            store_entry(data_conn, info_conn, f_data["data"], table["schema"], db_name, table_name, table_name, "data")

            # @TODO tags

            # descriptors
            if "descriptors" in f_data:
                for descriptor_name in f_data["descriptors"]:
                    for descriptor_info in f_data["descriptors"][descriptor_name]:
                        store_entry(data_conn, info_conn, descriptor_info, table["descriptors"][descriptor_name], database_name, table_name, "{table_name}_{descriptor_name}_descriptors", "descriptors")
        
        # @TODO recovery
        
        # queue a sync
        SYNC_EVENT.set()

        return HttpResponse()
    
    return JsonResponse({"detail": "csrf cookie set"})