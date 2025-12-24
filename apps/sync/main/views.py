from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import HttpResponse, JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseServerError
from typing import List
import yaml
import json
import re
import psycopg
from psycopg import sql
import threading
from os import environ as env
import requests
import uuid

def to_snake_case(target: str) -> str:
    """Attempts to convert from regular words/sentences to snake_case. This will not affect strings already in underscore notation. (Does not work with camelCase)
    @param target
    @return Returns underscore notation string. e.g. "hi I am Wywy" -> "hi_I_am_Wywy"
    """
    stringFrags: List[str] = re.split(r"[\.\ \-]", target)
    
    output: str = ""
    
    for i in stringFrags:
        output += i + "_"
    
    return output[:-1] # remove trailing underscore with "[:-1]"

def to_lower_snake_case(target: str) -> str:
    """Attempts to convert from regular words/sentences to lower_snake_case. This will not affect strings already in underscore notation. (Does not work with camelCase)
    @param target
    @return Returns lower_snake_case string. e.g. "hi I am Wywy" -> "hi_i_am_wywy"
    """
    stringFrags: List[str] = re.split(r"[\.\ \-]", target)
    
    output: str = ""
    
    for i in stringFrags:
        output += i.lower() + "_"
    
    return output[:-1] # remove trailing underscore with "[:-1]"

def auto_sync(stop_event: threading.Event) -> None:
    # automatic sync interval in minutes
    auto_sync_interval: float = float(env.get("AUTOSYNC_INTERVAL", 5))
    
    if not auto_sync_interval > 0:
        auto_sync_interval = 5
    
    print(f"Auto-sync is set to sync every {auto_sync_interval} minutes.")
    
    while True:
        # wait until automatic sync interval or until interupted
        if stop_event.wait(timeout=(auto_sync_interval*60)):
            print("Sync requested via interupt...")
            stop_event.clear()
        else:
            print("Automatically syncing...")
        
        sync()

def sync() -> None:
    pass

def get_next_id(table_name: str) -> int:
    with open("/run/secrets/admin", "r") as f:
        response = requests.get(config["referenceUrls"]["db"] + "/wywywebsite/" + table_name + "/get_next_id", cookies={
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

STOP_EVENT: threading.Event = threading.Event()

AUTO_SYNC_THREAD: threading.Thread = threading.Thread(target=auto_sync, args=(STOP_EVENT,))
AUTO_SYNC_THREAD.start()

# END - Global variables

@ensure_csrf_cookie
def index(request: HttpRequest) -> HttpResponse:
    if request.method == "POST" and request.content_type == "application/json":
        # look for the target table
        url_chunks: List[str] = request.path.split("/")
        if len(url_chunks) != 4:
            return HttpResponseBadRequest()
        db_name = to_lower_snake_case(url_chunks[2])
        table_name = to_lower_snake_case(url_chunks[3])
        if not db_name in databases or not table_name in databases[db_name]:
            return HttpResponseBadRequest()
        table: dict = databases[db_name][table_name]

        
        # load in body
        data = json.loads(request.body)
        
        # make a copy of data with snake_cased keys
        f_data = {
            to_lower_snake_case(key): data[key]
            for key in data
        }
        
        if data == None:
            return HttpResponseBadRequest()
        
        # START - validate schema
        valid_request = True # innocent until proven guilty
        # check every item
        for display_column_name in data:
            column_name = to_lower_snake_case(display_column_name)
            is_comments_column: bool = len(column_name) > 9 and column_name[-9:] == "_comments"
            
            # special logic for comments
            if is_comments_column:
                # check if this column is allowed to have comments
                if not table["schema"][column_name[:-9]].get("comments", False):
                    valid_request = False
                    break
                
                if not DATATYPE_CHECK["str"](data[display_column_name]):
                    valid_request = False
                    break
                continue
            
            # check if this is a valid column
            if not column_name in table["schema"]:
                valid_request = False
                break
            
            # check if the datatype is correct
            if not DATATYPE_CHECK[table["schema"][column_name]["datatype"]](data[display_column_name]):
                valid_request = False
                break
            
            # @TODO min/max, etc. checks
        
        if not valid_request:
            return HttpResponseBadRequest()
        # END - validate schema
        
        # store data
        # https://en.wikipedia.org/wiki/Two-phase_commit_protocol
        
        
        # we need our ID to match the production db's ID.
        # if our DB currently does not have any entries, we need to copy the production DB's next ID. Assume, since there is only one user who can commit data, that this ID is accurate.
        # fetch the production DB's next ID.
        next_id: int = get_next_id(table_name)
        if not next_id:
            return HttpResponseServerError()
        
        
        with psycopg.connect(
            dbname=db_name,
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
            
            # cols_string: str = "id,"
            # values_string: str = f"{next_id},"
            cols: List[str] = ["id"]
            values: List = [next_id]
            
            # populate column names & insert values
            for col_name in table["schema"]:
                # cols_string += str(col_name) + ","
                cols.append(col_name)
                
                if col_name in f_data:
                    # if REQUIRES_QUOTATION[table["schema"][col_name]["datatype"]] and :
                    #     values_string += f"'{f_data[col_name]}'"
                    # match (table["schema"][col_name]["datatype"]):
                    #     # case "str", "string", "text":
                    #     #     values_string += f"'{f_data[col_name]}'"
                    #     case "bool", "boolean":
                    #         values_string += str(f_data[col_name]).capitalize()
                    #     case _:
                    #         values_string += str(f_data[col_name])
                    values.append(f_data[col_name])
                else:
                    match(table["schema"][col_name]["datatype"]):
                        case "str", "string", "text":
                            # values_string += "''"
                            values.append("")
                        case _:
                            # values_string += "NULL"
                            values.append(None)
                # values_string += ","
            
            data_conn.execute(sql.SQL("INSERT INTO {table} ({fields}) VALUES({placeholders});").format(table=sql.Identifier(table_name), fields=sql.SQL(', ').join(map(sql.Identifier, cols)),placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))), values).close()
            info_conn.execute("INSERT INTO sync_status (table_name, db_name, entry_id, sync_timestamp, status) VALUES (%s, %s, %s, NULL, NULL);", (table_name, db_name, next_id)).close()
        
        # @TODO recovery
        
        # queue a sync
        STOP_EVENT.set()

        return HttpResponse()
    
    return JsonResponse({"detail": "csrf cookie set"})