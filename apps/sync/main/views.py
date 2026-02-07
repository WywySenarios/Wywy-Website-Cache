from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import HttpResponse, JsonResponse, HttpRequest, HttpResponseBadRequest, HttpResponseServerError, HttpResponseForbidden
from typing import List, Literal
import yaml
import json
import re
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from os import environ as env
import datetime

from utils import to_lower_snake_case, get_env_int, chunkify_url
from schema import check_entry, databases
from sync.sync import queue_sync
from db import store_entry, store_raw_entry

# peak at config
with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

def index(request: HttpRequest) -> HttpResponse:
    if request.method == "GET":
        # look for the target table
        url_chunks: List[str] = chunkify_url(request.path)
        
        if len(url_chunks) < 4:
            return HttpResponseBadRequest("Bad GET URL.")
        
        database_name = to_lower_snake_case(url_chunks[1])
        table_name = to_lower_snake_case(url_chunks[2])
        
        # validate database name
        if database_name not in databases:
            return HttpResponseBadRequest(f"Database \"{database_name}\" was not found.")
        # validate table name
        if table_name not in databases[database_name]:
            return HttpResponseBadRequest(f"Table \"{table_name}\" was not found inside database \"{database_name}\"")
        
        # check if the target table has read permissions
        if not databases[database_name][table_name]["read"]:
            return HttpResponseForbidden(f"{database_name}/{table_name} does not have read permissions.")
        
        # fetch the requested data
        # @TODO more control over the exact parts of the query
        with psycopg.connect(
            dbname=database_name,
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "password"),
            host="wywywebsite-cache_database",
            port=env.get("POSTGRES_PORT", 5433),
        ) as conn:
            with conn.cursor() as cur:
                # @TODO change to tag_aliases
                # @TODO LIMIT
                cur.execute(sql.SQL("SELECT * FROM {table_name};").format(table_name=sql.Identifier(f"{table_name}_tag_names")))
                
                output = {"columns": [column.name for column in cur.description], "data": cur.fetchall()}
        
        # format the data
        date_columns: List[int] = []
        # string_columns: List[int] = []
        if len(output["data"]) > 0:
            for i in range(len(output["data"][0])):
                item = output["data"][0][i]
                if isinstance(item, datetime.datetime) or isinstance(item, datetime.date) or isinstance(item, datetime.time):
                    date_columns.append(i)
                # elif isinstance(item, str):
                    # string_columns.append(i)

            for row in output["data"]:
                for date_index in date_columns:
                    row[date_index] = row[date_index].isoformat()
        
        # return the data
        return JsonResponse(output)
    
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

        if not isinstance(data["data"], dict):
            return HttpResponse("The data section must be composed of JSON objects.")

        # make a copy of data with snake_cased keys
        f_data: dict = {
            "data": {
                to_lower_snake_case(key): value
                for key, value in data["data"].items()
            }
        }

        if "tags" in data:
            if not isinstance(data["tags"], list):
                return HttpResponseBadRequest("Tags must be supplied in an array.")
            
            f_data["tags"] = data["tags"]
        
        if "descriptors" in data:
            if not isinstance(data["descriptors"], dict):
                return HttpResponseBadRequest("Descriptors must be supplied in a JSON object with the key as the descriptor type and the value as an array of descriptors of the corresponding type. The arrays may be empty.")

            try:
                f_data["descriptors"] = {
                    to_lower_snake_case(descriptor_type): list(map(lambda x: {
                        to_lower_snake_case(key): value
                        for key, value in x.items()
                    } if isinstance(x, dict) else None, descriptor_array))
                    for descriptor_type, descriptor_array in data["descriptors"].items()
                }
            except:
                return HttpResponseBadRequest("Failed to parse the supplied descriptors.")
        
        if not check_entry(f_data, database_name, table):
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
            try:
                # @TODO atomicity
                # main entry
                entry_id = store_entry(data_conn, info_conn, f_data["data"], table["schema"], database_name, table_name, table_name, "data", tagging=("tagging" in table and table["tagging"] == True))
                
                # tags
                if "tags" in f_data:
                    for tag_id in f_data["tags"]:
                        store_raw_entry(data_conn, info_conn, {
                            "entry_id": entry_id,
                            "tag_id": tag_id
                        }, database_name, f"{table_name}_tags", table_name, "tags")

                # descriptors
                if "descriptors" in f_data:
                    for descriptor_name in f_data["descriptors"]:
                        for descriptor_info in f_data["descriptors"][descriptor_name]:
                            store_entry(data_conn, info_conn, descriptor_info, table["descriptors"][descriptor_name]["schema"], database_name, f"{table_name}_{descriptor_name}_descriptors", table_name, "descriptors")
            except (psycopg.Error, ValueError) as e:
                data_conn.rollback()
                info_conn.rollback()
                return HttpResponseServerError("Database/schema check faliure. Contact the website administrator and dev for a fix.")
        
        # @TODO recovery
        
        # queue a sync
        queue_sync()

        return HttpResponse()
    
    return JsonResponse({"detail": "csrf cookie set"})