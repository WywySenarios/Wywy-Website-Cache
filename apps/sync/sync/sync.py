# @TODO fix multithreading database transaction issues >:(
import threading
import yaml
import requests
import datetime
from os import environ as env
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from typing import List, Literal

from utils import to_lower_snake_case, get_env_int
from schema import databases
from db import update_foreign_key

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

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
        targets_cur = info_conn.execute("SELECT id, table_name, parent_table_name, table_type, database_name, entry_id, status FROM sync_status WHERE status IN ('failed') OR status IS NULL;")
        
        num_successes = 0
        num_failures = 0
        for target in targets_cur.fetchall():
            sync_status_id = target[0]
            table_name: str = target[1]
            parent_table_name: str = target[2]
            table_type: str = target[3]
            id_column_name: str = "id"
            
            # update the ID column information
            match(table_type):
                case "tag_aliases":
                    id_column_name = "alias"
            
            database_name: str = target[4]
            target_id: str = target[5]
            
            # find the correct endpoint to POST to
            endpoint: str = ""
            match(table_type):
                case "data":
                    endpoint = f"{config["referenceUrls"]["db"]}/{database_name}/{parent_table_name}/{table_type}"
                case _:
                    endpoint = f"{config["referenceUrls"]["db"]}/{database_name}/{parent_table_name}/{table_type}/{table_name.removeprefix(f"{parent_table_name}_").removesuffix(f"_{table_type}")}"
            
            # get the information relating to the target
            target_record_conn = psycopg.connect(
                dbname=database_name,
                user=env.get("POSTGRES_USER", "postgres"),
                password=env.get("POSTGRES_PASSWORD", "password"),
                host="wywywebsite-cache_database",
                port=env.get("POSTGRES_PORT", 5433), row_factory=dict_row
            )
            target_record_cur = target_record_conn.execute(sql.SQL("""
                                    SELECT * FROM {table} WHERE {id_column_name}=%s;
                                    """).format(table=sql.Identifier(table_name), id_column_name=sql.Identifier(id_column_name)), (target_id,))
            
            # @TODO check if the target already exists
            
            # contact sql-receptionist and ask for a record addition
            # @TODO sql-receptionist should reject problematic id keys
            payload = dict(next(target_record_cur))
            for k, v in payload.items():
                if v is None:
                    print(f"Sync failed. Anomalous item: ({database_name}/{table_name} ({parent_table_name})): {payload}")
                    return
                elif isinstance(v, datetime.datetime) or isinstance(v, datetime.date) or isinstance(v, datetime.time):
                    payload[k] =  f"'{v.isoformat()}'"
                elif isinstance(v, str):
                    payload[k] = f"'{v.removeprefix("'").removesuffix("'")}'"
            
            # remove numerical id because the sql-receptionist will take care of that.
            if "id" in payload:
                del payload["id"]
            
            status: None | Literal['updated', 'failed'] = None
            
            # correct the foreign key to the master database's key ids. If those key IDs are not yet available, abort.
            try:
                match (table_type):
                    case "tag_aliases":
                        update_foreign_key(payload, database_name, f"{parent_table_name}_tag_names", "tag_id")
                    case "tag_groups":
                        update_foreign_key(payload, database_name, f"{parent_table_name}_tag_names", "tag_id")
                    case "tags":
                        update_foreign_key(payload, database_name, f"{parent_table_name}_tag_names", "tag_id")
                        update_foreign_key(payload, database_name, parent_table_name, "entry_id")
            except RuntimeError as e:
                status = "failed"
                
            remote_id: int | str | None = None
            
            try:
                if status == "failed":
                    raise Warning()
                with open("/run/secrets/admin", "r") as f:
                    response = requests.post(endpoint, timeout=5, cookies={
                        "username": "admin",
                        "password": f.read()
                    }, json=payload)
                    response.raise_for_status()
                    
                    remote_id = response.text
                    
                    if (not remote_id): raise ValueError("remote_id is not valid.")
            except (requests.HTTPError, requests.exceptions.RequestException) as e:
                status = "failed"
                num_failures += 1
            except ValueError as e:
                status = "anomalous"
                num_failures += 1
            except Warning as w:
                status = "failed"
                num_failures += 1
            else:
                status = "added"
                num_successes += 1
            finally:
                info_conn.execute("""
                                UPDATE sync_status
                                SET status=%s, sync_timestamp=%s, remote_id=%s
                                WHERE "id"=%s;
                                """, (status, datetime.datetime.now().isoformat(), remote_id, sync_status_id,)).close()
            target_record_cur.close()
        targets_cur.close()
        
        if SYNC_VERBOSITY > 0:
            print(f"Successfully synced {num_successes} entries and failed to sync {num_failures} entries.")

        if SYNC_VERBOSITY > 1:
            summary_cur = info_conn.execute("SELECT * FROM sync_status WHERE status='failed';")
            print(summary_cur.fetchall())
            summary_cur.close()
    
    # print("Sync complete.")

def queue_sync() -> None:
    SYNC_EVENT.set()

SYNC_EVENT: threading.Event = threading.Event()

AUTO_SYNC_THREAD: threading.Thread = threading.Thread(target=auto_sync, args=(SYNC_EVENT,))
AUTO_SYNC_THREAD.start()