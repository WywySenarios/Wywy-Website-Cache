import psycopg
from psycopg import sql
import requests
from typing import List

def get_local_next_id(database_name: str, table_name: str) -> int | None:
    """Gets the next available ID (assuming the table has a SERIAL PRIMARY KEY column called "id").

    Args:
        database_name (str): The database that contains the target table.
        table_name (str): The target table name.

    Returns:
        int | None: None on failure. Otherwise the next avialable ID.
    """
    with psycopg.connect(
            dbname=database_name,
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "password"),
            host="wywywebsite-cache_database",
            port=env.get("POSTGRES_PORT", 5433)
        ) as data_conn:
        with data_conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT MAX(id) AS highest_id FROM {table_name};").format(table_name=sql.Identifier(table_name)))
            next_id: int | None = next(cur)[0]
            if next_id is None:
                return None
            return next_id

def get_next_id(db_name: str, table_name: str) -> int:
    with open("/run/secrets/admin", "r") as f:
        response = requests.get(config["referenceUrls"]["db"] + "/" + db_name + "/" + table_name + "/get_next_id", cookies={
            "username": "admin",
            "password": f.read()
        }, timeout=5)
        response.raise_for_status()
        
        return int(response.text)

def store_entry(data_conn, info_conn, item: dict, schema: dict, target_database_name: str, target_table_name: str, target_parent_table_name: str, target_table_type: str, tagging = False) -> str | None:
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
        tagging (bool, optional): _description_. Defaults to False.

    Raises:
        ValueError: When a schema column is missing from the given entry to record.

    Returns:
        str | None: Returns a string with an error message or None if there was no error.
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
            raise ValueError(f"Column name {col_name} is not within the schema.")
    
    # check for primary tag column
    if tagging:
        cols.append("primary_tag")
        values.append(item["primary_tag"])

    # record the main entry
    data_conn.execute(sql.SQL("INSERT INTO {table} ({fields}) VALUES({placeholders});").format(table=sql.Identifier(target_table_name), fields=sql.SQL(', ').join(map(sql.Identifier, cols)),placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))), values).close()
    info_conn.execute("INSERT INTO sync_status (table_name, parent_table_name, table_type, db_name, entry_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL);", (target_table_name, target_parent_table_name, target_table_type, target_database_name, next_id)).close()

def store_raw_entry(item: dict, target_database_name: str, target_table_name: str, target_parent_table_name: str, target_table_type: str, id: int | None = None) -> None:
    """Stores an entry, assuming that item is valid, does not contain extra columns, and is not missing any columns.

    Args:
        item (dict): The item whose data will be enter.
        taregt_database_name (str): The name of the target database.
        target_table_name (str): The name of the target table.
        target_parent_table_name (str): The name of the target table's parent.
        target_table_type (str): The target table's type.
        id (int): The ID that the new entry will have.
    """
    if id is None:
        id = get_local_next_id(target_database_name, target_table_name)
        if id is None:
            id = 1

    columns: List[str] = ["id"]
    values: list = [id]

    for column_name in item:
        columns.append(column_name)
        values.append(item[column_name])

    with psycopg.connect(
            dbname=target_database_name,
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
        data_conn.execute(sql.SQL("INSERT INTO {table} ({fields}) VALUES({placeholders});").format(table=sql.Identifier(target_table_name), fields=sql.SQL(', ').join(map(sql.Identifier, columns)),placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))), values).close()
        info_conn.execute("INSERT INTO sync_status (table_name, parent_table_name, table_type, db_name, entry_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL);", (target_table_name, target_parent_table_name, target_table_type, target_database_name, id)).close()