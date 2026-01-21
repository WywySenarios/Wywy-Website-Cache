import psycopg
from psycopg import sql
import requests
from typing import List
from os import environ as env

def store_entry(data_conn, info_conn, item: dict, schema: dict, target_database_name: str, target_table_name: str, target_parent_table_name: str, target_table_type: str, id_column_name: str = "id", tagging = False) -> str | None:
    """Stores an entry in both the respective data table and the info/sync table.

    Args:
        data_conn (_type_): Connection to the target database.
        info_conn (_type_): Connection to the info database.
        item (dict): The item whose data will be enter.
        schema (dict): The column schema corresponding to the entry.
        taregt_database_name (str): The name of the target database.
        target_table_name (str): The name of the target table.
        target_parent_table_name (str): The name of the target table's parent.
        target_table_type (str): The target table's type.
        id_column_name (str, optional): The ID column's name. Defaults to "id".
        tagging (bool, optional): _description_. Defaults to False.

    Raises:
        ValueError: When a schema column is missing from the given entry to record.

    Returns:
        int | str | None: The ID of the newly stored column.
    """
    id: int | str | None = None
    
    cols: List[str] = []
    values: List = []
    
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
    try:
        data_cur = data_conn.execute(sql.SQL("INSERT INTO {table} ({fields}) VALUES({placeholders}) RETURNING {id_column_name};").format(table=sql.Identifier(target_table_name), fields=sql.SQL(', ').join(map(sql.Identifier, cols)), placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values)), id_column_name=id_column_name), values)
        id = next(data_cur)[0]
        info_conn.execute("INSERT INTO sync_status (table_name, parent_table_name, table_type, db_name, entry_id, remote_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL);", (target_table_name, target_parent_table_name, target_table_type, target_database_name, next_id)).close()
    except:
        data_conn.rollback()
        info_conn.rollback()
    return id

def store_raw_entry(item: dict, target_database_name: str, target_table_name: str, target_parent_table_name: str, target_table_type: str, id_column_name: str = "id") -> int | str:
    """Stores an entry, assuming that item is valid, does not contain extra columns, and is not missing any columns.

    Args:
        item (dict): The item whose data will be enter.
        taregt_database_name (str): The name of the target database.
        target_table_name (str): The name of the target table.
        target_parent_table_name (str): The name of the target table's parent.
        target_table_type (str): The target table's type.
        id_column_name (str, optional): The name of the ID column (PRIMARY KEY).
        
    Returns:
        int | str | None: The ID (PRIMARY KEY) that was pushed to the data table
    """

    columns: List[str] = []
    values: list = []
    id: int | str | None = None

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
        try:
            data_cur = data_conn.execute(sql.SQL("INSERT INTO {table} ({fields}) VALUES({placeholders}) RETURNING {id_column};").format(table=sql.Identifier(target_table_name), fields=sql.SQL(', ').join(map(sql.Identifier, columns)),placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values)), id_column=sql.Identifier(id_column_name)), values)
            id = next(data_cur)[0]
            info_conn.execute("INSERT INTO sync_status (table_name, parent_table_name, table_type, db_name, entry_id, remote_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL);", (target_table_name, target_parent_table_name, target_table_type, target_database_name, id)).close()
            data_cur.close()
        except psycopg.Error as e:
            data_conn.rollback()
            info_conn.rollback()
        return id