import psycopg
from psycopg import sql, Connection
from typing import Any, List, TypedDict
from Wywy_Website_Types import EntryData, DictSchema
from constants import CONN_CONFIG


def get_remote_id(
    database_name: str, table_name: str, id: int | str
) -> int | str | None:
    """Attempts to fetch the remote id from the sync_status table.

    Args:
        database_name (str): The target database name.
        table_name (str): The target table name (i.e. the table that contains the FKEY).
        id (int | str): The local ID.

    Returns:
        int | str | None: Returns the remote id on success and None if the parent entry has not been synced yet.
    """
    with psycopg.connect(**CONN_CONFIG, dbname="info") as info_conn:
        info_cur = info_conn.execute(
            "SELECT remote_id FROM sync_status WHERE database_name = %s AND table_name = %s AND entry_id = %s;",
            (database_name, table_name, str(id)),
        )
        output = next(info_cur)[0]
        info_cur.close()
        return output


def update_foreign_key(
    entry: EntryData,
    database_name: str,
    table_name: str,
    target: str,
    target_type: type | None = None,
) -> None:
    """Updates one foreign key of the given entry.

    Args:
        entry (dict): The entry to modify.
        database_name (str): The related database name.
        table_name (str): The target table name (i.e. the table that contains the FKEY).
        targets (str): The key to modify.
        target_type (type, Optional): The expected type.

    Raises:
        RuntimeError: Raises a runtime error if the remote ID is not found.
        TypeError: When the type coercion fails.
        ValueError: When the type coercion fails.
    """
    if target not in entry:
        return

    remote_id = get_remote_id(database_name, table_name, entry[target])
    if remote_id is None:
        raise RuntimeError("Remote ID not found.")

    if target_type is not None:
        try:
            remote_id = target_type(remote_id)
        except (ValueError, TypeError):
            print(f"Cannot coerce `{remote_id}` to `{target_type.__name__}`")
            raise

    entry[target] = remote_id


class DecomposedEntry(TypedDict):
    columns: List[str]
    values_shapes: List[sql.Composable]
    values: List[Any]


def decompose_entry(
    item: EntryData, schema: DictSchema, tagging: bool = False
) -> DecomposedEntry:
    """Decomposes an entry into columns and values based on the given schema.

    Args:
        item (dict): The entry to decompose.
        schema (dict): The schema that the entry should conform to.
        primary_column_name (str): The name of the primary column.
        tagging (bool, optional): Whether or not tagging is enabled. Defaults to False.

    Raises:
        ValueError: When there are missing columns. Ignores erroneous columns.

    Returns:
        Tuple[List[str], List[sql.Composeable], List[Any]]: The column names, the value_shapes, and the values.
    """
    columns: List[str] = []
    values_shapes: List[sql.Composable] = []
    values: List[Any] = []

    # check for primary tag column
    if tagging:
        columns.append("primary_tag")
        values.append(item["primary_tag"])
        values_shapes.append(sql.Placeholder())

    # populate column names & insert values
    for column_name in schema:
        columns.append(column_name)

        if column_name in item:
            values.append(item[column_name])

            # a special command needs to be added to INSERT a geodetic point
            match (schema[column_name]["datatype"]):
                case "geodetic point":
                    values_shapes.append(sql.SQL("ST_GeographyFromText(%s)"))
                case _:
                    values_shapes.append(sql.Placeholder())
        else:
            raise ValueError(f"Column name {column_name} is not within the schema.")

    return {"columns": columns, "values_shapes": values_shapes, "values": values}


def store_entry(
    data_conn: Connection,
    info_conn: Connection,
    target_database_name: str,
    target_table_name: str,
    target_parent_table_name: str,
    target_table_type: str,
    columns: List[str],
    values: List[Any],
    id_column_name: str = "id",
    values_shapes: List[sql.Composable] | None = None,
) -> int | str | None:
    """Stores an entry, assuming that item is valid, does not contain extra columns, and is not missing any columns.

    Args:
        data_conn (psycopg.Connection): Connection to the target database.
        info_conn (psycopg.Connection): Connection to the info database.
        columns (List[str]): The column names to enter.
        values (list): An ordered list of the values to insert.
        target_database_name (str): The name of the target database.
        target_table_name (str): The name of the target table.
        target_parent_table_name (str): The name of the target table's parent.
        target_table_type (str): The target table's type.
        id_column_name (str, optional): The name of the ID column (PRIMARY KEY). Defaults to "id".
        values_shapes (List[sql.Composable] | None, optional): The structure of the VALUES part of the INSERT INTO query. Defaults to None.
    Raises:
        Psycopg.Error: When storing the entry fails. store_entry should be encapsulated in a try-catch to rollback when necessary.

    Returns:
        int | str | None: The ID (PRIMARY KEY) that was pushed to the data table
    """

    id: int | str | None = None

    values_shape: sql.Composable

    if values_shapes is None:
        values_shape = sql.SQL(", ").join(sql.Placeholder() * len(values))
    else:
        values_shape = sql.SQL(", ").join(values_shapes)

    data_cur = data_conn.execute(
        sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES({values}) RETURNING {id_column};"
        ).format(
            table=sql.Identifier(target_table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=values_shape,
            id_column=sql.Identifier(id_column_name),
        ),
        (*values,),
    )
    id = next(data_cur)[0]
    info_conn.execute(
        "INSERT INTO sync_status (table_name, parent_table_name, table_type, database_name, entry_id, remote_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL);",
        (
            target_table_name,
            target_parent_table_name,
            target_table_type,
            target_database_name,
            id,
        ),
    ).close()
    data_cur.close()
    return id
