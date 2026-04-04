# datatype checking functions
import logging
import re
from utils import to_lower_snake_case
from constants import CONN_CONFIG
from wywy_website_types import (
    Datatype,
    DictDatabaseInfo,
    DictTableInfo,
    Entry,
    DictSchema,
)
from config import CONFIG
from typing import Callable, Any, List
import psycopg
from psycopg import sql

logger = logging.getLogger("schema")


def is_geodetic_point(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    matches = re.fullmatch(
        r"POINT ?\((-?\d+(?:\.\d+)?) (-?\d+(?:\.\d+)?)\)",
        value,
    )
    if matches is None:
        return False

    # check longitude (X) and latitude (Y)
    if matches.group(1) is None or not (-180 < float(matches.group(1)) < 180):
        return False
    if matches.group(2) is None or not (-90 < float(matches.group(2)) < 90):
        return False

    return True


DATATYPE_CHECK: dict[Datatype, Callable[[Any], bool]] = {
    "int": lambda x: isinstance(x, int),
    "integer": lambda x: isinstance(x, int),
    "float": lambda x: isinstance(x, (int, float)),
    "number": lambda x: isinstance(x, (int, float)),
    "string": lambda x: x is not None,
    "str": lambda x: x is not None,
    "text": lambda x: x is not None,
    "bool": lambda x: str(x).lower() in ("true", "false"),
    "boolean": lambda x: str(x).lower() in ("true", "false"),
    # yyyy-mm-dd (1–4 digit year)
    "date": lambda x: isinstance(x, str)
    and re.fullmatch(r"\'[0-9]{1,4}-[0-9]{2}-[0-9]{2}\'", x) is not None,
    # hh:mm:ss | hh:mm:ss.ssssss | Thhmmss | Thhmmss.ssssss
    "time": lambda x: isinstance(x, str)
    and re.fullmatch(
        r"\'([0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"|[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,6}"
        r"|T[0-9]{6}"
        r"|T[0-9]{6}\.[0-9]{1,6})\'",
        x,
    )
    is not None,
    # yyyy-mm-ddThh:mm:ss | yyyy-mm-ddThh:mm:ss.ssssss | yyyy-mm-ddTThhmmss | yyyy-mm-ddTThhmmss.ssssss
    "timestamp": lambda x: isinstance(x, str)
    and re.fullmatch(
        r"\'[0-9]{1,4}-[0-9]{2}-[0-9]{2}T("
        r"[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"|[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,6}"
        r"|[0-9]{6}"
        r"|[0-9]{6}\.[0-9]{1,6})\'",
        x,
    )
    is not None,
    # @TODO stricter enum checking
    "enum": lambda x: x is not None,
    "geodetic point": is_geodetic_point,
}

DEFAULT_VALUES: dict[Datatype, Any] = {
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
    "timestamp": "'0001-01-01T01:00:00'",
    "geodetic point": "POINT EMPTY",
}

REQUIRES_QUOTATION: dict[Datatype, bool] = {
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
    "timestamp": True,
}

# convert all the table schemas into dictionaries with snake_case
databases: dict[str, DictDatabaseInfo] = {
    to_lower_snake_case(db["dbname"]): {
        to_lower_snake_case(table["tableName"]): {
            **table,
            "schema": {
                to_lower_snake_case(column_schema["name"]): column_schema
                for column_schema in table["schema"]
            },
            "descriptors": (
                {
                    to_lower_snake_case(descriptor_schema["name"]): {
                        "name": to_lower_snake_case(descriptor_schema["name"]),
                        "schema": {
                            to_lower_snake_case(column_schema["name"]): column_schema
                            for column_schema in descriptor_schema["schema"]
                        },
                    }
                    for descriptor_schema in table["descriptors"]
                }
                if "descriptors" in table
                else {}
            ),
        }
        for table in db["tables"]
    }
    for db in CONFIG["data"]
}


def get_all_tags(database_name: str, parent_table_name: str) -> list[int]:
    """Get all the related tags. Expects the target table to have tagged enabled.

    Args:
        database_name (str): The database of the target table.
        parent_table_name (str): The name of the parent table.

    Returns:
        dict: The tag IDs of the target table.
    """
    with psycopg.connect(**CONN_CONFIG, dbname=database_name) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT (id) FROM {table_name};").format(
                    table_name=sql.Identifier(f"{parent_table_name}_tag_names")
                )
            )
            return [row[0] for row in cur.fetchall()]


def check_entry(entry: Entry, database_name: str, table_info: DictTableInfo) -> bool:
    """Checks the given entry against the given table schema.

    Args:
        entry (dict): The entry to check.
        database_name (str): The name of the database that contains the respective table.
        table_info (dict): The table schema to check against.

    Returns:
        bool: Whether or not the given entry conforms to the table schema.
    """

    # @TODO check tags
    if (
        "tagging" in table_info and table_info["tagging"] == True
    ):  # if tagging is enabled,
        # ensure that there is a primary tag
        if "primary_tag" not in entry["data"]:
            logger.debug(
                f"Tagging is enabled on table {table_info["tableName"]}. You must provide a primary tag."
            )
    # if tagging is disabled, ensure that there are no tags
    else:
        if "primary_tag" in entry["data"]:
            logger.debug(
                f"Tagging is disabled on table {table_info["tableName"]}. You cannot supply a primary tag."
            )
            return False

    if not "data" in entry or not check_item(entry["data"], table_info["schema"]):
        logger.debug("There is no data or the data is in an unexpected format.")
        return False

    return True


def check_item(data: Entry, schema: DictSchema, require_inclusion: bool = True) -> bool:
    """Checks the given data against the given schema.

    Args:
        data (dict): The data to check.
        schema (dict): The schema to check against.
        require_inclusion (bool): Whether or not each non-optional column must be present in the item for it to be valid.

    Returns:
        bool: Whether or not the data conforms to the schema.
    """
    # check every item
    for display_column_name in data:
        column_name = to_lower_snake_case(display_column_name)

        # special logic for comments
        if column_name.endswith("_comments"):
            # check if this column is allowed to have comments
            if not schema[column_name[:-9]].get("comments", False):
                logger.debug(
                    f"Comments column {column_name} is related to a column that does not have commenting enabled."
                )
                return False

            if not DATATYPE_CHECK["str"](data[display_column_name]):
                logger.debug(
                    f"Comments column {column_name} must contain a string comment."
                )
                return False
            continue

        # special logic for geodetic point accuracy columns
        if column_name.endswith("_latlong_accuracy"):
            if (
                schema[column_name[: -len("_latlong_accuracy")]]["datatype"]
                != "geodetic point"
            ):
                logger.debug(
                    f"Column {column_name[:-len("_latlong_accuracy")]} is not a geodetic point and therefore cannot have an altitude accuracy."
                )
                return False

            if not DATATYPE_CHECK["float"](data[display_column_name]):
                logger.debug(
                    f"Invalid datatype for column {display_column_name}. Expected a float."
                )
                return False
            continue

        if column_name.endswith("_altitude"):
            if schema[column_name[: -len("_altitude")]]["datatype"] != "geodetic point":
                logger.debug(
                    f"Column {column_name[:-len("_altitude")]} is not a geodetic point and therefore cannot have an altitude."
                )
                if not DATATYPE_CHECK["float"](data[display_column_name]):
                    logger.debug(
                        f"Invalid datatype for column {display_column_name}. Expected a float."
                    )
                return False
            continue

        if column_name.endswith("_altitude_accuracy"):
            if (
                schema[column_name[: -len("_altitude_accuracy")]]["datatype"]
                != "geodetic point"
            ):
                logger.debug(
                    f"Column {column_name[:-len("_altitude_accuracy")]} is not a geodetic point and therefore cannot have an altitude accuracy."
                )
                if not DATATYPE_CHECK["float"](data[display_column_name]):
                    logger.debug(
                        f"Invalid datatype for column {display_column_name}. Expected a float."
                    )
                return False
            continue

        # mostly ignore primary_tag because store_entry will be responsible for validating primary_tag's value.
        if column_name == "primary_tag":
            if not DATATYPE_CHECK["str"](data[display_column_name]):
                logger.debug("The primary tag column does not have a non-empty tag.")
                return False
            continue

        # check if this is a valid column
        if not column_name in schema:
            logger.debug(f'Column "{column_name}" is not within the table\'s schema.')
            return False

        # check if the datatype is correct
        if not DATATYPE_CHECK[schema[column_name]["datatype"]](
            data[display_column_name]
        ):
            logger.debug(
                f"Bad datatype for column {display_column_name}. Expected {schema[column_name]["datatype"]}."
            )
            return False

        # @TODO min/max, etc. checks

    # check if all columns are present
    if require_inclusion:
        for column_name in schema:
            if (
                not column_name in data
                and not schema[column_name].get("optional", False) is True
            ):
                logger.debug(f"Column {column_name} not found inside the data.")
                return False

        # do not check for comments because they are NULLable anyways

    return True


def check_tags(tags: List[Any], database_name: str, table_name: str) -> bool:
    """Checks whether or not all the given tags are unique and valid.

    Args:
        tags (dict): The tags to check.
        database_name (str): The name of the database that contains the table whose entry is being tagged.
        table_name (str): The name of the table whose entry is being tagged.

    Returns:
        bool: Whether or not the given tags are a valid set of tags or not.
    """
    tags_schema = set(get_all_tags(database_name, table_name))

    for tag_id in tags:
        if not isinstance(tag_id, int) and (
            not isinstance(tag_id, str) or not tag_id.isdigit()
        ):
            logger.debug(f'Tag "{tag_id}" is invalid.')
            return False

        if int(tag_id) not in tags_schema:
            logger.debug(f'Tag ID "{tag_id}" was not found.')
            return False

        tags_schema.remove(int(tag_id))

    return True
