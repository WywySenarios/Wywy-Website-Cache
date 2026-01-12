# datatype checking functions
import re
import yaml
from utils import to_lower_snake_case, get_env_int
from os import environ as env

VERBOSITY_LEVEL = get_env_int("SCHEMA_CHECK_VERBOSITY", 0)

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
            **table,
            "schema": {
                to_lower_snake_case(column_schema["name"]): column_schema
                for column_schema in table["schema"]
            },

            "descriptors": {
                to_lower_snake_case(descriptor_schema["name"]): {
                    "schema": {
                        to_lower_snake_case(column_schema["name"]): column_schema
                        for column_schema in descriptor_schema["schema"]
                    }
                }
                for descriptor_schema in table["descriptors"]
            } if "descriptors" in table else None,
        }
        for table in db["tables"]
    }
    for db in config["data"]
}

def check_entry(entry: dict, table_info: dict) -> bool:
    """Checks the given entry against the given table schema.

    Args:
        entry (dict): The entry to check.
        table_info (dict): The table schema to check against.

    Returns:
        bool: Whether or not the given entry conforms to the table schema.
    """

    if not "data" in entry or not check_item(entry["data"], table_info["schema"]):
        if VERBOSITY_LEVEL > 0:
            print("There is no data or the data is in an unexpected format.")
        return False

    # @TODO check tags
    if ("tags" in entry and ("tagging" not in table_info or table_info["tagging"] != True)):
        if VERBOSITY_LEVEL > 0:
            print(f"Tagging is disabled on table {table_info.name}.")
        return False
    elif ("tags" not in entry and ("tagging" in table_info and table_info["tagging"] == True)):
        if VERBOSITY_LEVEL > 0:
            print(f"Tagging is enabled on table {table_info.name}. You must provide at least one tag.")
        return False
    
    if "tags" in entry:
        if not isinstance(entry["tags"], list):
            if VERBOSITY_LEVEL > 0:
                print("An array of tags must be provided. This array can be empty.")
            return False
    
        for tag_id in entry["tags"]:
            if not (isinstance(tag_id, str) and tag_id.isdigit()) and not isinstance(tag_id, int):
                if VERBOSITY_LEVEL > 0:
                    print(f"Tag \"{tag_id}\" is invalid. Tag IDs must be provided rather than tag names.")
                return False

    # check descriptors
    # check if the descriptors exist when they should.
    if not ((not not ("descriptors" in entry)) == (not not ("descriptors" in table_info and table_info["descriptors"]))):
        return False
    
    if "descriptors" in entry:
        # check each descriptor individually
        # iterate through each descriptor category
        for descriptor_name in entry["descriptors"]:
            # verify if this descriptor is in the config
            if not descriptor_name in table_info["descriptors"]:
                if VERBOSITY_LEVEL > 0:
                    print(f"Descriptor type \"{descriptor_name}\" was not found.")
                return False

            if not isinstance(entry["descriptors"][descriptor_name], list):
                if VERBOSITY_LEVEL > 0:
                    print(f"Descriptor(s) of type \"{descriptor_name}\" are not in an array. You must provide descriptors in arrays, even if there is only one descriptor.")
                return False

            # iterate through every descriptor
            for descriptor_entry in entry["descriptors"][descriptor_name]:
                if not isinstance(descriptor_entry, dict):
                    if VERBOSITY_LEVEL > 0:
                        print(f"The format of a {descriptor_name} descriptor is invalid.")
                    return False
                
                if not check_item(descriptor_entry, table_info["descriptors"][descriptor_name]["schema"]):
                    if VERBOSITY_LEVEL > 0:
                        print(f"A {descriptor_name} descriptor does not conform to the descriptor schema.")
                    return False
    
    return True


# @TODO verify every entry that is needed is inside
def check_item(data: dict, schema: dict) -> bool:
    """Checks the given data against the given schema.

    Args:
        data (dict): The data to check.
        schema (dict): The schema to check against.

    Returns:
        bool: Whether or not the data conforms to the schema.
    """
    # check every item
    for display_column_name in data:
        column_name = to_lower_snake_case(display_column_name)
        is_comments_column: bool = len(column_name) > 9 and column_name[-9:] == "_comments"
        
        # special logic for comments
        if is_comments_column:
            # check if this column is allowed to have comments
            if not schema[column_name[:-9]].get("comments", False):
                if VERBOSITY_LEVEL > 0:
                    print(f"Comments column {column_name} is related to a column that does not have commenting enabled.")
                return False
            
            if not DATATYPE_CHECK["str"](data[display_column_name]):
                if VERBOSITY_LEVEL > 0:
                    print(f"Comments column {column_name} must contain a string comment.")
                return False
            continue
        
        # special logic for primary_tag
        if column_name == "primary_tag":
            if not DATATYPE_CHECK["str"](data[display_column_name]):
                if VERBOSITY_LEVEL > 0:
                    print("The primary tag column does not have a non-empty tag.")
                return False
            continue
        
        # check if this is a valid column
        if not column_name in schema:
            if VERBOSITY_LEVEL > 0:
                print(f"Column \"{column_name}\" is not within the table's schema.")
            return False
        
        # check if the datatype is correct
        if not DATATYPE_CHECK[schema[column_name]["datatype"]](data[display_column_name]):
            if VERBOSITY_LEVEL > 0:
                print(f"Bad datatype {data[display_column_name]} {schema[column_name]["datatype"]}")
            return False
        
        # @TODO min/max, etc. checks
    return True

def check_tags(tags: List[str], database_name: str, table_name: str) -> bool:
    """Checks whether or not all the given tags are unique and valid.

    Args:
        tags (dict): The tags to check.
        database_name (str): The name of the database that contains the table whose entry is being tagged.
        table_name (str): The name of the table whose entry is being tagged.

    Returns:
        bool: Whether or not the given tags are a valid set of tags or not.
    """
    table = databases[database_name][table_name]
    return True