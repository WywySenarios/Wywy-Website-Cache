# datatype checking functions
import re
import yaml
from utils import to_lower_snake_case


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

# @TODO verify every entry that is needed is inside
def validate_entry(data: dict, database_name: str, table_name: str) -> bool:
    table = databases[database_name][table_name]
    # check every item
    for display_column_name in data:
        column_name = to_lower_snake_case(display_column_name)
        is_comments_column: bool = len(column_name) > 9 and column_name[-9:] == "_comments"
        
        # special logic for comments
        if is_comments_column:
            # check if this column is allowed to have comments
            if not table["schema"][column_name[:-9]].get("comments", False):
                return False
            
            if not DATATYPE_CHECK["str"](data[display_column_name]):
                return False
            continue
        
        # check if this is a valid column
        if not column_name in table["schema"]:
            print("col name invalid")
            return False
        
        # check if the datatype is correct
        if not DATATYPE_CHECK[table["schema"][column_name]["datatype"]](data[display_column_name]):
            print(f"bad datatype {data[display_column_name]} {table["schema"][column_name]["datatype"]}")
            return False
        
        # @TODO min/max, etc. checks
    return True