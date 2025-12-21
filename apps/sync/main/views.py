from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import HttpResponse, JsonResponse, HttpRequest, HttpResponseBadRequest
from typing import List
import yaml
import json
import re

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

# datatype checking functions
DATATYPE_CHECK: dict = {
    "int": lambda x: isinstance(x, int),
    "integer": lambda x: isinstance(x, int),
    "float": lambda x: isinstance(x, (int, float)),
    "number": lambda x: isinstance(x, (int, float)),
    "string": lambda x: x is not None,
    "str": lambda x: x is not None,
    "text": lambda x: x is not None,
    "bool": lambda x: x in ("true", "false", True, False),
    "boolean": lambda x: x in ("true", "false", True, False),

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

        return JsonResponse({"ok": True})
    
    return JsonResponse({"detail": "csrf cookie set"})