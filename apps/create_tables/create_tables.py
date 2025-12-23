"""Helper script to create PostgreSQL tables based on the config.yml file.
@TODO write to stderr on errors, and figure out warnings, too
"""
# imports
from os import environ as env
from typing import List
import re
import psycopg2
import yaml

BASE_URL = "wywywebsite-cache_database"

# Constants
RESERVEDCOLUMNNAMES = ["id", "user", "users"]
PSQLDATATYPES = {
    "int": "integer",
    "integer": "integer",
    "float": "real",
    "number": "real",
    "double": "double precision",
    "str": "text",
    "string": "text",
    "text": "text",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "date",
    "time": "time",
    "timestamp": "timestamp",
}

psycopg2config: dict = {
    "host": BASE_URL,
    "port": env["POSTGRES_PORT"],
    "user": env["DB_USERNAME"],
    "password": env["DB_PASSWORD"],
}

# peak at config
with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

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

def add_info_table() -> None:
    conn = psycopg2.connect(**psycopg2config)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    
    # create info db if necessary
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT FROM pg_database WHERE datname = %s);", ("info",))
            dbExists = cur.fetchone()[0]
            
            if not dbExists:
                try:
                    cur.execute("CREATE DATABASE info;")
                except psycopg2.errors.DuplicateDatabase:
                    pass
    finally:
        conn.close()
    
    # create the info table
    with psycopg2.connect(host=BASE_URL, port=env["POSTGRES_PORT"], user=env["DB_USERNAME"], password=env["DB_PASSWORD"], dbname="info") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'sync_status');")
            tableExists = cur.fetchone()[0]
            
            if tableExists:
                print("Table \"sync_status\" already exists in database \"info\"; skipping creation.")
            else:
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_type WHERE typname = 'sync_status_enum'
                        ) THEN
                            CREATE TYPE sync_status_enum AS ENUM (
                                'already exists',
                                'added',
                                'mismatch',
                                'failed',
                                'updated'
                            );
                        END IF;
                    END
                    $$;

                    CREATE TABLE sync_status (
                        id BIGSERIAL PRIMARY KEY,
                        table_name TEXT NOT NULL,
                        sync_timestamp TIMESTAMPTZ NULL,
                        status sync_status_enum NULL
                    );
                """)

if __name__ == "__main__":
    print("Attempting to create tables based on config.yml...")
    # loop through every database that has tables to be created
    for dbInfo in config["data"]:
        dbInfo["dbname"] = to_lower_snake_case(dbInfo["dbname"])

        psycopg2config.pop("dbname", None)
        
        # check if the table already exists
        # @TODO reduce the number of with statements
        conn = psycopg2.connect(**psycopg2config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT FROM pg_database WHERE datname = %s);", (dbInfo["dbname"],))
                dbExists = cur.fetchone()[0]
                
                if not dbExists:
                    try:
                        cur.execute("CREATE DATABASE " + dbInfo["dbname"] + ";")
                    except psycopg2.errors.DuplicateDatabase:
                        pass
        finally:
            conn.close()

        psycopg2config["dbname"] = dbInfo["dbname"]

        # loop through every table that needs to be created @TODO verify config validity to avoid errors
        for tableInfo in dbInfo.get("tables", []):
            # convert to lower_snake_case
            tableInfo["tableName"] = to_lower_snake_case(tableInfo["tableName"])

            # skip any already created tables without raising any issues
            with psycopg2.connect(**psycopg2config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '" + tableInfo["tableName"] + "');")
                    tableExists = cur.fetchone()[0]
                    
                    if tableExists:
                        print("Table \"", tableInfo["tableName"], "\" already exists in database \"", dbInfo["dbname"] + "\"; skipping creation.", sep="")
                        continue
            
            # look for prereqs:
            valid = True # innocent until proven guilty
            # it has a name
            if not "tableName" in tableInfo or tableInfo["tableName"] is None or len(tableInfo["tableName"]) == 0:
                print("Tables must have a non-empty name specified in key \"tableName\"")
                valid = False
                
            # there are 1+ columns
            if not "schema" in tableInfo or not (type(tableInfo["schema"]) is List or type(tableInfo["schema"]) is list) or len(tableInfo["schema"]) < 1 or not tableInfo["schema"]:
                print("Table", tableInfo["tableName"], "must have a column schema containing at least 1 column of data to store.")
                valid = False
            
            # ensure that column names will not interfere with comments in the Astro schema (see apps/astro-app/src/components/data/data-entry-form.tsx)
            if "schema" in tableInfo:
                for columnInfo in tableInfo["schema"]:
                    # we may guarentee that the column has a name because of earlier checks
                    if columnInfo.get("name")[-9:] == "_comments":
                        print("Table", tableInfo["tableName"], "must not have a column with a name that ends in \"_comments\".")

            # avoid reserved columns
            if "schema" in tableInfo:
                for i in RESERVEDCOLUMNNAMES:
                    if i in tableInfo["schema"]:
                        print("Table", tableInfo["tableName"], "must not have a column named \"" + i + "\", which is a reserved column name.")
                        valid = False
            # @todo it has a name that will be recognized by the PostgresSQL database & server

            if not valid:
                print("Skipping creation of table", tableInfo.get("tableName", "???"), "due to schema violation(s).")
                continue
            
            with psycopg2.connect(**psycopg2config) as conn:
                with conn.cursor() as cur:
                    # @TODO create a function to modify constraints rather than create new tables
                    currentCommand: str = "CREATE TABLE " + tableInfo["tableName"] + " ("

                    # since SQL cannot tolerate trailing commas, I will add the primary key last and never give it a comma.
                    # add in all of the columns
                    for columnInfo in tableInfo["schema"]:
                        # @TODO verify column validity
                        if not "name" in columnInfo:
                            print("Skipping a column in table", tableInfo["tableName"], "due to missing \"name\" key in column schema.")
                        
                        columnDisplayName = to_snake_case(columnInfo["name"])
                        
                        # add in the column's name & datatype @TODO validate datatype
                        currentCommand += columnDisplayName + " " + PSQLDATATYPES[columnInfo["datatype"]] + ","
                        
                        # check out constraints
                        if columnInfo.get("unique", False) == True:
                            currentCommand += "CONSTRAINT " + columnDisplayName + "_unique UNIQUE(" + columnDisplayName + "),"
                        if columnInfo.get("optional", True) == False:
                            currentCommand += "CONSTRAINT " + columnDisplayName + "_optional NOT NULL,"
                        # @TODO CHECK (REGEX, number comparisons)
                        whitelist: list = columnInfo.get("whitelist", [])
                        if whitelist is list and len(whitelist) > 0: # @TODO datatype validation
                            currentCommand += "CONSTRAINT " + columnDisplayName + "_whitelist CHECK (" + columnDisplayName + " IN ("
                            for item in whitelist:
                                if item is None: break # do NOT deal with null values.
                                currentCommand += "\'" + str(item) + "\',"
                            
                            # strip trailing comma & add closing brackets
                            currentCommand = currentCommand[:-1] + ")),"
                        # comments
                        if columnInfo.get("comments", False):
                            currentCommand += columnDisplayName + "_comments text,"
                        # @TODO foreign keys
                        # @TODO defaults
                        
                    # add in ID column
                    currentCommand += "id SERIAL PRIMARY KEY"
                    
                    currentCommand += ")"
                    
                    # try to execute the command
                    cur.execute(currentCommand)
                    # print(currentCommand)
    
    add_info_table()
    print("Finished creating tables.")