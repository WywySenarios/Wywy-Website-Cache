import psycopg
import sys
from typing import Literal, Optional


def run_sql(
    sql: str,
    database_name: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: Optional[str] = None,
):
    """
    Execute SQL query against a PostgreSQL database with autocommit enabled.

    Args:
        sql: SQL query to execute
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
    """
    conn_string = f"host={host} port={port} dbname={database_name} user={user}"
    if password:
        conn_string += f" password={password}"

    try:
        # Connect with autocommit enabled
        with psycopg.connect(conn_string, autocommit=True) as conn:
            with conn.cursor() as cur:
                # Execute the SQL
                cur.execute(sql)

                # Try to fetch results if it's a SELECT query
                if cur.description:
                    results = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]

                    # Print column names
                    print(" | ".join(columns))
                    print("-" * (sum(len(col) for col in columns) + len(columns) * 3))

                    # Print rows
                    for row in results:
                        print(" | ".join(str(val) for val in row))

                    print(f"\n{len(results)} row(s) returned")
                else:
                    # For INSERT, UPDATE, DELETE, etc.
                    print(f"Query executed successfully. Rows affected: {cur.rowcount}")

    except psycopg.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


DB_CONFIG: dict[Literal["host", "port", "database", "user", "password"], str | int] = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "password",  # or None if no password
}

if __name__ == "__main__":
    database_name = "wywywebsite"

    # You can also pass SQL via command line argument
    # if len(sys.argv) > 1:
    #     sql_query = sys.argv[1]
    if len(sys.argv) < 1:
        raise ValueError("Invalid number of arguments.")

    database_name: str = sys.argv[1]

    while True:
        sql_query = input("Query: ")
        print(f"Executing query:\n{sql_query}\n")
        run_sql(sql_query, database_name, **DB_CONFIG)
