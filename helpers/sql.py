import psycopg
import sys
from typing import Optional

def run_sql(
    sql: str,
    host: str = "localhost",
    port: int = 5432,
    database: str = "postgres",
    user: str = "postgres",
    password: Optional[str] = None
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
    conn_string = f"host={host} port={port} dbname={database} user={user}"
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


if __name__ == "__main__":
    database_name = "wywywebsite"
    
    # You can also pass SQL via command line argument
    if len(sys.argv) > 1:
        sql_query = sys.argv[1]
    if len(sys.argv) > 2:
        database_name = sys.argv[2]
    
    DB_CONFIG = {
        "host": "localhost",
        "port": 5433,
        "database": database_name,
        "user": "postgres",
        "password": "password"  # or None if no password
    }
    
    print(f"Executing query:\n{sql_query}\n")
    run_sql(sql_query, **DB_CONFIG)