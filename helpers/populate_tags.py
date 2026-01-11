import psycopg

# Define the connection parameters (change these according to your setup)
HOST = 'localhost'
PORT = 5433
DATABASE = 'wywywebsite'
USER = 'postgres'
PASSWORD = 'password'

# SQL query you want to run
sql_query = """
BEGIN;
INSERT INTO events_tag_names (id, tag_name) values (1, 'a');
INSERT INTO events_tag_names (id, tag_name) values (2, 'b');
INSERT INTO events_tag_aliases (alias, tag_id) values ('a', 1);
INSERT INTO events_tag_aliases (alias, tag_id) values ('1', 1);
INSERT INTO events_tag_aliases (alias, tag_id) values ('b', 2);
INSERT INTO events_tag_aliases (alias, tag_id) values ('2', 2);
COMMIT;
"""

# Connect to the PostgreSQL database
with psycopg.connect(
    host=HOST,
    port=PORT,
    dbname=DATABASE,
    user=USER,
    password=PASSWORD
) as conn:
    # Create a cursor object to interact with the database
    with conn.cursor() as cur:
        # Execute the SQL query
        cur.execute(sql_query)

        # Fetch the results (if it's a SELECT query)
        # result = cur.fetchall()

        # Print the results
        # for row in result:
        #     print(row)

# Connection is automatically closed when the 'with' block is exited
