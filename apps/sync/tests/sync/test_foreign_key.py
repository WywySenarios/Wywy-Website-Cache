import unittest
import psycopg
from psycopg.rows import dict_row
from sync.sync import prepare_payload
from ..generic_database_api.transformations.purge import purge_database
from ..generic_database_api.transformations.populate import populate_transformation
from ..generic_database_api.transformations.transform import TransformTargets
from constants import CONN_CONFIG
from wywy_website_types.data import TableInfo
from config import CONFIG
from utils import to_lower_snake_case

MOCK_FOREIGN_ID = 2325


class TestForeignKeyInsertion(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        purge_database()

    def test_primary_tag_recognition(self):
        """Test if the correct tag id is set to be synchronized."""
        database_name: str = ""
        table_name: str = ""
        target_table_info: TableInfo | None = None
        # find a table with tagging
        for database_info in CONFIG["data"]:
            database_name = to_lower_snake_case(database_info["dbname"])
            for table_info in database_info["tables"]:
                if table_info.get("tagging", False) is True:
                    target_table_info = table_info
                    table_name = to_lower_snake_case(table_info["tableName"])
                    break

        if target_table_info is None:
            self.fail(
                "Unable to test primary_tag foreign key: no tables have primary_tag enabled."
            )

        target: TransformTargets = {}
        target[table_name] = ("data", target_table_info)
        target[f"{table_name}_tags"] = ("tags", None)
        target[f"{table_name}_tag_aliases"] = ("tag_aliases", None)
        target[f"{table_name}_tag_names"] = ("tag_names", None)
        target[f"{table_name}_tag_groups"] = ("tag_groups", None)

        with psycopg.connect(
            **CONN_CONFIG, dbname=database_name, row_factory=dict_row
        ) as data_conn, psycopg.connect(**CONN_CONFIG, dbname="info") as info_conn:
            # populate test values
            with data_conn.cursor() as data_cur, info_conn.cursor() as info_cur:
                populate_transformation(data_cur, target)

                info_cur.execute(
                    "INSERT INTO sync_status (table_name, parent_table_name, table_type, database_name, entry_id, remote_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL);",
                    (
                        f"{table_name}_tag_names",
                        table_name,
                        "tag_names",
                        database_name,
                        1,
                        MOCK_FOREIGN_ID,
                    ),
                )
                info_cur.execute(
                    "INSERT INTO sync_status (table_name, parent_table_name, table_type, database_name, entry_id, remote_id, sync_timestamp, status) VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL);",
                    (
                        table_name,
                        table_name,
                        "data",
                        database_name,
                        1,
                    ),
                )

        # expect one target with an updated foreign key (i.e. foreign key MOCK_FOREIGN_ID)

        # test payload production

        output = prepare_payload("1", database_name, table_name, table_name, "data")

        if output is None:
            self.assertIsNotNone(output)
            return
        else:
            payload = output[1]

        self.assertEqual(payload.get("primary_tag", None), MOCK_FOREIGN_ID)

        # @TODO test that no other targets are available (probably not important to test though, it's very hard for the cache to know)
