from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import pipeline_io
import postgres_io


DB_CREDENTIALS = {
    "host": "db.example",
    "port": 5432,
    "dbname": "dq",
    "user": "dq_user",
    "password": "secret",
}


class PipelineIOPostgresTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace(
            BU_MAPPING_DB_TABLE="public.dqm_business_unit_mapping"
        )

    def make_io(self, **kwargs) -> pipeline_io.PipelineIO:
        return pipeline_io.PipelineIO(
            write_mode="postgres",
            config_module=self.config,
            db_credentials=DB_CREDENTIALS,
            **kwargs,
        )

    def test_snapshot_and_special_table_names(self) -> None:
        io = self.make_io()

        self.assertEqual(
            io._postgres_table_name("dataset_rule_details"),
            "public.dqm_auto_dataset_rule_details",
        )
        self.assertEqual(
            io._postgres_table_name("business_unit_mapping"),
            "public.dqm_business_unit_mapping",
        )

    def test_stage_table_override_prevents_snapshot_collision(self) -> None:
        io = self.make_io(
            postgres_table_overrides={
                "dataset_custom_rules": "public.dqm_auto_jjdmc_dataset_custom_rules"
            }
        )

        self.assertEqual(
            io._postgres_table_name("dataset_custom_rules"),
            "public.dqm_auto_jjdmc_dataset_custom_rules",
        )

    @patch("pipeline_io.postgres_io.replace_table", return_value=1)
    @patch("pipeline_io.postgres_io.upsert_business_unit_mapping", return_value=1)
    def test_write_dispatches_by_output_policy(self, mock_upsert, mock_replace) -> None:
        io = self.make_io()
        frame = pd.DataFrame([{"dataset": "ds_one"}])

        io.write_output(frame, "business_unit_mapping")
        io.write_output(frame, "dataset_runid")

        mock_upsert.assert_called_once()
        mock_replace.assert_called_once()
        self.assertEqual(
            mock_replace.call_args.args[1].table,
            "public.dqm_auto_dataset_runid",
        )

    @patch("pipeline_io.postgres_io.read_table")
    def test_read_uses_postgres_snapshot(self, mock_read_table) -> None:
        expected = pd.DataFrame([{"dataset": "ds_one"}])
        mock_read_table.return_value = expected

        actual = self.make_io().read_input("dataset_runid")

        self.assertIs(actual, expected)
        self.assertEqual(
            mock_read_table.call_args.args[0].table,
            "public.dqm_auto_dataset_runid",
        )


class FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.last_statement = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, statement, params=None) -> None:
        self.last_statement = str(statement)
        self.statements.append(self.last_statement)

    def fetchone(self):
        return ("public.dqm_auto_dataset_runid",)

    def fetchall(self):
        if "information_schema.columns" in self.last_statement:
            return [("dataset",), ("run_date",)]
        return []


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self) -> FakeCursor:
        return self.fake_cursor


class PostgresReplaceTests(unittest.TestCase):
    @patch("postgres_io.upsert_dataframe", return_value=1)
    def test_s1_bu_upsert_preserves_s2_only_columns(self, mock_upsert) -> None:
        settings = postgres_io.PostgresSettings(
            host="db.example",
            port=5432,
            dbname="dq",
            user="dq_user",
            password="secret",
            table="public.dqm_business_unit_mapping",
        )
        frame = pd.DataFrame(
            [{"dataset": "ds_one", "region": "apac", "business_unit": "IM"}]
        )

        postgres_io.upsert_business_unit_mapping(frame, settings)

        self.assertEqual(
            mock_upsert.call_args.kwargs["all_columns"],
            ("dataset", "region", "business_unit"),
        )
        self.assertNotIn("db_nm", mock_upsert.call_args.kwargs["all_columns"])

    @patch("postgres_io.extras.execute_values")
    @patch("postgres_io.connect")
    def test_replace_stages_rows_before_truncating(self, mock_connect, mock_execute_values) -> None:
        cursor = FakeCursor()
        mock_connect.return_value = FakeConnection(cursor)
        settings = postgres_io.PostgresSettings(
            host="db.example",
            port=5432,
            dbname="dq",
            user="dq_user",
            password="secret",
            table="public.dqm_auto_dataset_runid",
        )
        frame = pd.DataFrame(
            [{"dataset": "ds_one", "run_date": "2026-10-05T00:00:00Z"}]
        )

        written = postgres_io.replace_table(frame, settings)

        self.assertEqual(written, 1)
        mock_execute_values.assert_called_once()
        self.assertFalse(
            any(statement.startswith("CREATE SCHEMA") for statement in cursor.statements)
        )
        truncate_index = next(
            index
            for index, statement in enumerate(cursor.statements)
            if statement.startswith("TRUNCATE TABLE")
        )
        create_stage_index = next(
            index
            for index, statement in enumerate(cursor.statements)
            if statement.startswith("CREATE TEMP TABLE")
        )
        self.assertLess(create_stage_index, truncate_index)
        self.assertTrue(cursor.statements[-1].startswith("INSERT INTO"))


if __name__ == "__main__":
    unittest.main()
