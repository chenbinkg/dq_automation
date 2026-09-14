"""
Sync PostgreSQL tables from development to production, with following actions supported:
    - "ensure-dev": ensure dev environment have the necessary tables and schema.
    - "ensure-prod": ensure prod environment have the necessary tables and schema.  
    - "migrate-dev-to-prod": migrate data from dev environment to prod environment.
    - "validate-dev-to-prod": validate data consistency between dev and prod environments.
    Additional options:
        --batch-size <number>: specify the batch size for migrating tables.
        --insert-only: insert missing primary keys without updating existing PROD records.
    
Ensure below environment variables are set for both DEV and PROD environments in env.env file
DEV_DB_HOST=dev-postgres-host
DEV_DB_PORT=5432
DEV_DB_NAME=dev-database
DEV_DB_USER=dev-user
DEV_DB_PASSWORD=dev-password
DEV_DB_SSLMODE=require

PROD_DB_HOST=prod-postgres-host
PROD_DB_PORT=5432
PROD_DB_NAME=prod-database
PROD_DB_USER=prod-user
PROD_DB_PASSWORD=prod-password
PROD_DB_SSLMODE=require

Run it by explicitly loading .env.dev into the shell, 
or rename/copy it to .env so load_dotenv() finds it automatically.

run bash command:

set -a
source .env.dev
set +a
python postgre_io.py migrate-dev-to-prod

"""

from __future__ import annotations

from typing import Any, Iterable, Sequence
import logging
import os
import psycopg2.extras as extras
from psycopg2 import sql
from postgres_io import PostgresSettings, build_settings, connect
from postgres_io import _clean_value, ensure_tables

logger = logging.getLogger(__name__)

MIGRATION_TABLES = (
    "public.dqm_dashboard_history_apac",
    "public.dqm_dashboard_global_position",
    "public.dqm_business_unit_mapping",
    "public.dqm_dataset_definitions",
    "public.dqm_dataset_custom_rule_details",
)

def settings_from_prefixed_environment(
    prefix: str,
    table: str = "public.dqm_dashboard_history_apac",
) -> PostgresSettings:
    """
    Build PostgreSQL settings from prefixed environment variables.

    Example prefixes:
        DEV
        PROD

    Expected environment variables:
        DEV_DB_HOST
        DEV_DB_PORT
        DEV_DB_NAME
        DEV_DB_USER
        DEV_DB_PASSWORD
        DEV_DB_SSLMODE

        PROD_DB_HOST
        PROD_DB_PORT
        PROD_DB_NAME
        PROD_DB_USER
        PROD_DB_PASSWORD
        PROD_DB_SSLMODE
    """
    prefix = prefix.upper().rstrip("_")

    variable_names = {
        "host": f"{prefix}_DB_HOST",
        "port": f"{prefix}_DB_PORT",
        "dbname": f"{prefix}_DB_NAME",
        "user": f"{prefix}_DB_USER",
        "password": f"{prefix}_DB_PASSWORD",
        "sslmode": f"{prefix}_DB_SSLMODE",
    }

    settings = build_settings(
        host=os.getenv(variable_names["host"]),
        port=os.getenv(variable_names["port"], "5432"),
        dbname=os.getenv(variable_names["dbname"]),
        user=os.getenv(variable_names["user"]),
        password=os.getenv(variable_names["password"]),
        table=table,
        sslmode=os.getenv(variable_names["sslmode"], "require"),
    )

    if settings is None:
        required_variables = (
            variable_names["host"],
            variable_names["dbname"],
            variable_names["user"],
            variable_names["password"],
        )

        missing_variables = [
            variable
            for variable in required_variables
            if not os.getenv(variable)
        ]

        raise ValueError(
            f"{prefix} PostgreSQL settings are incomplete. "
            f"Missing environment variables: {', '.join(missing_variables)}"
        )

    return settings


def _split_table_name(table_name: str) -> tuple[str, str]:
    """
    Split schema-qualified table name.

    Example:
        public.dqm_dashboard_history_apac
        -> ("public", "dqm_dashboard_history_apac")
    """
    parts = table_name.split(".")

    if len(parts) == 1:
        return "public", parts[0]

    if len(parts) == 2:
        return parts[0], parts[1]

    raise ValueError(
        f"Invalid table name {table_name!r}. "
        "Expected 'table' or 'schema.table'."
    )


def _get_table_columns(
    conn,
    table_name: str,
) -> list:
    """Return table columns in their defined ordinal order."""
    schema_name, unqualified_table_name = _split_table_name(table_name)

    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(
            query,
            (schema_name, unqualified_table_name),
        )
        columns = [row[0] for row in cur.fetchall()]

    if not columns:
        raise ValueError(
            f"Table {table_name!r} does not exist or has no columns."
        )

    return columns


def _get_primary_key_columns(
    conn,
    table_name: str,
) -> list:
    """Return primary-key columns in primary-key position order."""
    schema_name, unqualified_table_name = _split_table_name(table_name)

    query = """
        SELECT attribute.attname
        FROM pg_index AS index_definition
        JOIN pg_class AS table_definition
          ON table_definition.oid = index_definition.indrelid
        JOIN pg_namespace AS namespace_definition
          ON namespace_definition.oid = table_definition.relnamespace
        JOIN unnest(index_definition.indkey)
             WITH ORDINALITY AS key_column(attnum, key_position)
          ON TRUE
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = table_definition.oid
         AND attribute.attnum = key_column.attnum
        WHERE namespace_definition.nspname = %s
          AND table_definition.relname = %s
          AND index_definition.indisprimary
        ORDER BY key_column.key_position
    """

    with conn.cursor() as cur:
        cur.execute(
            query,
            (schema_name, unqualified_table_name),
        )
        key_columns = [row[0] for row in cur.fetchall()]

    if not key_columns:
        raise ValueError(
            f"Table {table_name!r} does not have a primary key. "
            "A primary key is required for upsert migration."
        )

    return key_columns


def _validate_migration_table(
    source_conn,
    target_conn,
    table_name: str,
) -> tuple[list[str], list[str]]:
    """
    Validate that source and target tables have matching columns and keys.

    Returns:
        tuple of:
            all columns
            primary-key columns
    """
    source_columns = _get_table_columns(source_conn, table_name)
    target_columns = _get_table_columns(target_conn, table_name)

    if source_columns != target_columns:
        source_only = [
            column
            for column in source_columns
            if column not in target_columns
        ]
        target_only = [
            column
            for column in target_columns
            if column not in source_columns
        ]

        raise ValueError(
            f"Column mismatch for {table_name}. "
            f"DEV-only columns: {source_only}. "
            f"PROD-only columns: {target_only}. "
            f"DEV order: {source_columns}. "
            f"PROD order: {target_columns}."
        )

    source_keys = _get_primary_key_columns(source_conn, table_name)
    target_keys = _get_primary_key_columns(target_conn, table_name)

    if source_keys != target_keys:
        raise ValueError(
            f"Primary-key mismatch for {table_name}. "
            f"DEV keys: {source_keys}. "
            f"PROD keys: {target_keys}."
        )

    return source_columns, target_keys


def migrate_table(
    source_settings: PostgresSettings,
    target_settings: PostgresSettings,
    table_name: str,
    batch_size: int = 1000,
    update_existing: bool = False,
) -> int:
    """
    Migrate one PostgreSQL table from source to target.

    Behaviour:
        - New primary keys are inserted.
        - Existing primary keys are updated when update_existing=True.
        - Existing primary keys are skipped when update_existing=False.
        - Target rows absent from source are not deleted.

    Returns:
        Number of source rows processed.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    schema_name, unqualified_table_name = _split_table_name(table_name)

    source_table = sql.Identifier(
        schema_name,
        unqualified_table_name,
    )
    target_table = sql.Identifier(
        schema_name,
        unqualified_table_name,
    )

    processed_rows = 0

    with connect(source_settings) as source_conn:
        with connect(target_settings) as target_conn:
            columns, key_columns = _validate_migration_table(
                source_conn=source_conn,
                target_conn=target_conn,
                table_name=table_name,
            )

            update_columns = [
                column
                for column in columns
                if column not in key_columns
            ]

            quoted_columns = sql.SQL(", ").join(
                sql.Identifier(column)
                for column in columns
            )

            quoted_keys = sql.SQL(", ").join(
                sql.Identifier(column)
                for column in key_columns
            )

            if update_existing and update_columns:
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(column),
                        sql.Identifier(column),
                    )
                    for column in update_columns
                )

                conflict_clause = sql.SQL(
                    "ON CONFLICT ({}) DO UPDATE SET {}"
                ).format(
                    quoted_keys,
                    set_clause,
                )
            else:
                conflict_clause = sql.SQL(
                    "ON CONFLICT ({}) DO NOTHING"
                ).format(quoted_keys)

            insert_query = sql.SQL(
                """
                INSERT INTO {} ({})
                VALUES %s
                {}
                """
            ).format(
                target_table,
                quoted_columns,
                conflict_clause,
            )

            select_query = sql.SQL(
                "SELECT {} FROM {}"
            ).format(
                quoted_columns,
                source_table,
            )

            cursor_name = (
                "migration_"
                + unqualified_table_name.replace("-", "_")
            )

            with source_conn.cursor(name=cursor_name) as source_cur:
                source_cur.itersize = batch_size
                source_cur.execute(select_query)

                with target_conn.cursor() as target_cur:
                    while True:
                        rows = source_cur.fetchmany(batch_size)

                        if not rows:
                            break

                        cleaned_rows = [
                            tuple(_clean_value(value) for value in row)
                            for row in rows
                        ]

                        extras.execute_values(
                            target_cur,
                            insert_query.as_string(target_conn),
                            cleaned_rows,
                            page_size=batch_size,
                        )

                        processed_rows += len(cleaned_rows)

                        logger.info(
                            "Migrated %d rows so far for %s.",
                            processed_rows,
                            table_name,
                        )

            target_conn.commit()

    logger.info(
        "Completed migration for %s. Source rows processed: %d.",
        table_name,
        processed_rows,
    )

    return processed_rows


def migrate_tables(
    source_settings: PostgresSettings,
    target_settings: PostgresSettings,
    table_names: Sequence[str] = MIGRATION_TABLES,
    batch_size: int = 1000,
    update_existing: bool = True,
    ensure_target_tables: bool = True,
) -> dict[str, int]:
    """
    Migrate multiple tables from a source PostgreSQL environment to a target.

    Each table is committed separately. If one table fails, previously
    completed tables remain committed.

    Returns:
        Mapping of table name to source rows processed.
    """
    if source_settings.host == target_settings.host:
        logger.warning(
            "Source and target hosts are identical: %s",
            source_settings.host,
        )

    if (
        source_settings.host == target_settings.host
        and source_settings.port == target_settings.port
        and source_settings.dbname == target_settings.dbname
    ):
        raise ValueError(
            "Source and target point to the same PostgreSQL database. "
            "Migration stopped to prevent accidental self-migration."
        )

    if ensure_target_tables:
        logger.info("Ensuring target tables exist before migration.")
        ensure_tables(
            settings=target_settings,
            seed_global_position=False,
        )

    migration_results: dict[str, int] = {}

    for table_name in table_names:
        logger.info(
            "Starting migration for %s.",
            table_name,
        )

        processed_rows = migrate_table(
            source_settings=source_settings,
            target_settings=target_settings,
            table_name=table_name,
            batch_size=batch_size,
            update_existing=update_existing,
        )

        migration_results[table_name] = processed_rows

    logger.info(
        "Completed migration of %d tables.",
        len(migration_results),
    )

    return migration_results


def get_table_row_count(
    settings: PostgresSettings,
    table_name: str,
) -> int:
    """Return the number of records in a table."""
    schema_name, unqualified_table_name = _split_table_name(table_name)

    query = sql.SQL(
        "SELECT COUNT(*) FROM {}"
    ).format(
        sql.Identifier(
            schema_name,
            unqualified_table_name,
        )
    )

    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()

    return int(result[0])


def validate_migration_counts(
    source_settings: PostgresSettings,
    target_settings: PostgresSettings,
    table_names: Sequence[str] = MIGRATION_TABLES,
) -> dict[str, dict]:
    """
    Validate that the number of rows in the source and target tables match.

    Returns:
        Mapping of table name to a dictionary with 'source' and 'target' row counts.
    """
    migration_counts: dict[str, dict] = {}

    for table_name in table_names:
        source_count = get_table_row_count(source_settings, table_name)
        target_count = get_table_row_count(target_settings, table_name)

        migration_counts[table_name] = {
            "source": source_count,
            "target": target_count,
        }

        if source_count != target_count:
            logger.warning(
                "Row count mismatch for table %s: source=%d, target=%d",
                table_name,
                source_count,
                target_count,
            )

    return migration_counts


def main() -> None:
    """
    Main entry point for the PostgreSQL DQM table setup and migration utility.

    Actions supported:
        - "ensure-dev": ensure dev environment have the necessary tables and schema.
        - "ensure-prod": ensure prod environment have the necessary tables and schema.
        - "migrate-dev-to-prod": migrate data from dev environment to prod environment.
        - "validate-dev-to-prod": validate data consistency between dev and prod environments.
    Batch size for migrating tables can be specified using the --batch-size argument.
    The --insert-only flag can be used to insert missing primary keys without updating existing PROD records.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="PostgreSQL DQM table setup and migration utility."
    )

    parser.add_argument(
        "action",
        choices=(
            "ensure-dev",
            "ensure-prod",
            "migrate-dev-to-prod",
            "validate-dev-to-prod",
        ),
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--insert-only",
        action="store_true",
        help=(
            "Insert missing primary keys but do not update existing "
            "PROD records."
        ),
    )

    args = parser.parse_args()

    dev_settings = settings_from_prefixed_environment("DEV")
    prod_settings = settings_from_prefixed_environment("PROD")

    if args.action == "ensure-dev":
        ensure_tables(settings=dev_settings)

    elif args.action == "ensure-prod":
        ensure_tables(settings=prod_settings)

    elif args.action == "migrate-dev-to-prod":
        results = migrate_tables(
            source_settings=dev_settings,
            target_settings=prod_settings,
            batch_size=args.batch_size,
            update_existing=not args.insert_only,
            ensure_target_tables=True,
        )

        for table_name, processed_rows in results.items():
            logger.info(
                "%s: %d DEV rows processed.",
                table_name,
                processed_rows,
            )

        validate_migration_counts(
            source_settings=dev_settings,
            target_settings=prod_settings,
        )

    elif args.action == "validate-dev-to-prod":
        validate_migration_counts(
            source_settings=dev_settings,
            target_settings=prod_settings,
        )


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    main()
