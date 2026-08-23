"""Shared input/output helpers for pipeline scripts."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd


class PipelineIO:
    """Read and write pipeline datasets from local CSV or Unity Catalog."""

    def __init__(
        self,
        *,
        write_mode: str = "csv",
        local_output_dir: str = "./outputs",
        dbutils: Any = None,
        spark: Any = None,
        config_module: Any = None,
        secret_scope: str = "collibra",
        uc_catalog: Optional[str] = None,
        uc_schema: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        sanitize_uc_table_names: bool = False,
    ) -> None:
        self.write_mode = str(write_mode).strip().lower()
        self.local_output_dir = local_output_dir
        self.dbutils = dbutils
        self.spark = spark
        self.config_module = config_module
        self.secret_scope = secret_scope
        self.logger = logger or logging.getLogger(__name__)
        self.sanitize_uc_table_names = sanitize_uc_table_names
        if self.running_on_databricks():
            self.write_mode = "uc"

        if self.write_mode not in ("csv", "uc", "both"):
            raise ValueError("PIPELINE_WRITE_MODE must be one of: csv, uc, both")

        self.uc_catalog = uc_catalog
        self.uc_schema = uc_schema
        self.uc_volume_path = f"/Volumes/{self.uc_catalog}/{self.uc_schema}" if self.uc_catalog and self.uc_schema else None
        if self.write_mode in ("uc", "both"):
            self.uc_catalog, self.uc_schema = self._resolve_uc_target()

    def running_on_databricks(self) -> bool:
        # Most reliable generic signal in Databricks runtime
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            return True
        # Extra fallback often present on cluster driver
        if os.getenv("DB_IS_DRIVER") == "TRUE":
            return True
        return False

    def _resolve_uc_target(self) -> tuple[Optional[str], Optional[str]]:
        if self.uc_catalog and self.uc_schema:
            return self.uc_catalog, self.uc_schema

        if self.dbutils is not None:
            try:
                return (
                    self.dbutils.secrets.get(scope=self.secret_scope, key="uc_catalog"),
                    self.dbutils.secrets.get(scope=self.secret_scope, key="uc_schema"),
                )
            except Exception as exc:
                self.logger.info("Falling back to config.py UC values: %s", exc)

        if self.config_module is not None:
            return (
                getattr(self.config_module, "UC_CATALOG", None),
                getattr(self.config_module, "UC_SCHEMA", None),
            )

        return None, None

    def output_name_from_file_name(self, file_name: str) -> str:
        """Convert a file name like foo.csv into the pipeline output name foo."""
        return Path(file_name).stem

    def read_from_csv(self, file_name: str) -> pd.DataFrame:
        """Read CSV from local outputs directory."""
        file_path = os.path.join(self.local_output_dir, f"{file_name}.csv")
        self.logger.info("Reading from CSV: %s", file_path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV not found: {file_path}")
        return pd.read_csv(file_path)

    def read_from_uc_table(self, table_name: str) -> pd.DataFrame:
        """Read from Unity Catalog table."""
        if not self.uc_catalog or not self.uc_schema:
            raise RuntimeError("UC_CATALOG/UC_SCHEMA not configured")
        if self.spark is None:
            raise RuntimeError("Spark session not found")

        full_table_name = f"{self.uc_catalog}.{self.uc_schema}.{table_name}"
        self.logger.info("Reading from UC table: %s", full_table_name)
        return self.spark.read.format("delta").table(full_table_name).toPandas()

    def read_input(self, table_name: str) -> pd.DataFrame:
        """Read input based on PIPELINE_WRITE_MODE flag."""
        if self.write_mode in ("csv", "both"):
            try:
                return self.read_from_csv(table_name)
            except FileNotFoundError:
                if self.write_mode == "csv":
                    raise
                self.logger.info("CSV not found, falling back to UC for %s", table_name)

        if self.write_mode in ("uc", "both"):
            if self.uc_volume_path:
                return self.read_from_uc_volume(table_name)
            return self.read_from_uc_table(table_name)

        raise ValueError(f"Unable to read {table_name}")

    def write_pandas_to_csv(self, df: pd.DataFrame, file_name: str) -> str:
        """Write DataFrame to local CSV."""
        os.makedirs(self.local_output_dir, exist_ok=True)
        output_path = os.path.join(self.local_output_dir, f"{file_name}.csv")
        df.to_csv(output_path, index=False)
        self.logger.info("Exported to CSV: %s", output_path)
        return output_path

    def write_pandas_to_uc_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Write DataFrame to UC Delta table."""
        if not self.uc_catalog or not self.uc_schema:
            raise RuntimeError("UC_CATALOG/UC_SCHEMA not configured")
        if self.spark is None:
            raise RuntimeError("Spark session not found")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.uc_catalog}.{self.uc_schema}")
        full_table_name = f"{self.uc_catalog}.{self.uc_schema}.{table_name}"
        spark_df = self.spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        self.logger.info("Exported to UC table: %s", full_table_name)
        return full_table_name

    # def write_output_to_uc_safe(self, df: pd.DataFrame, output_name: str) -> str:
    #     """Write to UC using a sanitized table name, while keeping the requested output label for CSV."""
    #     safe_table_name = output_name.lower().replace(" ", "_")
    #     return self.write_pandas_to_uc_table(df, safe_table_name)

    def write_output(
        self,
        df: pd.DataFrame,
        output_name: str
    ) -> None:
        """Write output based on PIPELINE_WRITE_MODE flag."""
        if self.write_mode in ("csv", "both"):
            self.write_pandas_to_csv(df, output_name)

        if self.sanitize_uc_table_names:
            safe_table_name = output_name.lower().replace(" ", "_")
        else:
            safe_table_name = output_name

        if self.write_mode in ("uc", "both"):
            if self.uc_volume_path:
                self.write_to_uc_volume(df, safe_table_name)
            else:
                self.write_pandas_to_uc_table(df, safe_table_name)

    def read_from_uc_volume(self, table_name: str) -> pd.DataFrame:
        """Read from Unity Catalog volume path."""
        if not self.uc_volume_path:
            raise RuntimeError("UC volume path not configured")
        full_table_name = f"{self.uc_volume_path}/{table_name}.csv"
        self.logger.info("Reading from UC volume path: %s", full_table_name)
        return pd.read_csv(full_table_name)

    def write_to_uc_volume(self, df: pd.DataFrame, table_name: str) -> str:
        """Write DataFrame to Unity Catalog volume path."""
        if not self.uc_volume_path:
            raise RuntimeError("UC volume path not configured")
        full_table_name = f"{self.uc_volume_path}/{table_name}.csv"
        df.to_csv(full_table_name, index=False)
        self.logger.info("Written to UC volume path: %s", full_table_name)
        return full_table_name
