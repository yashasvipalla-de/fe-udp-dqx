# Databricks notebook source
# DBTITLE 1,Gold Layer — Materialized Views from Silver
# =====================================================================
# GOLD LAYER — MATERIALIZED VIEWS (NO DQX VALIDATION)
# What's happening: This notebook reads SQL transformation files from
# src/02_gold/sql/<source_system>/<table>.sql and registers each as a
# materialized view in the gold catalog and schema. Unlike silver, there
# are no DQX rules, no quarantine tables, and no metrics — just clean
# business-ready transformations promoted from silver.
# =====================================================================

import os
import yaml
from pyspark import pipelines as dp  # Lakeflow SDP Standard Module

# ─── INFRASTRUCTURE PARAMETERS ───
GLOBAL_SILVER_CATALOG = spark.conf.get("pipeline.silver_catalog")
GLOBAL_SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema")

GLOBAL_GOLD_CATALOG = spark.conf.get("pipeline.gold_catalog")
GLOBAL_GOLD_SCHEMA = spark.conf.get("pipeline.gold_schema")

SOURCE_SYSTEM = spark.conf.get("pipeline.source_system")

# Map file system base directories dynamically
notebook_path = (
    dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
BASE_DIR = os.path.join("/Workspace", project_root.lstrip("/"))

# Load gold config
CONFIG_PATH = os.path.join(BASE_DIR, "config", "gold.yml")

if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as config_file_handle:
        source_config = yaml.safe_load(config_file_handle) or {}
        raw_tables = source_config.get("tables", {}).get(SOURCE_SYSTEM, {}) or {}
        tables_dictionary = {
            table_name: {**table_cfg, "source_system": SOURCE_SYSTEM}
            for table_name, table_cfg in raw_tables.items()
        }
else:
    raise FileNotFoundError(
        f"Expected configuration profile asset not found at: {CONFIG_PATH}"
    )

print(
    f"[CONFIG] Gold Pipeline loaded {len(tables_dictionary)} tables from: gold.yml [{SOURCE_SYSTEM}]"
)


# =====================================================================
# MATERIALIZED VIEW FACTORY
# What's happening: For each table in the config, we read the
# corresponding SQL file and register a materialized view that
# selects from silver with business-level transformations applied.
# =====================================================================


def build_gold_pipeline(table_config_name, table_configuration):

    source_table_name = table_configuration.get("source_table", table_config_name)
    target_table_name = table_configuration.get("target_table", table_config_name)
    source_system_name = table_configuration.get("source_system")

    sql_file_path = os.path.join(
        BASE_DIR,
        "src",
        "02_gold",
        "sql",
        source_system_name,
        f"{table_config_name}.sql",
    )

    with open(sql_file_path, "r") as sql_file_handle:
        raw_sql_query = sql_file_handle.read().strip()

    if (
        not raw_sql_query
        or raw_sql_query.startswith("--")
        and len(raw_sql_query.splitlines()) == 1
    ):
        raise ValueError(
            f"CRITICAL: The SQL asset file at '{sql_file_path}' appears to be empty or unsaved."
        )

    # Replace the __silver__ placeholder with fully qualified silver path
    # Convention: SQL files use __silver__.table_name as a stable,
    # environment-agnostic reference to the upstream silver layer.
    resolved_sql = raw_sql_query.replace(
        f"__silver__.{source_table_name}",
        f"{GLOBAL_SILVER_CATALOG}.{GLOBAL_SILVER_SCHEMA}.{source_table_name}",
    )

    # ─── MATERIALIZED VIEW: GOLD BUSINESS TABLE ───
    @dp.materialized_view(
        name=f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{target_table_name}",
        comment=f"Gold materialized view presenting business-ready {table_config_name} data promoted from silver.",
    )
    def gold_view(sql_expression=resolved_sql):
        return spark.sql(sql_expression)


# =====================================================================
# PIPELINE EXECUTION — REGISTER ALL CONFIGURED GOLD TABLES
# =====================================================================

for table_name, table_config in tables_dictionary.items():
    build_gold_pipeline(table_name, table_config)
    print(f"[GOLD] Registered materialized view: {table_name}")


