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
import re
import yaml
from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ---------------------------------------------------------------------
# Infrastructure parameters
# ---------------------------------------------------------------------

GLOBAL_SILVER_CATALOG = spark.conf.get("pipeline.silver_catalog")
GLOBAL_SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema")

GLOBAL_GOLD_CATALOG = spark.conf.get("pipeline.gold_catalog")
GLOBAL_GOLD_SCHEMA = spark.conf.get("pipeline.gold_schema")

SOURCE_SYSTEM = spark.conf.get("pipeline.source_system")

# ---------------------------------------------------------------------
# Resolve project root
# ---------------------------------------------------------------------

notebook_path = (
    dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

project_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
BASE_DIR = os.path.join("/Workspace", project_root.lstrip("/"))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "gold.yml")

# ---------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------

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
        f"Expected gold configuration file not found: {CONFIG_PATH}"
    )

print(
    f"[CONFIG] Gold Pipeline loaded {len(tables_dictionary)} tables from gold.yml [{SOURCE_SYSTEM}]"
)

if not tables_dictionary:
    raise ValueError(
        f"No gold tables found for source_system='{SOURCE_SYSTEM}' in {CONFIG_PATH}"
    )


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def safe_identifier(value):
    return re.sub(r"[^A-Za-z0-9_]", "_", value).lower()


def get_source_tables(table_config_name, table_configuration):
    source_tables = table_configuration.get("source_tables")

    if source_tables:
        return source_tables

    return [
        table_configuration.get("source_table", table_config_name)
    ]


def read_sql_file(source_system_name, sql_file_name):
    sql_file_path = os.path.join(
        BASE_DIR,
        "src",
        "02_gold",
        "sql",
        source_system_name,
        sql_file_name,
    )

    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"Gold SQL file not found: {sql_file_path}")

    with open(sql_file_path, "r") as sql_file_handle:
        raw_sql_query = sql_file_handle.read().strip()

    if not raw_sql_query:
        raise ValueError(f"Gold SQL file is empty: {sql_file_path}")

    forbidden_commands = ["TRUNCATE ", "INSERT ", "MERGE ", "DELETE ", "UPDATE ", "DROP ", "CREATE "]

    upper_sql = raw_sql_query.upper()

    for command in forbidden_commands:
        if command in upper_sql:
            raise ValueError(
                f"Gold SQL file must be SELECT-only for Lakeflow. "
                f"Found forbidden command '{command.strip()}' in {sql_file_path}"
            )

    return raw_sql_query, sql_file_path


def resolve_silver_placeholders_batch(raw_sql_query, source_table_names, sql_file_path):
    resolved_sql = raw_sql_query

    for source_table_name in source_table_names:
        resolved_sql = resolved_sql.replace(
            f"__silver__.{source_table_name}",
            f"`{GLOBAL_SILVER_CATALOG}`.`{GLOBAL_SILVER_SCHEMA}`.`{source_table_name}`",
        )

    if "__silver__." in resolved_sql:
        raise ValueError(
            f"Unresolved __silver__ placeholder found in SQL file: {sql_file_path}. "
            f"Configured source tables are: {source_table_names}"
        )

    return resolved_sql


def build_streaming_sql_from_silver(raw_sql_query, table_config_name, source_table_names):
    local_stream_identifiers = {
        source_table_name: (
            f"local_stream_gold_"
            f"{safe_identifier(table_config_name)}_"
            f"{safe_identifier(source_table_name)}"
        )
        for source_table_name in source_table_names
    }

    transformed_sql = raw_sql_query

    for source_table_name, local_stream_identifier in local_stream_identifiers.items():
        transformed_sql = transformed_sql.replace(
            f"__silver__.{source_table_name}",
            local_stream_identifier,
        )

    if "__silver__." in transformed_sql:
        raise ValueError(
            f"Unresolved __silver__ placeholder for table '{table_config_name}'. "
            f"Configured source tables are: {source_table_names}"
        )

    return transformed_sql, local_stream_identifiers

def build_singlestore_view(table_config_name, table_configuration):

    source_system_name = table_configuration.get("source_system")
    target_table_name = table_configuration.get("target_table", table_config_name)
    sql_file_name = table_configuration.get("sql_file", f"{table_config_name}.sql")

    source_table_names = get_source_tables(table_config_name, table_configuration)

    raw_sql_query, sql_file_path = read_sql_file(
        source_system_name,
        sql_file_name,
    )

    resolved_sql = resolve_silver_placeholders_batch(
        raw_sql_query,
        source_table_names,
        sql_file_path,
    )

    @dp.materialized_view(
        name=f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{target_table_name}",
        comment=f"Gold SingleStore view conversion for {table_config_name}",
    )
    def gold_singlestore_view(sql_expression=resolved_sql):
        return spark.sql(sql_expression)
    
def build_delta_type1(table_config_name, table_configuration):

    source_system_name = table_configuration.get("source_system")
    target_table_name = table_configuration.get("target_table", table_config_name)
    sql_file_name = table_configuration.get("sql_file", f"{table_config_name}.sql")

    source_table_names = get_source_tables(table_config_name, table_configuration)

    keys = table_configuration.get("keys")
    sequence_by = table_configuration.get("sequence_by")
    except_columns = table_configuration.get("except_columns", [])

    if not keys:
        raise ValueError(
            f"delta_type1 operation requires keys for table '{table_config_name}'"
        )

    if not sequence_by:
        raise ValueError(
            f"delta_type1 operation requires sequence_by for table '{table_config_name}'"
        )

    raw_sql_query, sql_file_path = read_sql_file(
        source_system_name,
        sql_file_name,
    )

    streaming_sql, local_stream_identifiers = build_streaming_sql_from_silver(
        raw_sql_query,
        table_config_name,
        source_table_names,
    )

    staging_view_name = f"stg_gold_type1_{table_config_name}"

    @dp.temporary_view(name=staging_view_name)
    def gold_type1_source(
        silver_table_names=source_table_names,
        silver_catalog=GLOBAL_SILVER_CATALOG,
        silver_schema=GLOBAL_SILVER_SCHEMA,
        sql_expression=streaming_sql,
        local_view_identifiers=local_stream_identifiers,
    ):
        for silver_table_name in silver_table_names:
            stream_df = spark.readStream.table(
                f"{silver_catalog}.{silver_schema}.{silver_table_name}"
            )

            stream_df.createOrReplaceTempView(
                local_view_identifiers[silver_table_name]
            )

        return spark.sql(sql_expression)

    target_full_name = (
        f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{target_table_name}"
    )

    dp.create_streaming_table(
        name=target_full_name,
        comment=f"Gold Delta Type 1 table for {table_config_name}",
    )

    dp.create_auto_cdc_flow(
        target=target_full_name,
        source=staging_view_name,
        keys=keys,
        sequence_by=col(sequence_by),
        except_column_list=except_columns,
        stored_as_scd_type=1,
    )

def build_gold_pipeline(table_config_name, table_configuration):

    operation_type = table_configuration.get(
        "operation_type",
        "singlestore_view",
    )

    if operation_type == "singlestore_view":
        build_singlestore_view(table_config_name, table_configuration)

    elif operation_type == "delta_type1":
        build_delta_type1(table_config_name, table_configuration)

    else:
        raise ValueError(
            f"Unsupported operation_type='{operation_type}' for table '{table_config_name}'"
        )


for table_name, table_config in tables_dictionary.items():
    build_gold_pipeline(table_name, table_config)
    print(
        f"[GOLD] Registered operation_type={table_config.get('operation_type')} for table={table_name}"
    )
