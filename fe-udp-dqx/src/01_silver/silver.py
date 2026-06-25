# Databricks notebook source
# =====================================================================
# STEP 1: INFRASTRUCTURE SETUP & SPECIFIC CONFIG ROUTING
# =====================================================================

import os
import uuid
import yaml
from datetime import datetime
from pyspark import pipelines as dp  # Lakeflow SDP Standard Module
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

# Initialize global platform tools
ws = WorkspaceClient()
generator = DQGenerator(workspace_client=ws, spark=spark)

# Fully decoupled environment catalogs/schemas
GLOBAL_BRONZE_CATALOG = spark.conf.get("pipeline.bronze_catalog")
GLOBAL_BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema")

GLOBAL_SILVER_CATALOG = spark.conf.get("pipeline.silver_catalog")
GLOBAL_SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema")

GLOBAL_QUARANTINE_CATALOG = spark.conf.get("pipeline.quarantine_catalog")
GLOBAL_QUARANTINE_SCHEMA = spark.conf.get("pipeline.quarantine_schema")

GLOBAL_METRICS_CATALOG = spark.conf.get("pipeline.metrics_catalog")
GLOBAL_METRICS_SCHEMA = spark.conf.get("pipeline.metrics_schema")

SOURCE_SYSTEM = spark.conf.get("pipeline.source_system")

# Map file system base directories dynamically
notebook_path = (
    dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
BASE_DIR = os.path.join("/Workspace", project_root.lstrip("/"))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "silver.yml")

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
    f"[CONFIG] Active Streaming Pipeline loaded {len(tables_dictionary)} tables from: silver.yml [{SOURCE_SYSTEM}]"
)


# =====================================================================
# STEP 2: THE STREAMING PIPELINE FACTORY
# =====================================================================


def build_silver_pipeline(table_config_name, table_configuration):

    source_table_name = table_configuration.get("source_table", table_config_name)
    target_table_name = table_configuration.get("target_table", table_config_name)
    source_system_name = table_configuration.get("source_system")
    exclude_columns_raw = table_configuration.get("exclude_columns", "")

    contract_file_path = os.path.join(
        BASE_DIR,
        "contracts",
        "01_silver",
        source_system_name,
        f"{source_system_name}.yml",
    )
    sql_file_path = os.path.join(
        BASE_DIR,
        "src",
        "01_silver",
        "sql",
        source_system_name,
        f"{table_config_name}.sql",
    )

    # ─────────────────────────────────────────────────────────────────
    # ─── DQX: DYNAMIC RULE COMPILATION ─────────────────────────────────
    # DQX method that reads a simple ODCS-compliant YAML contract file and compiles
    # it into programmatic execution rules on the fly, completely
    # decoupling data quality constraints from the core execution code.
    # ─────────────────────────────────────────────────────────────────
    if not os.path.exists(contract_file_path):
        raise FileNotFoundError(f"Contract file not found: {contract_file_path}")

    all_rules = generator.generate_rules_from_contract(
        contract_file=contract_file_path,
        generate_predefined_rules=False,
        generate_schema_validation=True,
        strict_schema_validation=False,
        process_text_rules=False,
        default_criticality="error",
    )

    rules = [
        rule
        for rule in all_rules
        if rule.get("user_metadata", {}).get("schema") == table_config_name
    ]

    print(
        f"[DQX] Table {table_config_name}: using {len(rules)} rules out of {len(all_rules)} total rules from {contract_file_path}"
    )

    if not rules:
        raise ValueError(
            f"No DQX rules found for table '{table_config_name}' in contract '{contract_file_path}'. "
            f"Check that schema.name in the ODCS contract exactly matches the table config name."
        )

    with open(sql_file_path, "r") as sql_file_handle:
        raw_sql_query = sql_file_handle.read().strip()

    if (
        not raw_sql_query
        or raw_sql_query.startswith("--")
        and len(raw_sql_query.splitlines()) == 1
    ):
        raise ValueError(
            f"CRITICAL: The SQL asset file at '{sql_file_path}' appears to be empty."
        )

    local_stream_identifier = f"local_stream_{table_config_name}"
    # Replace the __bronze__ placeholder with the local stream temp view name
    # Convention: SQL files use __bronze__.table_name as a stable,
    # environment-agnostic reference to the upstream bronze layer.
    transformed_streaming_sql = raw_sql_query.replace(
        f"__bronze__.{source_table_name}", local_stream_identifier
    )

    # ─── 1. COMBINED INCREMENTAL STREAM & TRANSFORMATION LAYER ───
    @dp.temporary_view(name=f"stg_{table_config_name}")
    def stg_view(
        bronze_table_name=source_table_name,
        bronze_catalog=GLOBAL_BRONZE_CATALOG,
        bronze_schema=GLOBAL_BRONZE_SCHEMA,
        sql_expression=transformed_streaming_sql,
        local_view_identifier=local_stream_identifier,
        columns_to_exclude=exclude_columns_raw,
    ):
        stream_df = spark.readStream.table(
            f"{bronze_catalog}.{bronze_schema}.{bronze_table_name}"
        )
        stream_df.createOrReplaceTempView(local_view_identifier)
        transformed_df = spark.sql(sql_expression)

        if columns_to_exclude:
            columns_to_drop = [
                col.strip() for col in columns_to_exclude.split(",") if col.strip()
            ]
            transformed_df = transformed_df.drop(*columns_to_drop)
        return transformed_df

    # ─── 2. TARGET: SILVER STREAMING TABLE (CLEAN DATA TARGET) ───
    @dp.table(
        name=f"{GLOBAL_SILVER_CATALOG}.{GLOBAL_SILVER_SCHEMA}.{target_table_name}",
        comment=f"Core streaming table containing validated {table_config_name} records passing contract constraints.",
    )
    def silver_table(pipeline_table_name=table_config_name, validation_rules=rules):
        engine = DQEngine(ws)
        transformed_df = spark.readStream.table(f"stg_{pipeline_table_name}")

        # ─────────────────────────────────────────────────────────────
        # ─── DQX: NATIVE STREAM SPLITTING (GOOD ROW PATH) ────────────
        # `good_df` automatically retains all fully valid rows as well
        # as rows that only triggered a non-blocking "warning".
        # ─────────────────────────────────────────────────────────────
        good_df, _ = engine.apply_checks_by_metadata_and_split(
            transformed_df, validation_rules
        )
        return good_df

    # ─── 3. TARGET: QUARANTINE INCREMENTAL STREAMING TABLE (BAD ROW TARGET) ───
    @dp.table(
        name=f"{GLOBAL_QUARANTINE_CATALOG}.{GLOBAL_QUARANTINE_SCHEMA}.{target_table_name}_quarantine",
        comment=f"Streaming quarantine table storing invalid {table_config_name} records violating contract constraints.",
    )
    def quarantine_table(pipeline_table_name=table_config_name, validation_rules=rules):
        engine = DQEngine(ws)
        transformed_df = spark.readStream.table(f"stg_{pipeline_table_name}")

        # ─────────────────────────────────────────────────────────────
        # ─── DQX: NATIVE STREAM SPLITTING (BAD ROW PATH) ─────────────
        # `bad_df` isolates records failing "error" or "critical" checks,
        # routing them away from the production Silver layer for auditing.
        # ─────────────────────────────────────────────────────────────
        _, bad_df = engine.apply_checks_by_metadata_and_split(
            transformed_df, validation_rules
        )
        return bad_df

    # ─── 4. INTERMEDIATE TEMPORARY VIEW: METRICS STAGING LAYER ───
    @dp.temporary_view(name=f"stg_metrics_{table_config_name}")
    def summary_metrics(
        bronze_table_name=source_table_name,
        validation_rules=rules,
        original_sql_query=raw_sql_query,
        bronze_catalog=GLOBAL_BRONZE_CATALOG,
        bronze_schema=GLOBAL_BRONZE_SCHEMA,
        columns_to_exclude=exclude_columns_raw,
        silver_table_name=target_table_name,
    ):
        from databricks.labs.dqx.metrics_observer import DQMetricsObserver

        # ─────────────────────────────────────────────────────────────
        # ─── DQX: THE METRICS OBSERVER AGGREGATOR ──────────────────────
        # Streaming micro-batches process asynchronously in the background,
        # preventing direct inline metric access. This view targets a batch
        # snapshot to initialize an observer, run the validation rules,
        # and safely extract data quality summary metrics.
        # ─────────────────────────────────────────────────────────────
        metrics_observer = DQMetricsObserver()
        dq_engine_with_observer = DQEngine(ws, observer=metrics_observer)

        # Replace __bronze__ placeholder with fully qualified bronze path
        resolved_batch_sql = original_sql_query.replace(
            f"__bronze__.{bronze_table_name}",
            f"{bronze_catalog}.{bronze_schema}.{bronze_table_name}",
        )

        transformed_df = spark.sql(resolved_batch_sql)

        if columns_to_exclude:
            columns_to_drop = [
                col.strip() for col in columns_to_exclude.split(",") if col.strip()
            ]
            transformed_df = transformed_df.drop(*columns_to_drop)

        good_df, _, metrics_observation = (
            dq_engine_with_observer.apply_checks_by_metadata_and_split(
                transformed_df, validation_rules
            )
        )

        # ─────────────────────────────────────────────────────────────
        # ─── DQX: FORCING LAZY EVALUATION ────────────────────────────────
        # Spark execution is lazy. Triggering an action like `.count()`
        # forces evaluation and populates the observer accumulators
        # before the metrics ledger is read.
        # ─────────────────────────────────────────────────────────────
        _ = good_df.count()

        observed_data = metrics_observation.get
        run_id = observed_data.get("run_id", str(uuid.uuid4()))
        run_time = datetime.now()

        output_location = (
            f"{GLOBAL_SILVER_CATALOG}.{GLOBAL_SILVER_SCHEMA}.{silver_table_name}"
        )
        quarantine_location = f"{GLOBAL_QUARANTINE_CATALOG}.{GLOBAL_QUARANTINE_SCHEMA}.{silver_table_name}_quarantine"

        # ─────────────────────────────────────────────────────────────
        # ─── DQX: PER-RULE FAILURE BREAKDOWN (JSON) ────────────────────
        # Captures standard numeric counters along with 'check_metrics',
        # a JSON array detailing exactly which rules failed and their
        # respective error/warning counts. 'metric_value' is typed as a
        # string to accommodate this rich JSON payload.
        # ─────────────────────────────────────────────────────────────
        metric_records = [
            (
                output_location,
                quarantine_location,
                run_id,
                run_time,
                "input_row_count",
                str(int(observed_data.get("input_row_count", 0))),
            ),
            (
                output_location,
                quarantine_location,
                run_id,
                run_time,
                "valid_row_count",
                str(int(observed_data.get("valid_row_count", 0))),
            ),
            (
                output_location,
                quarantine_location,
                run_id,
                run_time,
                "error_row_count",
                str(int(observed_data.get("error_row_count", 0))),
            ),
            (
                output_location,
                quarantine_location,
                run_id,
                run_time,
                "warning_row_count",
                str(int(observed_data.get("warning_row_count", 0))),
            ),
            (
                output_location,
                quarantine_location,
                run_id,
                run_time,
                "check_metrics",
                str(observed_data.get("check_metrics", "[]")),
            ),
        ]

        schema_str = "output_location string, quarantine_location string, run_id string, run_time timestamp, metric_name string, metric_value string"
        return spark.createDataFrame(metric_records, schema_str)


# =====================================================================
# STEP 3: THE AUTOMATED PIPELINE GENERATOR LOOP
# =====================================================================
for config_table_name, table_specific_config in tables_dictionary.items():
    build_silver_pipeline(config_table_name, table_specific_config)


# =====================================================================
# STEP 4: SYSTEM-LEVEL QUALITY METRICS
# =====================================================================
@dp.materialized_view(
    name=f"{GLOBAL_METRICS_CATALOG}.{GLOBAL_METRICS_SCHEMA}.{SOURCE_SYSTEM}_quality_metrics",
    comment=f"Consolidated DQX logging ledger aggregating statistics for the {SOURCE_SYSTEM} pipeline.",
)
def aggregate_system_metrics():
    schema_str = "output_location string, quarantine_location string, run_id string, run_time timestamp, metric_name string, metric_value string"

    if not tables_dictionary:
        return spark.createDataFrame([], schema_str)

    master_df = None
    for current_table_name in tables_dictionary.keys():
        table_metrics_df = spark.read.table(f"stg_metrics_{current_table_name}")
        if master_df is None:
            master_df = table_metrics_df
        else:
            master_df = master_df.unionAll(table_metrics_df)

    return master_df

