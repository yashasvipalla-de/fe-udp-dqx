# Databricks notebook source
# DBTITLE 1,Gold Layer — Materialized Views from Silver
# Databricks notebook source
# =====================================================================
# GOLD LAYER — MATERIALIZED VIEWS WITH OPTIONAL BUSINESS-LEVEL DQX
#
# What this does:
#   1. Reads config/gold.yml
#   2. Reads SQL files from src/02_gold/sql/<source_system>/<table>.sql
#   3. Creates Gold materialized views
#   4. Optionally applies Gold/business-level DQX rules
#   5. Sends failed Gold records to quarantine
#   6. Captures Gold DQ metrics
#
# Notes:
#   - sql_file is optional in gold.yml.
#     If not provided, this notebook uses <table_config_name>.sql.
#
#   - Gold DQX checks are optional per table:
#       apply_dqx: true
#
#   - Gold contracts live under:
#       contracts/02_gold/<source_system>/<contract_file>
#
#   - Gold contracts should only define important business columns/rules.
#     Full schema validation is intentionally disabled for Gold.
# =====================================================================

import os
import re
import uuid
import yaml
from datetime import datetime

from pyspark import pipelines as dp
from pyspark.sql.functions import col

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator


# =====================================================================
# STEP 1: PLATFORM / DQX INITIALIZATION
# =====================================================================

ws = WorkspaceClient()
generator = DQGenerator(workspace_client=ws, spark=spark)


# =====================================================================
# STEP 2: INFRASTRUCTURE PARAMETERS
# =====================================================================

GLOBAL_SILVER_CATALOG = spark.conf.get("pipeline.silver_catalog")
GLOBAL_SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema")

GLOBAL_GOLD_CATALOG = spark.conf.get("pipeline.gold_catalog")
GLOBAL_GOLD_SCHEMA = spark.conf.get("pipeline.gold_schema")

GLOBAL_QUARANTINE_CATALOG = spark.conf.get("pipeline.quarantine_catalog", "")
GLOBAL_QUARANTINE_SCHEMA = spark.conf.get("pipeline.quarantine_schema", "")

GLOBAL_METRICS_CATALOG = spark.conf.get("pipeline.metrics_catalog", "")
GLOBAL_METRICS_SCHEMA = spark.conf.get("pipeline.metrics_schema", "")

SOURCE_SYSTEM = spark.conf.get("pipeline.source_system")


# =====================================================================
# STEP 3: RESOLVE PROJECT ROOT
# =====================================================================

notebook_path = (
    dbutils.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

project_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
BASE_DIR = os.path.join("/Workspace", project_root.lstrip("/"))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "gold.yml")


# =====================================================================
# STEP 4: LOAD GOLD CONFIG
# =====================================================================

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
    f"[CONFIG] Gold Pipeline loaded {len(tables_dictionary)} table(s) from gold.yml [{SOURCE_SYSTEM}]"
)

if not tables_dictionary:
    raise ValueError(
        f"No gold tables found for source_system='{SOURCE_SYSTEM}' in {CONFIG_PATH}"
    )

# COMMAND ----------

# DBTITLE 1,Helper functions
# =====================================================================
# STEP 5: HELPER FUNCTIONS
# =====================================================================

def safe_identifier(value):
    return re.sub(r"[^A-Za-z0-9_]", "_", value).lower()


def render_sql_parameters(raw_sql_query, sql_parameters, sql_file_path):
    """
    Replaces placeholders like ${PARAM_NAME} using values from gold.yml.

    Example:
      ${HASH_BITS}    -> 256
      ${EMAIL_FILTER} -> EMAIL.EMAIL_STATUS = 'VALID'
    """

    resolved_sql = raw_sql_query
    sql_parameters = sql_parameters or {}

    for parameter_name, parameter_value in sql_parameters.items():
        placeholder = "${" + parameter_name + "}"
        resolved_sql = resolved_sql.replace(placeholder, str(parameter_value))

    unresolved_matches = re.findall(r"\$\{[A-Za-z0-9_]+\}", resolved_sql)

    if unresolved_matches:
        raise ValueError(
            f"Unresolved SQL parameter(s) {unresolved_matches} found in {sql_file_path}. "
            f"Configured sql_parameters are: {list(sql_parameters.keys())}"
        )

    return resolved_sql


def get_source_tables(table_config_name, table_configuration):
    """
    Resolves source table list.

    For multi-source Gold views:
      source_tables:
        - table_a
        - table_b

    For single-source Gold views:
      source_table: table_a

    If neither is provided, defaults to table_config_name.
    """

    source_tables = table_configuration.get("source_tables")

    if source_tables:
        return source_tables

    return [table_configuration.get("source_table", table_config_name)]


def read_sql_file(source_system_name, table_config_name, table_configuration):
    """
    Reads the Gold SQL file.

    If sql_file is not provided in gold.yml, defaults to:
      <table_config_name>.sql

    Example:
      table_config_name = customer_contact_summary
      sql file path     = src/02_gold/sql/customer/customer_contact_summary.sql
    """

    sql_file_name = table_configuration.get("sql_file", f"{table_config_name}.sql",)

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

    return raw_sql_query, sql_file_path


def resolve_silver_placeholders_batch(raw_sql_query, source_table_names, sql_file_path):
    """
    Replaces __silver__.table_name with the fully qualified Silver table.
    """

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


def resolve_gold_placeholders_batch(raw_sql_query, source_table_names, sql_file_path):
    """
    Replaces __gold__.table_name with the fully qualified Gold table.
    Useful if a Gold view depends on another Gold view/table.
    """

    resolved_sql = raw_sql_query

    for source_table_name in source_table_names:
        resolved_sql = resolved_sql.replace(
            f"__gold__.{source_table_name}",
            f"`{GLOBAL_GOLD_CATALOG}`.`{GLOBAL_GOLD_SCHEMA}`.`{source_table_name}`",
        )

    if "__gold__." in resolved_sql:
        raise ValueError(
            f"Unresolved __gold__ placeholder found in SQL file: {sql_file_path}. "
            f"Configured source tables are: {source_table_names}"
        )

    return resolved_sql

# COMMAND ----------

# DBTITLE 1,contracts
def get_gold_contract_path(source_system_name, table_configuration):
    """
    Resolves Gold contract path.

    Default:
      contracts/02_gold/<source_system>/<source_system>.yml

    If contract_file is provided:
      contracts/02_gold/<source_system>/<contract_file>
    """

    contract_file_name = table_configuration.get(
        "contract_file",
        f"{source_system_name}.yml",
    )

    return os.path.join(
        BASE_DIR,
        "contracts",
        "02_gold",
        source_system_name,
        contract_file_name,
    )


def load_gold_dqx_rules(table_config_name, table_configuration):
    """
    Loads selected business-level DQX rules for a Gold table.

    Important:
      generate_schema_validation=False

    This is intentional because Gold contracts should only validate selected
    business-critical columns, not every column in the output.
    """

    source_system_name = table_configuration.get("source_system")

    contract_file_path = get_gold_contract_path(
        source_system_name,
        table_configuration,
    )

    if not os.path.exists(contract_file_path):
        raise FileNotFoundError(
            f"Gold contract file not found for table '{table_config_name}': {contract_file_path}"
        )

    all_rules = generator.generate_rules_from_contract(
        contract_file=contract_file_path,
        generate_predefined_rules=False,
        generate_schema_validation=False,
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
        f"[DQX][GOLD] Table {table_config_name}: using {len(rules)} rule(s) "
        f"out of {len(all_rules)} total rule(s) from {contract_file_path}"
    )

    if not rules:
        raise ValueError(
            f"No Gold DQX rules found for table '{table_config_name}' in contract '{contract_file_path}'. "
            f"Check that schema.name in the Gold contract exactly matches the gold table config name."
        )

    return rules


def build_streaming_sql_from_silver(raw_sql_query, table_config_name, source_table_names):
    """
    Used by delta_type1 flow.

    Replaces __silver__.table_name with local streaming temp view names.
    """

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


def build_resolved_gold_sql(table_config_name, table_configuration):
    """
    Common SQL loading/rendering/resolution helper.
    """

    source_system_name = table_configuration.get("source_system")

    source_table_names = get_source_tables(
        table_config_name,
        table_configuration,
    )

    sql_parameters = table_configuration.get("sql_parameters", {}) or {}

    raw_sql_query, sql_file_path = read_sql_file(
        source_system_name,
        table_config_name,
        table_configuration,
    )

    parameterized_sql = render_sql_parameters(
        raw_sql_query,
        sql_parameters,
        sql_file_path,
    )

    resolved_sql = parameterized_sql

    if "__silver__." in resolved_sql:
        resolved_sql = resolve_silver_placeholders_batch(
            resolved_sql,
            source_table_names,
            sql_file_path,
        )

    if "__gold__." in resolved_sql:
        resolved_sql = resolve_gold_placeholders_batch(
            resolved_sql,
            source_table_names,
            sql_file_path,
        )

    return resolved_sql

# COMMAND ----------

# DBTITLE 1,apply_gold_dqx_gate
def apply_gold_dqx(
    table_config_name,
    table_configuration,
    input_view_name,
    target_table_name,
    is_streaming=False,
):
    """
    Applies Gold DQX as a reusable quality gate.

    If apply_dqx=false:
      - returns the original input_view_name

    If apply_dqx=true:
      - creates a valid-record temporary view
      - creates a quarantine table/materialized view
      - creates a metrics temporary view
      - returns the valid-record view name

    Downstream operations should consume the returned view name.
    """

    apply_dqx = bool(table_configuration.get("apply_dqx", False))

    if not apply_dqx:
        return input_view_name

    if not GLOBAL_QUARANTINE_CATALOG or not GLOBAL_QUARANTINE_SCHEMA:
        raise ValueError(
            f"Gold DQX is enabled for '{table_config_name}', "
            f"but quarantine catalog/schema configs are missing."
        )

    validation_rules = load_gold_dqx_rules(
        table_config_name,
        table_configuration,
    )

    safe_table_name = safe_identifier(table_config_name)

    valid_view_name = f"stg_gold_valid_{safe_table_name}"

    quarantine_full_name = (
        f"{GLOBAL_QUARANTINE_CATALOG}."
        f"{GLOBAL_QUARANTINE_SCHEMA}."
        f"{target_table_name}_gold_quarantine"
    )

    # -----------------------------------------------------------------
    # Valid records view
    # -----------------------------------------------------------------
    @dp.temporary_view(
        name=valid_view_name,
        comment=f"DQX-valid Gold staging view for {table_config_name}.",
    )
    def gold_dqx_valid_view(
        source_view_name=input_view_name,
        validation_rules_for_table=validation_rules,
        streaming=is_streaming,
    ):
        engine = DQEngine(ws)

        if streaming:
            staged_df = spark.readStream.table(source_view_name)
        else:
            staged_df = spark.read.table(source_view_name)

        good_df, _ = engine.apply_checks_by_metadata_and_split(
            staged_df,
            validation_rules_for_table,
        )

        return good_df

    # -----------------------------------------------------------------
    # Quarantine output
    # -----------------------------------------------------------------
    if is_streaming:

        @dp.table(
            name=quarantine_full_name,
            comment=f"Streaming Gold quarantine table for {table_config_name}.",
        )
        def gold_dqx_streaming_quarantine(
            source_view_name=input_view_name,
            validation_rules_for_table=validation_rules,
        ):
            engine = DQEngine(ws)

            staged_df = spark.readStream.table(source_view_name)

            _, bad_df = engine.apply_checks_by_metadata_and_split(
                staged_df,
                validation_rules_for_table,
            )

            return bad_df

    else:

        @dp.materialized_view(
            name=quarantine_full_name,
            comment=f"Gold quarantine materialized view for {table_config_name}.",
        )
        def gold_dqx_batch_quarantine(
            source_view_name=input_view_name,
            validation_rules_for_table=validation_rules,
        ):
            engine = DQEngine(ws)

            staged_df = spark.read.table(source_view_name)

            _, bad_df = engine.apply_checks_by_metadata_and_split(
                staged_df,
                validation_rules_for_table,
            )

            return bad_df

    # -----------------------------------------------------------------
    # Metrics staging view
    # -----------------------------------------------------------------
    @dp.temporary_view(
        name=f"stg_gold_metrics_{safe_table_name}",
        comment=f"Temporary Gold DQ metrics staging view for {table_config_name}.",
    )
    def gold_dqx_metrics_view(
        source_view_name=input_view_name,
        validation_rules_for_table=validation_rules,
        gold_table_name=target_table_name,
        quarantine_location=quarantine_full_name,
        streaming=is_streaming,
    ):
        from databricks.labs.dqx.metrics_observer import DQMetricsObserver

        schema_str = (
            "output_location string, "
            "quarantine_location string, "
            "run_id string, "
            "run_time timestamp, "
            "metric_name string, "
            "metric_value string"
        )

        # For streaming CDC paths, inline batch-style metrics are not reliable
        # because count() on a streaming DataFrame is not valid in the same way.
        # The DQX split still happens; this returns an empty metrics frame.
        if streaming:
            return spark.createDataFrame([], schema_str)

        metrics_observer = DQMetricsObserver()
        dq_engine_with_observer = DQEngine(ws, observer=metrics_observer)

        staged_df = spark.read.table(source_view_name)

        good_df, _, metrics_observation = (
            dq_engine_with_observer.apply_checks_by_metadata_and_split(
                staged_df,
                validation_rules_for_table,
            )
        )

        # Force Spark evaluation so observer metrics are populated.
        _ = good_df.count()

        observed_data = metrics_observation.get
        run_id = observed_data.get("run_id", str(uuid.uuid4()))
        run_time = datetime.now()

        output_location = (
            f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{gold_table_name}"
        )

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

        return spark.createDataFrame(metric_records, schema_str)

    return valid_view_name

# COMMAND ----------

# DBTITLE 1,build_materialized_view
# =====================================================================
# STEP 6: GOLD MATERIALIZED VIEW BUILDER
# =====================================================================

def build_materialized_view(table_config_name, table_configuration):

    target_table_name = table_configuration.get(
        "target_table",
        table_config_name,
    )

    resolved_sql = build_resolved_gold_sql(
        table_config_name,
        table_configuration,
    )

    staging_view_name = f"stg_gold_{safe_identifier(table_config_name)}"

    # -----------------------------------------------------------------
    # Raw Gold SQL output
    # -----------------------------------------------------------------
    @dp.temporary_view(
        name=staging_view_name,
        comment=f"Temporary Gold staging view for {table_config_name}.",
    )
    def gold_staging_view(sql_expression=resolved_sql):
        return spark.sql(sql_expression)

    # -----------------------------------------------------------------
    # Optional DQX gate
    # If apply_dqx=false, this returns staging_view_name unchanged.
    # If apply_dqx=true, this returns the valid-record DQX view.
    # -----------------------------------------------------------------
    final_source_view_name = apply_gold_dqx(
        table_config_name=table_config_name,
        table_configuration=table_configuration,
        input_view_name=staging_view_name,
        target_table_name=target_table_name,
        is_streaming=False,
    )

    # -----------------------------------------------------------------
    # Final Gold materialized view
    # -----------------------------------------------------------------
    @dp.materialized_view(
        name=f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{target_table_name}",
        comment=f"Gold materialized view for {table_config_name}.",
    )
    def gold_materialized_view(source_view_name=final_source_view_name):
        return spark.read.table(source_view_name)

# COMMAND ----------

# DBTITLE 1,build_delta_type1
# =====================================================================
# STEP 7: GOLD DELTA TYPE 1 BUILDER
# Creates an internal CDC streaming table and exposes a final materialized view
# =====================================================================

def build_delta_type1(table_config_name, table_configuration):
    """
    Builds a Gold Delta Type 1 flow where the final consumer-facing object
    is a materialized view.

    Flow:
      Silver streaming table
        -> temporary staging view
        -> internal CDC streaming table using create_auto_cdc_flow
        -> final Gold materialized view reading the internal CDC table

    Note:
      create_auto_cdc_flow requires a persistent streaming table target.
      Therefore the CDC backing table cannot be purely temporary.
    """

    source_system_name = table_configuration.get("source_system")

    target_table_name = table_configuration.get(
        "target_table",
        table_config_name,
    )

    # Internal backing table for create_auto_cdc_flow.
    # This is not intended to be the consumer-facing Gold object.
    cdc_target_table_name = table_configuration.get(
        "cdc_target_table",
        f"{target_table_name}_cdc_internal",
    )

    source_table_names = get_source_tables(
        table_config_name,
        table_configuration,
    )

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
        table_config_name,
        table_configuration,
    )

    parameterized_sql = render_sql_parameters(
        raw_sql_query,
        table_configuration.get("sql_parameters", {}) or {},
        sql_file_path,
    )

    streaming_sql, local_stream_identifiers = build_streaming_sql_from_silver(
        parameterized_sql,
        table_config_name,
        source_table_names,
    )

    staging_view_name = f"stg_gold_type1_{safe_identifier(table_config_name)}"

    @dp.temporary_view(
        name=staging_view_name,
        comment=f"Temporary streaming source view for Gold Type 1 table {table_config_name}.",
    )
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

    cdc_target_full_name = (
        f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{cdc_target_table_name}"
    )

    final_mv_full_name = (
        f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{target_table_name}"
    )

    # -----------------------------------------------------------------
    # Internal CDC streaming table
    # -----------------------------------------------------------------
    dp.create_streaming_table(
        name=cdc_target_full_name,
        comment=(
            f"Internal Delta Type 1 CDC backing table for {table_config_name}. "
            f"Consumers should query {final_mv_full_name}."
        ),
    )

    # -----------------------------------------------------------------
    # Optional DQX before CDC Type 1 apply
    # -----------------------------------------------------------------
    cdc_source_view_name = apply_gold_dqx(
        table_config_name=table_config_name,
        table_configuration=table_configuration,
        input_view_name=staging_view_name,
        target_table_name=target_table_name,
        is_streaming=True,
    )

    dp.create_auto_cdc_flow(
        target=cdc_target_full_name,
        source=cdc_source_view_name,
        keys=keys,
        sequence_by=col(sequence_by),
        except_column_list=except_columns,
        stored_as_scd_type=1,
    )

    # -----------------------------------------------------------------
    # Final consumer-facing Gold materialized view
    # -----------------------------------------------------------------
    @dp.materialized_view(
        name=final_mv_full_name,
        comment=(
            f"Gold materialized view for {table_config_name}, backed by an internal "
            f"Delta Type 1 CDC streaming table."
        ),
    )
    def gold_type1_materialized_view(
        cdc_table_full_name=cdc_target_full_name,
    ):
        return spark.read.table(cdc_table_full_name)

# COMMAND ----------

# DBTITLE 1,build_delta_type2
# =====================================================================
# STEP 7B: GOLD DELTA TYPE 2 / SCD TYPE 2 BUILDER
# Creates an internal SCD2 CDC streaming table and exposes a final materialized view
# =====================================================================

def build_delta_type2(table_config_name, table_configuration):
    """
    Builds a Gold Delta Type 2 / SCD Type 2 flow where the final consumer-facing
    object is a materialized view.

    Flow:
      Silver streaming table
        -> temporary staging view
        -> internal SCD2 CDC streaming table using create_auto_cdc_flow
        -> final Gold materialized view reading the internal SCD2 table

    Notes:
      - create_auto_cdc_flow requires a persistent streaming table target.
      - The internal CDC table cannot be a temporary view.
      - stored_as_scd_type=2 preserves history instead of overwriting old values.
    """

    source_system_name = table_configuration.get("source_system")

    target_table_name = table_configuration.get(
        "target_table",
        table_config_name,
    )

    # Internal backing table for create_auto_cdc_flow.
    # This is not intended to be the consumer-facing Gold object.
    cdc_target_table_name = table_configuration.get(
        "cdc_target_table",
        f"{target_table_name}_cdc_internal",
    )

    source_table_names = get_source_tables(
        table_config_name,
        table_configuration,
    )

    keys = table_configuration.get("keys")
    sequence_by = table_configuration.get("sequence_by")
    except_columns = table_configuration.get("except_columns", [])

    # Optional SCD2-specific controls.
    # If provided, these tell Databricks which columns should or should not
    # cause a new historical version.
    track_history_columns = table_configuration.get("track_history_columns")
    track_history_except_columns = table_configuration.get(
        "track_history_except_columns"
    )

    if not keys:
        raise ValueError(
            f"delta_type2 operation requires keys for table '{table_config_name}'"
        )

    if not sequence_by:
        raise ValueError(
            f"delta_type2 operation requires sequence_by for table '{table_config_name}'"
        )

    raw_sql_query, sql_file_path = read_sql_file(
        source_system_name,
        table_config_name,
        table_configuration,
    )

    parameterized_sql = render_sql_parameters(
        raw_sql_query,
        table_configuration.get("sql_parameters", {}) or {},
        sql_file_path,
    )

    streaming_sql, local_stream_identifiers = build_streaming_sql_from_silver(
        parameterized_sql,
        table_config_name,
        source_table_names,
    )

    staging_view_name = f"stg_gold_type2_{safe_identifier(table_config_name)}"

    @dp.temporary_view(
        name=staging_view_name,
        comment=f"Temporary streaming source view for Gold SCD Type 2 table {table_config_name}.",
    )
    def gold_type2_source(
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

    cdc_target_full_name = (
        f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{cdc_target_table_name}"
    )

    final_mv_full_name = (
        f"{GLOBAL_GOLD_CATALOG}.{GLOBAL_GOLD_SCHEMA}.{target_table_name}"
    )

    # -----------------------------------------------------------------
    # Internal SCD2 CDC streaming table
    # -----------------------------------------------------------------
    dp.create_streaming_table(
        name=cdc_target_full_name,
        comment=(
            f"Internal Delta Type 2 / SCD2 CDC backing table for {table_config_name}. "
            f"Consumers should query {final_mv_full_name}."
        ),
    )

    # -----------------------------------------------------------------
    # Optional DQX gate before CDC Type 2 apply
    # -----------------------------------------------------------------
    cdc_source_view_name = apply_gold_dqx(
        table_config_name=table_config_name,
        table_configuration=table_configuration,
        input_view_name=staging_view_name,
        target_table_name=target_table_name,
        is_streaming=True,
    )

    auto_cdc_arguments = {
        "target": cdc_target_full_name,
        "source": cdc_source_view_name,
        "keys": keys,
        "sequence_by": col(sequence_by),
        "except_column_list": except_columns,
        "stored_as_scd_type": 2,
    }

    if track_history_columns:
        auto_cdc_arguments["track_history_column_list"] = track_history_columns

    if track_history_except_columns:
        auto_cdc_arguments["track_history_except_column_list"] = (
            track_history_except_columns
        )

    dp.create_auto_cdc_flow(**auto_cdc_arguments)

    # -----------------------------------------------------------------
    # Final consumer-facing Gold materialized view
    # -----------------------------------------------------------------
    @dp.materialized_view(
        name=final_mv_full_name,
        comment=(
            f"Gold SCD Type 2 materialized view for {table_config_name}, backed by an "
            f"internal Delta Type 2 CDC streaming table."
        ),
    )
    def gold_type2_materialized_view(
        cdc_table_full_name=cdc_target_full_name,
    ):
        return spark.read.table(cdc_table_full_name)

# COMMAND ----------

# DBTITLE 1,build_gold_pipeline
# =====================================================================
# STEP 8: OPERATION ROUTER
# =====================================================================

def build_gold_pipeline(table_config_name, table_configuration):

    operation_type = table_configuration.get(
        "operation_type",
        "materialized_view",
    )

    if operation_type == "materialized_view":
        build_materialized_view(table_config_name, table_configuration)

    elif operation_type == "delta_type1":
        build_delta_type1(table_config_name, table_configuration)

    elif operation_type == "delta_type2":
        build_delta_type2(table_config_name, table_configuration)

    else:
        raise ValueError(
            f"Unsupported operation_type='{operation_type}' for table '{table_config_name}'"
        )

# =====================================================================
# STEP 9: AUTOMATED GOLD PIPELINE GENERATOR LOOP
# =====================================================================

for table_name, table_config in tables_dictionary.items():
    build_gold_pipeline(table_name, table_config)
    print(
        f"[GOLD] Registered operation_type={table_config.get('operation_type', 'materialized_view')} "
        f"for table={table_name}"
    )

# COMMAND ----------

# DBTITLE 1,aggregate_gold_quality_metrics
# =====================================================================
# STEP 10: SOURCE/DOMAIN-LEVEL GOLD QUALITY METRICS
# =====================================================================

@dp.materialized_view(
    name=f"{GLOBAL_METRICS_CATALOG}.{GLOBAL_METRICS_SCHEMA}.{SOURCE_SYSTEM}_gold_quality_metrics",
    comment=f"Consolidated Gold DQX metrics for the {SOURCE_SYSTEM} Gold pipeline.",
)
def aggregate_gold_quality_metrics():
    schema_str = (
        "output_location string, "
        "quarantine_location string, "
        "run_id string, "
        "run_time timestamp, "
        "metric_name string, "
        "metric_value string"
    )

    dqx_enabled_tables = [
        table_name
        for table_name, table_cfg in tables_dictionary.items()
        if bool(table_cfg.get("apply_dqx", False))
    ]

    if not dqx_enabled_tables:
        return spark.createDataFrame([], schema_str)

    master_df = None

    for current_table_name in dqx_enabled_tables:
        metrics_view_name = f"stg_gold_metrics_{safe_identifier(current_table_name)}"
        table_metrics_df = spark.read.table(metrics_view_name)

        if master_df is None:
            master_df = table_metrics_df
        else:
            master_df = master_df.unionAll(table_metrics_df)

    return master_df
