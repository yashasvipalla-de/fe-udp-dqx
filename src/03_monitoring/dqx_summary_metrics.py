# Databricks notebook source
# DBTITLE 1,Cell 1
# Databricks notebook source
# DBTITLE 1,Global Governance Ingestion Rollup
# =====================================================================
# GLOBAL DATA QUALITY
# =====================================================================

from pyspark import pipelines as dp

GLOBAL_METRICS_CATALOG = spark.conf.get("pipeline.metrics_catalog", "sandbox")
GLOBAL_METRICS_SCHEMA = spark.conf.get("pipeline.metrics_schema", "quality_monitoring")

# Discover all per-system quality metrics tables already materialized in the schema
tables_df = spark.sql(
    f"SHOW TABLES IN {GLOBAL_METRICS_CATALOG}.{GLOBAL_METRICS_SCHEMA}"
)
active_metrics_tables = [
    row.tableName
    for row in tables_df.collect()
    if row.tableName.endswith("_quality_metrics")
    and row.tableName != "global_quality_metrics"
]

print(
    f"[GLOBAL METRICS] Discovered {len(active_metrics_tables)} source metrics table(s): {active_metrics_tables}"
)


@dp.materialized_view(
    name=f"{GLOBAL_METRICS_CATALOG}.{GLOBAL_METRICS_SCHEMA}.global_quality_metrics",
    comment="Enterprise master data quality log aggregating stats across all system ingestion pipelines.",
)
def compile_global_dashboard_source(
    metrics_tables=active_metrics_tables,
    metrics_catalog=GLOBAL_METRICS_CATALOG,
    metrics_schema=GLOBAL_METRICS_SCHEMA,
):
    # ─────────────────────────────────────────────────────────────────
    # ─── DQX: CENTRALIZED ENTERPRISE OBSERVABILITY ───────────────────
    # Aggregates metrics across all downstream ingestion pipelines
    # (e.g., Oracle, Salesforce, SAP) into a single centralized ledger.
    # Because 'metric_value' is standardized as a string, it supports
    # unified enterprise-wide dashboarding over a consistent schema.
    # ─────────────────────────────────────────────────────────────────
    schema_str = "output_location string, quarantine_location string, run_id string, run_time timestamp, metric_name string, metric_value string"

    if not metrics_tables:
        return spark.createDataFrame([], schema_str)

    master_df = None
    for table_name in metrics_tables:
        df = spark.read.table(f"{metrics_catalog}.{metrics_schema}.{table_name}")
        master_df = df if master_df is None else master_df.unionAll(df)

    return master_df



