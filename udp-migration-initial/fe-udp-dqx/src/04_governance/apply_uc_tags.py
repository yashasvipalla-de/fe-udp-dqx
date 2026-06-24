# Databricks notebook source
# DBTITLE 1,Apply Unity Catalog Tags
# ─── Parameters ───────────────────────────────────────────────────────────────
dbutils.widgets.text("layer", "", "Layer (silver/gold)")
dbutils.widgets.text("catalog", "", "Target catalog")
dbutils.widgets.text("schema", "", "Target schema")
dbutils.widgets.text("config_path", "", "Config YAML path relative to project root")
dbutils.widgets.text("source_system", "", "Source system key in config")

layer = dbutils.widgets.get("layer")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
config_path = dbutils.widgets.get("config_path")
source_system = dbutils.widgets.get("source_system")

print(f"Layer: {layer} | Catalog: {catalog} | Schema: {schema}")
print(f"Source system: {source_system} | Config: {config_path}")

# ─── Load Config ──────────────────────────────────────────────────────────────
import yaml
from pathlib import Path

# Resolve project root from this notebook's location
# Notebook lives at: <root>/src/04_governance/apply_uc_tags
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
root_dir = Path("/Workspace") / "/".join(notebook_path.split("/")[:-3]).lstrip("/")
full_config_path = root_dir / config_path

print(f"Reading config from: {full_config_path}")
config = yaml.safe_load(full_config_path.read_text())

tables = config.get("tables", {}).get(source_system, {})
if not tables:
    print(f"No tables found for source_system='{source_system}' in config.")
    dbutils.notebook.exit("NO_TABLES")

print(f"Found {len(tables)} table(s) to tag: {list(tables.keys())}")

# ─── Apply Tags ───────────────────────────────────────────────────────────────
for table_name, table_cfg in tables.items():
    target = table_cfg.get("target_table", table_name)
    fqn = f"`{catalog}`.`{schema}`.`{target}`"
    tags_cfg = table_cfg.get("tags", {})

    # Table-level tags
    table_tags = tags_cfg.get("table", {})
    if table_tags:
        tag_pairs = ", ".join(f"'{k}' = '{v}'" for k, v in table_tags.items())
        sql = f"ALTER TABLE {fqn} SET TAGS ({tag_pairs})"
        print(f"[TABLE] {fqn}: {tag_pairs}")
        spark.sql(sql)

    # Column-level tags
    column_tags = tags_cfg.get("columns", {})
    if column_tags:
        for col_name, col_tags in column_tags.items():
            tag_pairs = ", ".join(f"'{k}' = '{v}'" for k, v in col_tags.items())
            sql = f"ALTER TABLE {fqn} ALTER COLUMN `{col_name}` SET TAGS ({tag_pairs})"
            print(f"[COLUMN] {fqn}.{col_name}: {tag_pairs}")
            spark.sql(sql)

print("Done — all tags applied.")


