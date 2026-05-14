# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Set up the violations log
# MAGIC
# MAGIC Creates `clinical-lab.trust.expectation_violations`. Every check the
# MAGIC expectations runner executes inserts one row per failed assertion so we
# MAGIC have a queryable audit trail of data-quality regressions.
# MAGIC
# MAGIC **Run once** when initialising a new workspace or environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"
print(f"catalog : {catalog}")

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {cat}.trust
    COMMENT 'Trust layer: data contracts, expectation violations, freshness + cost telemetry.'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.trust.expectation_violations (
        check_id          STRING    NOT NULL  COMMENT 'Stable id from the contract (e.g. dim_drug__set_id_not_null)',
        target_table      STRING    NOT NULL  COMMENT 'Fully qualified table the check ran against',
        layer             STRING              COMMENT 'bronze | silver | gold',
        severity          STRING              COMMENT 'error | warn',
        violation_count   BIGINT              COMMENT 'Number of rows that violated the assertion',
        row_count_total   BIGINT              COMMENT 'Total rows in the target table at check time',
        message           STRING              COMMENT 'Human-readable failure summary',
        run_id            STRING              COMMENT 'Lakeflow run id (or interactive_*) that produced this row',
        checked_at        TIMESTAMP NOT NULL  COMMENT 'When the check ran'
    )
    USING DELTA
    COMMENT 'One row per failed data-quality check. Successful checks are not recorded — see trust.expectation_runs for run-level summaries.'
    TBLPROPERTIES (
        'quality'                          = 'trust',
        'pii'                              = 'false',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.trust.expectation_runs (
        run_id           STRING    NOT NULL  COMMENT 'Lakeflow run id (or interactive_*)',
        checks_total     BIGINT              COMMENT 'Number of checks executed',
        checks_passed    BIGINT              COMMENT 'Number of checks with zero violations',
        checks_failed    BIGINT              COMMENT 'Number of checks with at least one violation',
        run_started_at   TIMESTAMP           COMMENT 'When the run began',
        run_finished_at  TIMESTAMP           COMMENT 'When the run completed'
    )
    USING DELTA
    COMMENT 'Per-run summary of expectation runs. Joined with expectation_violations for drilldown.'
""")

print(f"Tables ready: {catalog}.trust.expectation_violations, {catalog}.trust.expectation_runs")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cat}.trust"))
