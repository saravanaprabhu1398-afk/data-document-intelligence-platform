# Databricks notebook source
# MAGIC %md
# MAGIC # Trust — Data Quality Dashboard
# MAGIC
# MAGIC Single-screen view of expectation health. Every cell renders a chart or
# MAGIC table that can be dragged onto a Databricks SQL dashboard.
# MAGIC
# MAGIC Source tables:
# MAGIC - `trust.expectation_runs`        — one row per run
# MAGIC - `trust.expectation_violations`  — one row per failed assertion

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pass rate over time

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        DATE(run_started_at)                                AS run_date,
        sum(checks_passed)                                  AS checks_passed,
        sum(checks_failed)                                  AS checks_failed,
        ROUND(100.0 * sum(checks_passed) /
              NULLIF(sum(checks_total), 0), 1)              AS pass_rate_pct
    FROM {cat}.trust.expectation_runs
    GROUP BY DATE(run_started_at)
    ORDER BY run_date DESC
    LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top failing checks (last 7 days)

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        check_id,
        target_table,
        severity,
        count(*)              AS run_count_with_failure,
        sum(violation_count)  AS total_violations,
        MAX(checked_at)       AS last_failed_at
    FROM {cat}.trust.expectation_violations
    WHERE checked_at >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY check_id, target_table, severity
    ORDER BY total_violations DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Most recent violations (drilldown)

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        checked_at,
        run_id,
        check_id,
        target_table,
        severity,
        violation_count,
        row_count_total,
        message
    FROM {cat}.trust.expectation_violations
    ORDER BY checked_at DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver extraction quality — confidence score distribution

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        CASE
            WHEN confidence_score >= 0.9 THEN '0.9–1.0'
            WHEN confidence_score >= 0.8 THEN '0.8–0.9'
            WHEN confidence_score >= 0.7 THEN '0.7–0.8'
            WHEN confidence_score >= 0.6 THEN '0.6–0.7'
            WHEN confidence_score >= 0.5 THEN '0.5–0.6'
            ELSE                              '< 0.5'
        END                            AS confidence_bucket,
        count(*)                       AS labels
    FROM {cat}.silver.drug_label_extracted
    GROUP BY confidence_bucket
    ORDER BY confidence_bucket DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quarantine watch

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        DATE(quarantined_at)  AS quarantine_date,
        failure_stage,
        count(*)              AS rows_quarantined
    FROM {cat}.silver.drug_label_quarantine
    GROUP BY DATE(quarantined_at), failure_stage
    ORDER BY quarantine_date DESC
"""))
