# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Run data-quality expectations
# MAGIC
# MAGIC Executes a catalogue of SQL-based expectations across the Bronze, Silver,
# MAGIC and Gold layers. Every failed assertion lands one row in
# MAGIC `trust.expectation_violations`. Each invocation lands one row in
# MAGIC `trust.expectation_runs`.
# MAGIC
# MAGIC Checks mirror the YAML contracts under `phase5_trust/contracts/`. Adding
# MAGIC a contract assertion means adding a row to the `CHECKS` list below.

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"
print(f"catalog : {catalog}")

# COMMAND ----------

from datetime import datetime

def _resolve_run_id() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    except Exception:
        return f"interactive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

run_id = _resolve_run_id()
print(f"run_id : {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check catalogue
# MAGIC
# MAGIC Each entry is:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "check_id":     unique id, used as the key in violations log
# MAGIC   "target_table": fully-qualified table this check runs against
# MAGIC   "layer":        bronze | silver | gold
# MAGIC   "severity":     error | warn
# MAGIC   "violation_sql": SELECT count(*) AS violation_count, MAX(<total>) FROM ...
# MAGIC                   — must return a single row with violation_count + row_count_total
# MAGIC }
# MAGIC ```

# COMMAND ----------

CHECKS = [
    # ── Bronze: every row has a set_id and parseable bytes ───────────────────
    {
        "check_id":     "bronze__set_id_not_null",
        "target_table": f"{cat}.bronze.drug_label_raw",
        "layer":        "bronze",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(set_id IS NULL) AS violation_count,
                count(*)                 AS row_count_total
            FROM {cat}.bronze.drug_label_raw
        """,
    },
    {
        "check_id":     "bronze__file_size_positive",
        "target_table": f"{cat}.bronze.drug_label_raw",
        "layer":        "bronze",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(file_size_bytes IS NULL OR file_size_bytes <= 0) AS violation_count,
                count(*)                                                   AS row_count_total
            FROM {cat}.bronze.drug_label_raw
        """,
    },

    # ── Silver: required fields and effective_date sanity ────────────────────
    {
        "check_id":     "silver__drug_name_completeness_95",
        "target_table": f"{cat}.silver.drug_label_extracted",
        "layer":        "silver",
        "severity":     "error",
        "violation_sql": f"""
            WITH s AS (
                SELECT
                    count_if(drug_name IS NULL) AS missing,
                    count(*)                    AS total
                FROM {cat}.silver.drug_label_extracted
            )
            SELECT
                CASE WHEN total = 0 THEN 0
                     WHEN missing * 100.0 / total > 5 THEN missing
                     ELSE 0 END AS violation_count,
                total           AS row_count_total
            FROM s
        """,
    },
    {
        "check_id":     "silver__effective_date_not_future",
        "target_table": f"{cat}.silver.drug_label_extracted",
        "layer":        "silver",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(effective_date IS NOT NULL AND effective_date > current_date()) AS violation_count,
                count(*)                                                                  AS row_count_total
            FROM {cat}.silver.drug_label_extracted
        """,
    },
    {
        "check_id":     "silver__confidence_within_bounds",
        "target_table": f"{cat}.silver.drug_label_extracted",
        "layer":        "silver",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(confidence_score < 0.0 OR confidence_score > 1.0) AS violation_count,
                count(*)                                                    AS row_count_total
            FROM {cat}.silver.drug_label_extracted
        """,
    },

    # ── Gold: dim_drug + fact_adverse_events integrity ───────────────────────
    {
        "check_id":     "dim_drug__set_id_not_null",
        "target_table": f"{cat}.gold.dim_drug",
        "layer":        "gold",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(set_id IS NULL) AS violation_count,
                count(*)                 AS row_count_total
            FROM {cat}.gold.dim_drug
        """,
    },
    {
        "check_id":     "dim_drug__unique_set_id_version",
        "target_table": f"{cat}.gold.dim_drug",
        "layer":        "gold",
        "severity":     "error",
        "violation_sql": f"""
            WITH dupes AS (
                SELECT set_id, label_version, count(*) AS n
                FROM {cat}.gold.dim_drug
                GROUP BY set_id, label_version
                HAVING count(*) > 1
            ),
            t AS (SELECT count(*) AS total FROM {cat}.gold.dim_drug)
            SELECT
                (SELECT coalesce(sum(n), 0) FROM dupes) AS violation_count,
                (SELECT total FROM t)                   AS row_count_total
        """,
    },
    {
        "check_id":     "dim_drug__effective_date_not_future",
        "target_table": f"{cat}.gold.dim_drug",
        "layer":        "gold",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(effective_date IS NOT NULL AND effective_date > current_date()) AS violation_count,
                count(*)                                                                  AS row_count_total
            FROM {cat}.gold.dim_drug
        """,
    },
    {
        "check_id":     "fact_ae__drug_sk_fk_exists",
        "target_table": f"{cat}.gold.fact_adverse_events",
        "layer":        "gold",
        "severity":     "error",
        "violation_sql": f"""
            WITH t AS (SELECT count(*) AS total FROM {cat}.gold.fact_adverse_events)
            SELECT
                (SELECT count(*)
                   FROM {cat}.gold.fact_adverse_events f
                   LEFT ANTI JOIN {cat}.gold.dim_drug d ON d.drug_sk = f.drug_sk
                ) AS violation_count,
                (SELECT total FROM t) AS row_count_total
        """,
    },
    {
        "check_id":     "fact_ae__adverse_event_not_blank",
        "target_table": f"{cat}.gold.fact_adverse_events",
        "layer":        "gold",
        "severity":     "error",
        "violation_sql": f"""
            SELECT
                count_if(adverse_event IS NULL OR trim(adverse_event) = '') AS violation_count,
                count(*)                                                     AS row_count_total
            FROM {cat}.gold.fact_adverse_events
        """,
    },

    # ── Gold: chunks ─────────────────────────────────────────────────────────
    {
        "check_id":     "chunks__chunk_id_unique",
        "target_table": f"{cat}.gold.drug_label_chunks",
        "layer":        "gold",
        "severity":     "error",
        "violation_sql": f"""
            WITH dupes AS (
                SELECT chunk_id, count(*) AS n
                FROM {cat}.gold.drug_label_chunks
                GROUP BY chunk_id
                HAVING count(*) > 1
            ),
            t AS (SELECT count(*) AS total FROM {cat}.gold.drug_label_chunks)
            SELECT
                (SELECT coalesce(sum(n), 0) FROM dupes) AS violation_count,
                (SELECT total FROM t)                   AS row_count_total
        """,
    },
    {
        "check_id":     "chunks__text_length_bounds",
        "target_table": f"{cat}.gold.drug_label_chunks",
        "layer":        "gold",
        "severity":     "warn",
        "violation_sql": f"""
            SELECT
                count_if(length(chunk_text) < 100 OR length(chunk_text) > 2500) AS violation_count,
                count(*)                                                          AS row_count_total
            FROM {cat}.gold.drug_label_chunks
        """,
    },
]

print(f"Loaded {len(CHECKS)} checks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Execute checks and log violations

# COMMAND ----------

from pyspark.sql        import Row
from pyspark.sql.types  import StructType, StructField, StringType, LongType, TimestampType
from datetime           import datetime, timezone

run_started_at = datetime.now(timezone.utc)
violations:    list[Row] = []
passed = failed = 0

for c in CHECKS:
    try:
        row = spark.sql(c["violation_sql"]).first()
        vcount = int(row["violation_count"] or 0)
        rcount = int(row["row_count_total"] or 0)
    except Exception as e:
        vcount, rcount = 1, 0
        msg = f"check raised exception: {e}"
        violations.append(Row(
            check_id        = c["check_id"],
            target_table    = c["target_table"],
            layer           = c["layer"],
            severity        = c["severity"],
            violation_count = vcount,
            row_count_total = rcount,
            message         = msg,
            run_id          = run_id,
            checked_at      = datetime.now(timezone.utc),
        ))
        failed += 1
        print(f"  ✗  {c['check_id']} — exception: {e}")
        continue

    if vcount > 0:
        msg = f"{vcount} of {rcount} rows violated this assertion"
        violations.append(Row(
            check_id        = c["check_id"],
            target_table    = c["target_table"],
            layer           = c["layer"],
            severity        = c["severity"],
            violation_count = vcount,
            row_count_total = rcount,
            message         = msg,
            run_id          = run_id,
            checked_at      = datetime.now(timezone.utc),
        ))
        failed += 1
        marker = "✗" if c["severity"] == "error" else "⚠"
        print(f"  {marker}  {c['check_id']:<50} {msg}")
    else:
        passed += 1
        print(f"  ✓  {c['check_id']:<50} ok ({rcount} rows)")

run_finished_at = datetime.now(timezone.utc)
print(f"\nSummary: {passed} passed, {failed} failed (out of {len(CHECKS)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Persist results

# COMMAND ----------

if violations:
    schema = StructType([
        StructField("check_id",        StringType(),    False),
        StructField("target_table",    StringType(),    False),
        StructField("layer",           StringType(),    True),
        StructField("severity",        StringType(),    True),
        StructField("violation_count", LongType(),      True),
        StructField("row_count_total", LongType(),      True),
        StructField("message",         StringType(),    True),
        StructField("run_id",          StringType(),    True),
        StructField("checked_at",      TimestampType(), False),
    ])
    spark.createDataFrame(violations, schema=schema) \
         .write.mode("append").saveAsTable(f"{cat}.trust.expectation_violations")
    print(f"Wrote {len(violations)} violations to trust.expectation_violations")

spark.createDataFrame(
    [(run_id, len(CHECKS), passed, failed, run_started_at, run_finished_at)],
    "run_id STRING, checks_total BIGINT, checks_passed BIGINT, checks_failed BIGINT, run_started_at TIMESTAMP, run_finished_at TIMESTAMP",
).write.mode("append").saveAsTable(f"{cat}.trust.expectation_runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fail the task if any error-severity check failed
# MAGIC
# MAGIC When run as part of a Lakeflow Job, this causes the task to fail and
# MAGIC the on_failure email trigger fires.

# COMMAND ----------

errors = [v for v in violations if v["severity"] == "error"]
if errors:
    msg = f"{len(errors)} error-severity check(s) failed: " + ", ".join(v["check_id"] for v in errors)
    raise AssertionError(msg)

print("All error-severity checks passed.")
