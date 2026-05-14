# Databricks notebook source
# MAGIC %md
# MAGIC # Trust — Freshness + Cost Dashboard
# MAGIC
# MAGIC Two operational signals every senior reviewer asks about:
# MAGIC
# MAGIC 1. **Freshness**: how stale is each table vs its SLA?
# MAGIC 2. **Cost**: which job is burning DBUs, and what's the trend?
# MAGIC
# MAGIC The cost queries read from `system.billing.usage` — make sure the
# MAGIC `system` catalog is enabled in this workspace (Workspace settings →
# MAGIC System schemas → billing).

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table freshness vs SLA
# MAGIC
# MAGIC SLAs come from the YAML contracts. Edit this lookup if you add new tables.

# COMMAND ----------

freshness_targets = spark.createDataFrame(
    [
        ("bronze.drug_label_raw",        "ingest_timestamp",  24),
        ("silver.drug_label_extracted",  "extracted_at",      24),
        ("gold.dim_drug",                "created_at",        24),
        ("gold.fact_adverse_events",     "created_at",        24),
        ("gold.drug_label_chunks",       "created_at",        24),
    ],
    "table_path STRING, timestamp_col STRING, sla_hours INT",
)
freshness_targets.createOrReplaceTempView("freshness_targets")

# Build a UNION ALL — one row per target — of the max(timestamp_col)
unions = " UNION ALL ".join(
    f"SELECT '{t}' AS table_path, max({c}) AS last_loaded_at FROM {cat}.{t}"
    for t, c, _ in freshness_targets.collect()
)

display(spark.sql(f"""
    WITH measured AS ({unions})
    SELECT
        m.table_path,
        m.last_loaded_at,
        f.sla_hours,
        ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(m.last_loaded_at)) / 3600.0, 1) AS hours_since_load,
        CASE
            WHEN m.last_loaded_at IS NULL THEN 'no_data'
            WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(m.last_loaded_at)) / 3600.0
                 > f.sla_hours
            THEN 'BREACH'
            ELSE 'ok'
        END AS sla_status
    FROM measured m
    INNER JOIN freshness_targets f USING (table_path)
    ORDER BY hours_since_load DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DBU spend per Lakeflow Job — last 14 days
# MAGIC
# MAGIC Filters `system.billing.usage` for jobs whose name starts with
# MAGIC `clinical-docs-`. Adjust the LIKE filter if you rename the jobs.

# COMMAND ----------

display(spark.sql("""
    SELECT
        DATE(usage_start_time)                                  AS usage_date,
        custom_tags['job_name']                                 AS job_name,
        ROUND(SUM(usage_quantity), 2)                           AS dbus_consumed,
        ROUND(SUM(usage_quantity * list_price), 2)              AS estimated_cost_usd
    FROM system.billing.usage u
    INNER JOIN system.billing.list_prices p
      ON p.sku_name = u.sku_name
      AND p.currency_code = 'USD'
      AND p.price_start_time <= u.usage_start_time
      AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_start_time)
    WHERE usage_start_time >= current_date() - INTERVAL 14 DAYS
      AND custom_tags['job_name'] LIKE 'clinical-docs-%'
    GROUP BY usage_date, custom_tags['job_name']
    ORDER BY usage_date DESC, estimated_cost_usd DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Total estimated cost per pipeline stage

# COMMAND ----------

display(spark.sql("""
    SELECT
        REGEXP_EXTRACT(custom_tags['job_name'],
                       '^clinical-docs-([a-z_-]+)', 1)         AS stage,
        ROUND(SUM(usage_quantity * list_price), 2)             AS cost_usd_14d,
        ROUND(SUM(usage_quantity), 2)                          AS dbus_14d
    FROM system.billing.usage u
    INNER JOIN system.billing.list_prices p
      ON p.sku_name = u.sku_name
      AND p.currency_code = 'USD'
      AND p.price_start_time <= u.usage_start_time
      AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_start_time)
    WHERE usage_start_time >= current_date() - INTERVAL 14 DAYS
      AND custom_tags['job_name'] LIKE 'clinical-docs-%'
    GROUP BY stage
    ORDER BY cost_usd_14d DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vector Search index health (point-in-time)

# COMMAND ----------

# MAGIC %pip install --quiet databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("vs_endpoint", "clinical_docs_vs",                        "Vector Search endpoint")
dbutils.widgets.text("index_name",  "clinical-lab.gold.drug_label_chunks_idx", "Vector Search index")

vs_endpoint = dbutils.widgets.get("vs_endpoint")
index_name  = dbutils.widgets.get("index_name")

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)
try:
    idx = vsc.get_index(endpoint_name=vs_endpoint, index_name=index_name)
    print(idx.describe())
except Exception as e:
    print(f"Could not read index status: {e}")
