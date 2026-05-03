# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Bronze Setup
# MAGIC
# MAGIC Creates the Unity Catalog objects needed before ingestion runs:
# MAGIC - Catalog `clinical_docs`
# MAGIC - Schema `bronze`
# MAGIC - Bronze Delta table `drug_label_raw`
# MAGIC - Schema `silver` (placeholder so Silver notebooks can run later)
# MAGIC
# MAGIC **Run once** when initialising a new workspace or environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",      "clinical_docs", "Catalog")
dbutils.widgets.text("storage_root", "s3://YOUR-BUCKET/clinical-docs", "Storage root")

catalog      = dbutils.widgets.get("catalog")
storage_root = dbutils.widgets.get("storage_root").rstrip("/")

print(f"catalog      : {catalog}")
print(f"storage_root : {storage_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalog}
    COMMENT 'Clinical Document Intelligence Platform — all layers'
""")

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schemas

# COMMAND ----------

for schema, comment in [
    ("bronze", "Raw ingested documents — immutable audit log"),
    ("silver", "AI-extracted structured fields with quality metadata"),
    ("gold",   "Curated analytics tables and vector search metadata"),
]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
        COMMENT '{comment}'
        MANAGED LOCATION '{storage_root}/{schema}'
    """)
    print(f"Schema ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze table — `drug_label_raw`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.drug_label_raw (
        file_path           STRING  NOT NULL  COMMENT 'Full Volume path of the source file',
        file_name           STRING            COMMENT 'Basename of the file (e.g. "a1b2c3.xml")',
        file_type           STRING            COMMENT 'Source file format: xml | pdf',
        source_url          STRING            COMMENT 'DailyMed download URL the file originated from',
        set_id              STRING            COMMENT 'SPL set identifier extracted from the file path',
        raw_bytes           BINARY            COMMENT 'Raw file contents — never modify, only append',
        file_size_bytes     BIGINT            COMMENT 'File size in bytes',
        file_modified_time  TIMESTAMP         COMMENT 'Last-modified timestamp reported by the Volume',
        ingest_timestamp    TIMESTAMP         COMMENT 'Time this row was written to the Bronze table',
        source_batch_id     STRING            COMMENT 'Lakeflow Job run_id that produced this row',
        _rescued_data       STRING            COMMENT 'Auto Loader rescue column — captures unexpected schema fields'
    )
    USING DELTA
    COMMENT 'Bronze layer: raw FDA drug label files landed from DailyMed. One row per file per ingestion.'
    TBLPROPERTIES (
        'quality'                              = 'bronze',
        'owner'                                = 'data-engineering',
        'sla_freshness_hours'                  = '24',
        'pii'                                  = 'false',
        'delta.autoOptimize.optimizeWrite'     = 'true',
        'delta.autoOptimize.autoCompact'       = 'true',
        'delta.enableChangeDataFeed'           = 'true'
    )
""")

print(f"Table ready: {catalog}.bronze.drug_label_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine table — `drug_label_quarantine`
# MAGIC
# MAGIC Populated by the Silver extraction notebook for rows that fail
# MAGIC parsing or schema validation. Created here so the Bronze job
# MAGIC can reference it in lineage graphs.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.silver.drug_label_quarantine (
        file_path        STRING     COMMENT 'Source file path from Bronze',
        source_batch_id  STRING     COMMENT 'Batch that attempted extraction',
        failure_stage    STRING     COMMENT 'parse_error | schema_violation | low_confidence',
        failure_reason   STRING     COMMENT 'Human-readable error message',
        raw_response     STRING     COMMENT 'Raw LLM response or parse output before failure',
        quarantined_at   TIMESTAMP  COMMENT 'Time the row was written to quarantine'
    )
    USING DELTA
    COMMENT 'Silver quarantine: rows that failed AI extraction — review and reprocess manually.'
    TBLPROPERTIES (
        'quality'  = 'quarantine',
        'owner'    = 'ai-platform',
        'pii'      = 'false'
    )
""")

print(f"Table ready: {catalog}.silver.drug_label_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.bronze"))
display(spark.sql(f"SHOW TABLES IN {catalog}.silver"))
