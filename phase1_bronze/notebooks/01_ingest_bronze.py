# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Bronze Ingestion
# MAGIC
# MAGIC Reads new **XML (SPL)** files from the Unity Catalog Volume using **Auto Loader**
# MAGIC and appends them to `{catalog}.bronze.drug_label_raw`.
# MAGIC
# MAGIC - Source format: DailyMed SPL XML — one file per drug label bundle
# MAGIC - Auto Loader format: `binaryFile` — captures raw bytes + Volume metadata
# MAGIC - Trigger: `availableNow=True` — processes all new files then stops (batch mode, suitable for Lakeflow Jobs)
# MAGIC - Checkpoint: persists Auto Loader state so reruns are idempotent
# MAGIC - Schema evolution: `addNewColumns` — new metadata fields are added automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",      "clinical-lab",                                       "Catalog")
dbutils.widgets.text("source_path",  "/Volumes/clinical-lab/default/raw_clinical_pdf",     "Source path (XML files)")
dbutils.widgets.text("checkpoint",   "/Volumes/clinical-lab/default/_checkpoints/bronze",  "Checkpoint path")
dbutils.widgets.text("batch_id",     "",                                                    "Batch ID (leave blank to use job run_id)")

from datetime import datetime

catalog     = dbutils.widgets.get("catalog")
# Backtick-quoted for use in SQL — required when the catalog name contains hyphens
cat         = f"`{catalog}`"
source_path = dbutils.widgets.get("source_path").rstrip("/")
checkpoint  = dbutils.widgets.get("checkpoint").rstrip("/")

def _resolve_batch_id() -> str:
    # currentRunId() is only available when running as a Databricks Job
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    except Exception:
        return f"interactive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

batch_id = dbutils.widgets.get("batch_id") or _resolve_batch_id()

print(f"catalog      : {catalog}")
print(f"source_path  : {source_path}")
print(f"checkpoint   : {checkpoint}")
print(f"batch_id     : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read with Auto Loader

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "binaryFile")
    .option("cloudFiles.schemaLocation",      checkpoint + "/schema")
    .option("cloudFiles.schemaEvolutionMode", "none")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter",                 "*.xml")
    .option("recursiveFileLookup",            "true")
    .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform — add ingestion metadata

# COMMAND ----------

def extract_set_id(path_col):
    """
    DailyMed ZIP bundles use the SPL set_id (UUID) as the directory name.
    Volume path pattern: /Volumes/.../raw_clinical_pdf/<set_id>/<set_id>.xml
    Falls back to the filename stem if the UUID pattern does not match.
    """
    return F.coalesce(
        F.regexp_extract(path_col, r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/[^/]+\.xml$", 1),
        F.regexp_extract(path_col, r"/([^/]+)\.xml$", 1),
    )

bronze_df = (
    raw_df
    .withColumnRenamed("path",             "file_path")
    .withColumnRenamed("content",          "raw_bytes")
    .withColumnRenamed("length",           "file_size_bytes")
    .withColumnRenamed("modificationTime", "file_modified_time")
    .withColumn("file_name",        F.element_at(F.split(F.col("file_path"), "/"), -1))
    .withColumn("file_type",        F.lit("xml"))
    .withColumn("set_id",           extract_set_id(F.col("file_path")))
    .withColumn("source_url",       F.lit(None).cast("string"))  # populated by download script metadata
    .withColumn("ingest_timestamp", F.current_timestamp())
    .withColumn("source_batch_id",  F.lit(batch_id))
    .select(
        "file_path",
        "file_name",
        "file_type",
        "source_url",
        "set_id",
        "raw_bytes",
        "file_size_bytes",
        "file_modified_time",
        "ingest_timestamp",
        "source_batch_id",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Bronze Delta table

# COMMAND ----------

(
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint + "/checkpoint")
    .option("mergeSchema",        "true")
    # availableNow processes all pending files then terminates — ideal for scheduled jobs
    .trigger(availableNow=True)
    .toTable(f"{cat}.bronze.drug_label_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Post-run metrics

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        source_batch_id,
        file_type,
        COUNT(*)                            AS files_ingested,
        ROUND(SUM(file_size_bytes)/1e6, 2)  AS total_mb,
        COUNT(DISTINCT set_id)              AS unique_labels,
        MIN(ingest_timestamp)               AS first_file,
        MAX(ingest_timestamp)               AS last_file
    FROM {cat}.bronze.drug_label_raw
    WHERE source_batch_id = '{batch_id}'
    GROUP BY source_batch_id, file_type
""")

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cumulative table health

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        DATE(ingest_timestamp)              AS ingest_date,
        file_type,
        COUNT(*)                            AS files,
        ROUND(SUM(file_size_bytes)/1e6, 1)  AS total_mb,
        COUNT(DISTINCT set_id)              AS unique_labels
    FROM {cat}.bronze.drug_label_raw
    GROUP BY ingest_date, file_type
    ORDER BY ingest_date DESC
"""))
