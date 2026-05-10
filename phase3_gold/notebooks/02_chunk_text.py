# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Chunk SPL text for Vector Search
# MAGIC
# MAGIC Reads `silver.drug_label_extracted.full_text`, splits each label into
# MAGIC section-aware chunks, and writes them to `gold.drug_label_chunks`.
# MAGIC
# MAGIC ## Chunking strategy
# MAGIC
# MAGIC 1. Split by the section marker `\n\n## ` (the Silver text is already
# MAGIC    section-tagged this way).
# MAGIC 2. If a section exceeds `MAX_CHARS`, split it into sliding windows of
# MAGIC    `MAX_CHARS` with `OVERLAP_CHARS` overlap so retrieval contexts span
# MAGIC    sentence boundaries cleanly.
# MAGIC 3. Skip chunks shorter than `MIN_CHARS` — usually empty headers or noise.

# COMMAND ----------

dbutils.widgets.text("catalog",     "clinical-lab", "Catalog")
dbutils.widgets.text("max_chars",   "2000",         "Max chars per chunk")
dbutils.widgets.text("overlap",     "200",          "Overlap chars between sliding-window chunks")
dbutils.widgets.text("min_chars",   "100",          "Skip chunks shorter than this")

catalog     = dbutils.widgets.get("catalog")
cat         = f"`{catalog}`"
MAX_CHARS   = int(dbutils.widgets.get("max_chars"))
OVERLAP     = int(dbutils.widgets.get("overlap"))
MIN_CHARS   = int(dbutils.widgets.get("min_chars"))

print(f"catalog : {catalog}")
print(f"chunk   : max={MAX_CHARS}, overlap={OVERLAP}, min={MIN_CHARS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Silver

# COMMAND ----------

silver_df = spark.sql(f"""
    SELECT
        s.set_id,
        s.drug_name,
        s.generic_name,
        s.manufacturer,
        s.label_version,
        s.full_text
    FROM {cat}.silver.drug_label_extracted s
    WHERE s.full_text IS NOT NULL
      AND length(s.full_text) > {MIN_CHARS}
""")
silver_count = silver_df.count()
print(f"Silver rows to chunk: {silver_count}")

if silver_count == 0:
    dbutils.notebook.exit("No Silver rows to chunk. Stopping.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chunk via Python UDF

# COMMAND ----------

from pyspark.sql.functions import udf, col, posexplode
from pyspark.sql.types     import ArrayType, StructType, StructField, StringType, IntegerType

CHUNK_SCHEMA = ArrayType(StructType([
    StructField("section_title", StringType(),  True),
    StructField("chunk_text",    StringType(),  False),
]))

def chunk_text(full_text: str | None) -> list[dict]:
    if not full_text:
        return []
    chunks: list[dict] = []
    # Split on section markers; the first split element is whatever preceded "## ".
    sections = full_text.split("\n\n## ")
    for i, section in enumerate(sections):
        if i == 0 and not section.lstrip().startswith("## "):
            title, body = "PREAMBLE", section.strip()
        else:
            head, _, body = section.partition("\n")
            title = head.strip().lstrip("# ").strip() or "UNTITLED"
            body  = body.strip()

        if len(body) < MIN_CHARS:
            continue

        if len(body) <= MAX_CHARS:
            chunks.append({"section_title": title, "chunk_text": body})
        else:
            step = MAX_CHARS - OVERLAP
            for start in range(0, len(body), step):
                window = body[start:start + MAX_CHARS]
                if len(window) >= MIN_CHARS:
                    chunks.append({"section_title": title, "chunk_text": window})
                if start + MAX_CHARS >= len(body):
                    break
    return chunks

chunk_udf = udf(chunk_text, CHUNK_SCHEMA)

chunked_df = (
    silver_df
    .withColumn("chunks", chunk_udf(col("full_text")))
    .drop("full_text")
    .selectExpr(
        "set_id", "drug_name", "generic_name", "manufacturer", "label_version",
        "posexplode(chunks) AS (chunk_index, ck)",
    )
    .selectExpr(
        "concat(set_id, '_', cast(chunk_index AS string)) AS chunk_id",
        "set_id",
        "drug_name",
        "generic_name",
        "manufacturer",
        "label_version",
        "ck.section_title AS section_title",
        "chunk_index",
        "ck.chunk_text AS chunk_text",
        "current_timestamp() AS created_at",
    )
)

chunk_count = chunked_df.count()
print(f"Total chunks produced: {chunk_count}")
print(f"Avg chunks per label : {chunk_count / silver_count:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Overwrite `gold.drug_label_chunks`
# MAGIC
# MAGIC Full overwrite — chunks are derivative of Silver and cheap to rebuild.
# MAGIC The Vector Search delta-sync index will pick up the change automatically.

# COMMAND ----------

(
    chunked_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{cat}.gold.drug_label_chunks")
)

print(f"Wrote {chunk_count} chunks to {catalog}.gold.drug_label_chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spot-check chunks

# COMMAND ----------

display(spark.sql(f"""
    SELECT section_title,
           count(*) AS n_chunks,
           avg(length(chunk_text))::INT AS avg_chars,
           min(length(chunk_text)) AS min_chars,
           max(length(chunk_text)) AS max_chars
    FROM {cat}.gold.drug_label_chunks
    GROUP BY section_title
    ORDER BY n_chunks DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT chunk_id, drug_name, section_title,
           substring(chunk_text, 1, 150) AS chunk_preview
    FROM {cat}.gold.drug_label_chunks
    ORDER BY chunk_id
    LIMIT 10
"""))
