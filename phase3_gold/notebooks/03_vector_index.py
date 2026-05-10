# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Create Vector Search Index
# MAGIC
# MAGIC Creates a delta-sync Vector Search index over `gold.drug_label_chunks`.
# MAGIC The index auto-embeds `chunk_text` using a Databricks-managed embedding
# MAGIC endpoint and stays in sync with the source Delta table.
# MAGIC
# MAGIC ## Prerequisites (one-time, manual)
# MAGIC
# MAGIC 1. **Vector Search endpoint** — create one in the workspace UI:
# MAGIC    Compute → Vector Search → Create endpoint → name it `clinical_docs_vs`.
# MAGIC    (Standard endpoint is fine for this scale.)
# MAGIC 2. **Embedding endpoint** — `databricks-gte-large-en` is enabled by default
# MAGIC    in Foundation Model APIs. No setup needed.
# MAGIC 3. **CDF on source table** — already enabled in `00_setup_gold.py` via
# MAGIC    `delta.enableChangeDataFeed = true`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog",            "clinical-lab",                "Catalog")
dbutils.widgets.text("vs_endpoint",        "clinical_docs_vs",            "Vector Search endpoint name")
dbutils.widgets.text("index_name",         "drug_label_chunks_idx",       "Index name (under <catalog>.gold)")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en",     "Embedding model endpoint")
dbutils.widgets.text("pipeline_type",      "TRIGGERED",                   "TRIGGERED | CONTINUOUS")

catalog            = dbutils.widgets.get("catalog")
vs_endpoint        = dbutils.widgets.get("vs_endpoint")
index_short_name   = dbutils.widgets.get("index_name")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
pipeline_type      = dbutils.widgets.get("pipeline_type")

source_table  = f"{catalog}.gold.drug_label_chunks"
full_index    = f"{catalog}.gold.{index_short_name}"

print(f"source_table       : {source_table}")
print(f"index              : {full_index}")
print(f"vs_endpoint        : {vs_endpoint}")
print(f"embedding_endpoint : {embedding_endpoint}")
print(f"pipeline_type      : {pipeline_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify the endpoint exists

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if vs_endpoint not in endpoints:
    raise RuntimeError(
        f"Endpoint '{vs_endpoint}' not found. Existing: {endpoints}.\n"
        f"Create it in the UI under Compute → Vector Search → Create endpoint."
    )
print(f"Endpoint '{vs_endpoint}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create or replace the index

# COMMAND ----------

# Idempotent: drop the existing index first if it exists, then recreate.
existing = [
    i["name"]
    for i in vsc.list_indexes(name=vs_endpoint).get("vector_indexes", [])
]
if full_index in existing:
    print(f"Existing index found — deleting {full_index} so we can recreate cleanly.")
    vsc.delete_index(endpoint_name=vs_endpoint, index_name=full_index)

print("Creating delta-sync index…")
vsc.create_delta_sync_index(
    endpoint_name                 = vs_endpoint,
    index_name                    = full_index,
    source_table_name             = source_table,
    pipeline_type                 = pipeline_type,                    # TRIGGERED = on-demand sync, CONTINUOUS = auto
    primary_key                   = "chunk_id",
    embedding_source_column       = "chunk_text",
    embedding_model_endpoint_name = embedding_endpoint,
)
print(f"Index {full_index} created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Trigger an initial sync (TRIGGERED pipelines only)

# COMMAND ----------

if pipeline_type == "TRIGGERED":
    print(f"Kicking off initial sync for {full_index}…")
    vsc.get_index(endpoint_name=vs_endpoint, index_name=full_index).sync()
    print("Sync started. Track progress in the workspace UI under Catalog → drug_label_chunks_idx.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test query
# MAGIC
# MAGIC Wait until the index status shows `ONLINE` in the UI before running this
# MAGIC cell — the initial embedding pass takes a few minutes per 1000 chunks.

# COMMAND ----------

idx = vsc.get_index(endpoint_name=vs_endpoint, index_name=full_index)

results = idx.similarity_search(
    query_text="What are the common adverse cardiovascular events?",
    columns=["chunk_id", "drug_name", "section_title", "chunk_text"],
    num_results=5,
)

import pandas as pd
hits = results.get("result", {}).get("data_array", [])
schema = [c["name"] for c in results.get("manifest", {}).get("columns", [])]
df = pd.DataFrame(hits, columns=schema)
display(df)
