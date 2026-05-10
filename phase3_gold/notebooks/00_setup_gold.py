# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Gold Setup
# MAGIC
# MAGIC Creates the curated Gold tables used by BI and the RAG agent:
# MAGIC
# MAGIC | Table                          | Grain                                    |
# MAGIC |--------------------------------|------------------------------------------|
# MAGIC | `gold.dim_drug`                | one row per (set_id, label_version)      |
# MAGIC | `gold.fact_adverse_events`     | drug × adverse event × label_version     |
# MAGIC | `gold.drug_label_chunks`       | one row per text chunk (Vector Search)   |
# MAGIC
# MAGIC **Run once** when initialising a new workspace or environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"
print(f"catalog : {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_drug

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.gold.dim_drug (
        drug_sk              BIGINT GENERATED ALWAYS AS IDENTITY,
        set_id               STRING  NOT NULL  COMMENT 'SPL set identifier',
        label_version        STRING            COMMENT 'Version reported in the SPL header',
        drug_name            STRING            COMMENT 'Brand or trade name',
        generic_name         STRING            COMMENT 'Non-proprietary name',
        manufacturer         STRING            COMMENT 'Labeler / marketing authorisation holder',
        effective_date       DATE              COMMENT 'Effective date of this label version',
        mechanism_of_action  STRING            COMMENT 'Pharmacological MoA',
        indication_summary   STRING            COMMENT 'Approved indication, single paragraph',
        is_current_version   BOOLEAN           COMMENT 'TRUE if this is the latest label version we have for this set_id',
        created_at           TIMESTAMP         COMMENT 'When this row was last refreshed from Silver'
    )
    USING DELTA
    COMMENT 'Gold dimension: one row per drug label (set_id + version).'
    TBLPROPERTIES (
        'quality'                          = 'gold',
        'sla_freshness_hours'              = '24',
        'pii'                              = 'false',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.enableChangeDataFeed'       = 'true'
    )
""")
print(f"Table ready: {catalog}.gold.dim_drug")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_adverse_events

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.gold.fact_adverse_events (
        drug_sk          BIGINT  NOT NULL  COMMENT 'FK to gold.dim_drug',
        set_id           STRING            COMMENT 'Denormalised SPL set_id for query convenience',
        adverse_event    STRING  NOT NULL  COMMENT 'Adverse event as it appeared on the label',
        label_version    STRING            COMMENT 'Label version this fact was extracted from',
        effective_date   DATE              COMMENT 'Effective date of the source label',
        created_at       TIMESTAMP         COMMENT 'When this row was last refreshed from Silver'
    )
    USING DELTA
    COMMENT 'Gold fact: one row per (drug × adverse event × label version).'
    TBLPROPERTIES (
        'quality'                          = 'gold',
        'sla_freshness_hours'              = '24',
        'pii'                              = 'false',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.enableChangeDataFeed'       = 'true'
    )
""")
print(f"Table ready: {catalog}.gold.fact_adverse_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## drug_label_chunks
# MAGIC
# MAGIC The text chunks that back the Vector Search index. CDF is required for
# MAGIC delta-sync indices to detect changes incrementally.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.gold.drug_label_chunks (
        chunk_id        STRING  NOT NULL  COMMENT 'Stable id: <set_id>_<chunk_index>',
        set_id          STRING            COMMENT 'SPL set identifier (FK to dim_drug)',
        drug_name       STRING            COMMENT 'Denormalised for retrieval display',
        generic_name    STRING            COMMENT 'Denormalised for retrieval display',
        manufacturer    STRING            COMMENT 'Denormalised for retrieval display',
        label_version   STRING            COMMENT 'Label version of the source row',
        section_title   STRING            COMMENT 'SPL section header the chunk came from',
        chunk_index     INT               COMMENT 'Order within the source label, 0-based',
        chunk_text      STRING  NOT NULL  COMMENT 'The text content the embedding model will encode',
        created_at      TIMESTAMP         COMMENT 'When this chunk was last refreshed from Silver'
    )
    USING DELTA
    COMMENT 'Gold chunks: section-aware text chunks used as the source for Vector Search.'
    TBLPROPERTIES (
        'quality'                          = 'gold',
        'sla_freshness_hours'              = '24',
        'pii'                              = 'false',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'delta.enableChangeDataFeed'       = 'true'
    )
""")
print(f"Table ready: {catalog}.gold.drug_label_chunks")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cat}.gold"))
