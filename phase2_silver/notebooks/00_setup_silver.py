# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Silver Setup
# MAGIC
# MAGIC Creates the Silver extraction table. The quarantine table was already
# MAGIC created by Phase 1's `00_setup.py`.
# MAGIC
# MAGIC **Run once** when initialising a new workspace or environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")

catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"

print(f"catalog : {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver table — `drug_label_extracted`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cat}.silver.drug_label_extracted (
        -- source linkage
        file_path              STRING  NOT NULL  COMMENT 'Volume path of the source XML file',
        set_id                 STRING            COMMENT 'SPL set identifier',
        source_batch_id        STRING            COMMENT 'Lakeflow run_id of the Silver extraction job that produced this row',

        -- identity fields extracted from the label
        drug_name              STRING            COMMENT 'Brand or trade name as it appears on the label',
        generic_name           STRING            COMMENT 'Non-proprietary (generic) name',
        manufacturer           STRING            COMMENT 'Labeler / marketing authorisation holder',
        ndc_codes              ARRAY<STRING>     COMMENT 'National Drug Codes listed on the label',

        -- versioning
        label_version          STRING            COMMENT 'Version number from the SPL document',
        effective_date         DATE              COMMENT 'Effective date of this label version',

        -- clinical content
        indication             STRING            COMMENT 'Approved indication / intended use',
        contraindications      STRING            COMMENT 'Conditions where the drug must not be used',
        dosage_forms           ARRAY<STRING>     COMMENT 'Available dosage forms (e.g. tablet, injection)',
        dosage_instructions    STRING            COMMENT 'Recommended dosage and administration',
        warnings               STRING            COMMENT 'Boxed warnings and key precautions',
        adverse_events         ARRAY<STRING>     COMMENT 'Known adverse reactions / side effects',
        drug_interactions      ARRAY<STRING>     COMMENT 'Documented drug-drug interactions',
        mechanism_of_action    STRING            COMMENT 'Pharmacological mechanism of action',

        -- full clean text (used by Phase 3 chunking + embeddings)
        full_text              STRING            COMMENT 'Cleaned section-aware text extracted from the SPL XML',

        -- extraction metadata
        model_endpoint         STRING            COMMENT 'Databricks model serving endpoint used for extraction',
        prompt_version         STRING            COMMENT 'Version tag of the extraction prompt',
        confidence_score       DOUBLE            COMMENT 'Composite confidence score in [0.0, 1.0]',
        extracted_at           TIMESTAMP         COMMENT 'When the row was written to Silver'
    )
    USING DELTA
    COMMENT 'Silver layer: structured fields extracted from FDA drug label XML using Databricks AI Functions.'
    TBLPROPERTIES (
        'quality'                              = 'silver',
        'sla_freshness_hours'                  = '24',
        'pii'                                  = 'false',
        'delta.autoOptimize.optimizeWrite'     = 'true',
        'delta.autoOptimize.autoCompact'       = 'true',
        'delta.enableChangeDataFeed'           = 'true'
    )
""")

print(f"Table ready: {catalog}.silver.drug_label_extracted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cat}.silver"))
