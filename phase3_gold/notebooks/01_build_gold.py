# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Build Gold (dim_drug + fact_adverse_events)
# MAGIC
# MAGIC Reads from `silver.drug_label_extracted` and rebuilds the dimension
# MAGIC and fact tables. Uses `MERGE` so reruns are idempotent and existing
# MAGIC `drug_sk` surrogate keys remain stable.

# COMMAND ----------

dbutils.widgets.text("catalog", "clinical-lab", "Catalog")
catalog = dbutils.widgets.get("catalog")
cat     = f"`{catalog}`"
print(f"catalog : {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Refresh `dim_drug`
# MAGIC
# MAGIC One row per (set_id, label_version). Highest-confidence Silver row wins
# MAGIC if a label was extracted multiple times. `is_current_version` is set to
# MAGIC TRUE for the most recent `effective_date` per `set_id`.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {cat}.gold.dim_drug t
    USING (
        WITH ranked AS (
            SELECT
                set_id,
                label_version,
                drug_name,
                generic_name,
                manufacturer,
                effective_date,
                mechanism_of_action,
                indication,
                ROW_NUMBER() OVER (
                    PARTITION BY set_id, coalesce(label_version, '__null__')
                    ORDER BY     confidence_score DESC, extracted_at DESC
                ) AS rn
            FROM {cat}.silver.drug_label_extracted
            WHERE set_id IS NOT NULL
        ),
        deduped AS (
            SELECT * FROM ranked WHERE rn = 1
        ),
        with_currency AS (
            SELECT
                d.*,
                CASE
                    WHEN d.effective_date IS NULL                    THEN FALSE
                    WHEN d.effective_date = MAX(d.effective_date)
                         OVER (PARTITION BY d.set_id)                THEN TRUE
                    ELSE FALSE
                END AS is_current_version
            FROM deduped d
        )
        SELECT * FROM with_currency
    ) s
    ON  t.set_id        <=> s.set_id
    AND t.label_version <=> s.label_version
    WHEN MATCHED THEN UPDATE SET
        t.drug_name           = s.drug_name,
        t.generic_name        = s.generic_name,
        t.manufacturer        = s.manufacturer,
        t.effective_date      = s.effective_date,
        t.mechanism_of_action = s.mechanism_of_action,
        t.indication_summary  = s.indication,
        t.is_current_version  = s.is_current_version,
        t.created_at          = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        set_id, label_version, drug_name, generic_name, manufacturer,
        effective_date, mechanism_of_action, indication_summary,
        is_current_version, created_at
    ) VALUES (
        s.set_id, s.label_version, s.drug_name, s.generic_name, s.manufacturer,
        s.effective_date, s.mechanism_of_action, s.indication,
        s.is_current_version, current_timestamp()
    )
""")

dim_count = spark.sql(f"SELECT count(*) AS n FROM {cat}.gold.dim_drug").first()["n"]
print(f"dim_drug rows: {dim_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rebuild `fact_adverse_events`
# MAGIC
# MAGIC Truncate and reload — fact is a pure derivation of dim + Silver, so a
# MAGIC full rebuild is simpler than maintaining MERGE keys on the exploded array.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {cat}.gold.fact_adverse_events")

spark.sql(f"""
    INSERT INTO {cat}.gold.fact_adverse_events
    SELECT
        d.drug_sk,
        s.set_id,
        trim(ae)                          AS adverse_event,
        s.label_version,
        s.effective_date,
        current_timestamp()               AS created_at
    FROM        {cat}.silver.drug_label_extracted s
    LEFT JOIN   {cat}.gold.dim_drug                d
      ON  d.set_id        <=> s.set_id
      AND d.label_version <=> s.label_version
    LATERAL VIEW EXPLODE(coalesce(s.adverse_events, array())) tbl AS ae
    WHERE  d.drug_sk IS NOT NULL
      AND  ae IS NOT NULL
      AND  trim(ae) <> ''
""")

fact_count = spark.sql(f"SELECT count(*) AS n FROM {cat}.gold.fact_adverse_events").first()["n"]
print(f"fact_adverse_events rows: {fact_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Spot-check

# COMMAND ----------

display(spark.sql(f"""
    SELECT drug_name, generic_name, manufacturer, effective_date,
           is_current_version, substring(indication_summary, 1, 100) AS indication_preview
    FROM {cat}.gold.dim_drug
    ORDER BY drug_name
    LIMIT 20
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT d.drug_name, count(*) AS n_adverse_events
    FROM        {cat}.gold.fact_adverse_events f
    INNER JOIN  {cat}.gold.dim_drug             d ON d.drug_sk = f.drug_sk
    GROUP BY d.drug_name
    ORDER BY n_adverse_events DESC
    LIMIT 20
"""))
