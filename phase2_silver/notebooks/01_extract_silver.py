# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Silver Extraction
# MAGIC
# MAGIC Reads new Bronze rows, parses the SPL XML into clean text, sends each label
# MAGIC to a Databricks Foundation Model via `ai_query` with structured output, and
# MAGIC writes the resulting structured fields to:
# MAGIC
# MAGIC - `clinical-lab.silver.drug_label_extracted` — successful extractions
# MAGIC - `clinical-lab.silver.drug_label_quarantine` — parse failures or low confidence
# MAGIC
# MAGIC ## Design
# MAGIC
# MAGIC - **Incremental**: only processes Bronze rows whose `set_id` is not already
# MAGIC   in Silver, so reruns are cheap and idempotent.
# MAGIC - **Cost-controlled**: a `limit` widget caps how many rows go through
# MAGIC   `ai_query` per run.
# MAGIC - **Confidence-scored**: every extraction gets a composite score; rows below
# MAGIC   threshold are quarantined for review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text       ("catalog",          "clinical-lab",                              "Catalog")
dbutils.widgets.text       ("model_endpoint",   "databricks-meta-llama-3-3-70b-instruct",    "Model endpoint")
dbutils.widgets.text       ("prompt_version",   "v1",                                         "Prompt version")
dbutils.widgets.text       ("limit",            "50",                                         "Max rows per run")
dbutils.widgets.text       ("min_confidence",   "0.6",                                        "Min confidence to keep")
dbutils.widgets.text       ("batch_id",         "",                                           "Batch ID (blank = run_id)")

catalog          = dbutils.widgets.get("catalog")
cat              = f"`{catalog}`"
model_endpoint   = dbutils.widgets.get("model_endpoint")
prompt_version   = dbutils.widgets.get("prompt_version")
row_limit        = int(dbutils.widgets.get("limit"))
min_confidence   = float(dbutils.widgets.get("min_confidence"))

from datetime import datetime
def _resolve_batch_id() -> str:
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    except Exception:
        return f"interactive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

batch_id = dbutils.widgets.get("batch_id") or _resolve_batch_id()

print(f"catalog         : {catalog}")
print(f"model_endpoint  : {model_endpoint}")
print(f"prompt_version  : {prompt_version}")
print(f"row_limit       : {row_limit}")
print(f"min_confidence  : {min_confidence}")
print(f"batch_id        : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read new Bronze rows
# MAGIC
# MAGIC Anti-join against Silver so already-extracted labels are skipped.

# COMMAND ----------

new_rows = spark.sql(f"""
    SELECT b.file_path, b.set_id, b.raw_bytes
    FROM   {cat}.bronze.drug_label_raw b
    LEFT ANTI JOIN {cat}.silver.drug_label_extracted s
      ON s.set_id = b.set_id
    WHERE  b.file_type = 'xml'
      AND  b.set_id IS NOT NULL
    LIMIT  {row_limit}
""")

new_count = new_rows.count()
print(f"Bronze rows pending extraction: {new_count}")

if new_count == 0:
    dbutils.notebook.exit("No new rows to extract. Stopping.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse SPL XML to clean section-aware text
# MAGIC
# MAGIC SPL uses HL7 v3 namespaces. We pull the `<title>` and `<text>` of every
# MAGIC `<section>` and concatenate them as `## Title\n\nbody`. This preserves
# MAGIC structure for the LLM without exposing it to raw XML noise.

# COMMAND ----------

import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf
from pyspark.sql.types    import StringType

SPL_NS = "{urn:hl7-org:v3}"

def spl_xml_to_text(xml_bytes: bytes) -> str | None:
    if xml_bytes is None:
        return None
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    sections = []
    for section in root.iter(f"{SPL_NS}section"):
        title_el = section.find(f"{SPL_NS}title")
        text_el  = section.find(f"{SPL_NS}text")
        title = "".join(title_el.itertext()).strip() if title_el is not None else ""
        body  = "".join(text_el.itertext()).strip()  if text_el  is not None else ""
        if title or body:
            sections.append(f"## {title}\n\n{body}".strip())
    return "\n\n".join(sections) if sections else None

spl_to_text_udf = udf(spl_xml_to_text, StringType())

text_df = (
    new_rows
    .withColumn("full_text", spl_to_text_udf("raw_bytes"))
    .filter("full_text IS NOT NULL AND length(full_text) > 200")
    .drop("raw_bytes")
)

print(f"Rows with parseable text: {text_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the extraction prompt
# MAGIC
# MAGIC We render the `extraction_v1.md` prompt as a single string and substitute
# MAGIC `{full_text}` per row in SQL. The schema is duplicated here so the model
# MAGIC and the parser stay in lock-step.

# COMMAND ----------

PROMPT = """You are a regulatory data extraction assistant. Read the FDA SPL drug label text below and return a single JSON object conforming exactly to the schema. Never invent values. Use null for missing scalars and [] for missing arrays.

Schema:
{
  "drug_name": "string|null",
  "generic_name": "string|null",
  "manufacturer": "string|null",
  "ndc_codes": "string[]",
  "label_version": "string|null",
  "effective_date": "string|null (YYYY-MM-DD)",
  "indication": "string|null (max 500 chars)",
  "contraindications": "string|null (max 500 chars)",
  "dosage_forms": "string[]",
  "dosage_instructions": "string|null (max 500 chars)",
  "warnings": "string|null (max 500 chars)",
  "adverse_events": "string[] (distinct items, no bullets)",
  "drug_interactions": "string[] (distinct items, no bullets)",
  "mechanism_of_action": "string|null (max 500 chars)"
}

Output the JSON object only — no prose, no code fences.

Label text:
"""

RESPONSE_FORMAT = """{
  "type": "json_schema",
  "json_schema": {
    "name": "drug_label_extraction",
    "schema": {
      "type": "object",
      "properties": {
        "drug_name":            {"type": ["string", "null"]},
        "generic_name":         {"type": ["string", "null"]},
        "manufacturer":         {"type": ["string", "null"]},
        "ndc_codes":            {"type": "array", "items": {"type": "string"}},
        "label_version":        {"type": ["string", "null"]},
        "effective_date":       {"type": ["string", "null"]},
        "indication":           {"type": ["string", "null"]},
        "contraindications":    {"type": ["string", "null"]},
        "dosage_forms":         {"type": "array", "items": {"type": "string"}},
        "dosage_instructions":  {"type": ["string", "null"]},
        "warnings":             {"type": ["string", "null"]},
        "adverse_events":       {"type": "array", "items": {"type": "string"}},
        "drug_interactions":    {"type": "array", "items": {"type": "string"}},
        "mechanism_of_action":  {"type": ["string", "null"]}
      },
      "required": ["drug_name", "generic_name", "indication", "adverse_events", "drug_interactions"]
    },
    "strict": true
  }
}"""

# Stage the rows-to-extract so we can reference them in SQL by table
text_df.createOrReplaceTempView("rows_to_extract")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Call `ai_query` with structured output

# COMMAND ----------

# Truncate to ~24k chars so we stay well under Llama 3 context for the prompt + response.
# SPL labels longer than this are rare; the long ones are still parseable here because
# the LLM doesn't need every section to extract the headline fields.
extracted_df = spark.sql(f"""
    SELECT
        file_path,
        set_id,
        full_text,
        ai_query(
            '{model_endpoint}',
            concat(
                '{PROMPT.replace("'", "''")}',
                substring(full_text, 1, 24000)
            ),
            responseFormat => '{RESPONSE_FORMAT}'
        ) AS extraction_json
    FROM rows_to_extract
""")

extracted_df.createOrReplaceTempView("extracted_raw")
print(f"ai_query completed for {extracted_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parse JSON, compute confidence, branch into Silver vs quarantine

# COMMAND ----------

REQUIRED_FIELDS = ["drug_name", "generic_name", "indication", "adverse_events", "drug_interactions"]

scored_df = spark.sql(f"""
    WITH parsed AS (
        SELECT
            file_path,
            set_id,
            full_text,
            extraction_json,
            from_json(
                extraction_json,
                'struct<
                    drug_name:           string,
                    generic_name:        string,
                    manufacturer:        string,
                    ndc_codes:           array<string>,
                    label_version:       string,
                    effective_date:      string,
                    indication:          string,
                    contraindications:   string,
                    dosage_forms:        array<string>,
                    dosage_instructions: string,
                    warnings:            string,
                    adverse_events:      array<string>,
                    drug_interactions:   array<string>,
                    mechanism_of_action: string
                >'
            ) AS x
        FROM extracted_raw
    )
    SELECT
        file_path,
        set_id,
        full_text,
        extraction_json,
        x,
        -- Composite confidence: completeness of required fields + array depth signal
        CAST(
            (
                CAST(x.drug_name           IS NOT NULL AS INT) * 0.20 +
                CAST(x.generic_name        IS NOT NULL AS INT) * 0.15 +
                CAST(x.indication          IS NOT NULL AS INT) * 0.20 +
                CAST(size(coalesce(x.adverse_events,    array())) > 0 AS INT) * 0.20 +
                CAST(size(coalesce(x.drug_interactions, array())) > 0 AS INT) * 0.15 +
                CAST(x.mechanism_of_action IS NOT NULL AS INT) * 0.10
            ) AS DOUBLE
        ) AS confidence_score
    FROM parsed
""")

scored_df.cache()
print(f"Scored rows: {scored_df.count()}  (cached)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write good rows to Silver

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, to_date, col

good_df = scored_df.filter(col("confidence_score") >= min_confidence)
good_count = good_df.count()
print(f"Good extractions (>= {min_confidence}): {good_count}")

if good_count > 0:
    silver_df = (
        good_df
        .select(
            col("file_path"),
            col("set_id"),
            lit(batch_id).alias("source_batch_id"),
            col("x.drug_name").alias("drug_name"),
            col("x.generic_name").alias("generic_name"),
            col("x.manufacturer").alias("manufacturer"),
            col("x.ndc_codes").alias("ndc_codes"),
            col("x.label_version").alias("label_version"),
            to_date(col("x.effective_date")).alias("effective_date"),
            col("x.indication").alias("indication"),
            col("x.contraindications").alias("contraindications"),
            col("x.dosage_forms").alias("dosage_forms"),
            col("x.dosage_instructions").alias("dosage_instructions"),
            col("x.warnings").alias("warnings"),
            col("x.adverse_events").alias("adverse_events"),
            col("x.drug_interactions").alias("drug_interactions"),
            col("x.mechanism_of_action").alias("mechanism_of_action"),
            col("full_text"),
            lit(model_endpoint).alias("model_endpoint"),
            lit(prompt_version).alias("prompt_version"),
            col("confidence_score"),
            current_timestamp().alias("extracted_at"),
        )
    )
    silver_df.write.mode("append").saveAsTable(f"{cat}.silver.drug_label_extracted")
    print(f"Appended {good_count} rows to {catalog}.silver.drug_label_extracted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quarantine low-confidence rows

# COMMAND ----------

bad_df = scored_df.filter(col("confidence_score") < min_confidence)
bad_count = bad_df.count()
print(f"Quarantined extractions (< {min_confidence}): {bad_count}")

if bad_count > 0:
    quarantine_df = (
        bad_df
        .select(
            col("file_path"),
            lit(batch_id).alias("source_batch_id"),
            lit("low_confidence").alias("failure_stage"),
            col("confidence_score").cast("string").alias("failure_reason"),
            col("extraction_json").alias("raw_response"),
            current_timestamp().alias("quarantined_at"),
        )
    )
    quarantine_df.write.mode("append").saveAsTable(f"{cat}.silver.drug_label_quarantine")
    print(f"Appended {bad_count} rows to {catalog}.silver.drug_label_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Run summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        '{batch_id}'                           AS batch_id,
        {good_count}                           AS extracted,
        {bad_count}                            AS quarantined,
        {new_count}                            AS bronze_pending_at_start,
        '{model_endpoint}'                     AS model_endpoint,
        '{prompt_version}'                     AS prompt_version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Spot-check the output

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        drug_name,
        generic_name,
        confidence_score,
        size(adverse_events)    AS n_adverse_events,
        size(drug_interactions) AS n_interactions,
        substring(indication, 1, 120) AS indication_preview
    FROM {cat}.silver.drug_label_extracted
    WHERE source_batch_id = '{batch_id}'
    ORDER BY confidence_score DESC
    LIMIT 20
"""))
