# Architecture: Clinical Document Intelligence Platform

## Problem Statement

Regulatory and clinical documents (FDA drug labels, package inserts, clinical study reports) contain rich structured information buried in unstructured PDFs. Today this information is extracted manually — slowly, inconsistently, and at high cost. This platform automates extraction at scale, lands the results in a governed lakehouse, and exposes them through both BI and a conversational AI agent.

---

## Data Flow

```
DailyMed FTP
    │
    ▼
Cloud Storage (raw zone)
    │  Auto Loader — fileNotifications mode
    ▼
Bronze Delta Table
    │  ai_parse_document (PDF → text)
    │  ai_query (text → structured JSON)
    ▼
Silver Delta Table ──► Quarantine Table
    │
    ├──► Gold: dim_drug
    ├──► Gold: fact_adverse_events
    ├──► Gold: fact_dosage
    │
    └──► Chunk + embed
             │
             ▼
        Vector Search Index
             │
             ▼
         RAG Agent ──► Genie Space / Streamlit
```

---

## Layer Design

### Bronze — Raw Ingestion

**Purpose:** land raw bytes untouched; create an immutable audit log of every document we've ever seen.

**Schema:**
```sql
CREATE TABLE bronze.drug_label_raw (
  file_path        STRING,
  file_name        STRING,
  source_url       STRING,
  raw_bytes        BINARY,
  file_size_bytes  BIGINT,
  ingest_timestamp TIMESTAMP,
  source_batch_id  STRING,
  _rescued_data    STRING        -- Auto Loader rescue column
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality' = 'bronze'
);
```

**Key decisions:**
- Store `raw_bytes` so we can re-extract if the prompt changes — no need to re-download
- `source_batch_id` ties every row back to the Lakeflow Job run that created it
- Schema evolution via `cloudFiles.schemaEvolutionMode = 'addNewColumns'`

---

### Silver — AI Extraction

**Purpose:** transform raw bytes into clean, schema-enforced structured fields; quarantine failures without stopping the pipeline.

**Schema:**
```sql
CREATE TABLE silver.drug_label_extracted (
  -- source linkage
  file_path             STRING NOT NULL,
  source_batch_id       STRING NOT NULL,
  -- extracted fields
  set_id                STRING,        -- SPL set ID (unique per label)
  ndc_codes             ARRAY<STRING>,
  drug_name             STRING,
  generic_name          STRING,
  manufacturer          STRING,
  label_version         STRING,
  effective_date        DATE,
  indication            STRING,
  contraindications     STRING,
  dosage_forms          ARRAY<STRING>,
  dosage_instructions   STRING,
  warnings              STRING,
  adverse_events        ARRAY<STRING>,
  drug_interactions     ARRAY<STRING>,
  mechanism_of_action   STRING,
  -- extraction metadata
  extracted_at          TIMESTAMP,
  model_version         STRING,
  prompt_version        STRING,
  confidence_score      DOUBLE,        -- 0.0–1.0 composite score
  extraction_duration_s DOUBLE,
  -- raw text for vector layer
  full_text             STRING
)
USING DELTA
TBLPROPERTIES (
  'quality' = 'silver',
  'owner'   = 'ai-platform'
);
```

**Quarantine schema:**
```sql
CREATE TABLE silver.drug_label_quarantine (
  file_path       STRING,
  source_batch_id STRING,
  failure_reason  STRING,   -- 'parse_error' | 'schema_violation' | 'low_confidence'
  raw_response    STRING,
  quarantined_at  TIMESTAMP
);
```

**Confidence score logic:**
- Field completeness: what fraction of required fields are non-null (weight: 0.5)
- Model self-reported confidence in prompt response (weight: 0.3)
- Cross-check against SPL XML ground truth where available (weight: 0.2)
- Rows below threshold 0.6 go to quarantine

---

### Gold — Analytics + Vector

**Purpose:** curated, business-friendly tables optimized for query patterns; vector index for semantic retrieval.

**dim_drug** — one row per unique drug (set_id + version):
```sql
CREATE TABLE gold.dim_drug (
  drug_sk               BIGINT GENERATED ALWAYS AS IDENTITY,
  set_id                STRING NOT NULL,
  label_version         STRING NOT NULL,
  drug_name             STRING,
  generic_name          STRING,
  manufacturer          STRING,
  effective_date        DATE,
  mechanism_of_action   STRING,
  indication_category   STRING,   -- normalized via LLM classification
  is_current_version    BOOLEAN,
  created_at            TIMESTAMP
);
```

**fact_adverse_events** — grain: drug × adverse event × label version:
```sql
CREATE TABLE gold.fact_adverse_events (
  drug_sk          BIGINT,
  adverse_event    STRING,
  event_category   STRING,   -- e.g. 'cardiovascular', 'neurological'
  label_section    STRING,   -- e.g. 'WARNINGS', 'ADVERSE REACTIONS'
  source_sentence  STRING,
  label_version    STRING,
  effective_date   DATE
);
```

**fact_dosage** — grain: drug × route × population:
```sql
CREATE TABLE gold.fact_dosage (
  drug_sk            BIGINT,
  route              STRING,   -- 'oral', 'IV', 'topical'
  population         STRING,   -- 'adult', 'pediatric', 'renal impairment'
  dose_value         DOUBLE,
  dose_unit          STRING,
  frequency          STRING,
  source_sentence    STRING,
  label_version      STRING
);
```

**Vector Search:**
- Chunking strategy: section-aware splits (each SPL section = one or more chunks of ~500 tokens with 50-token overlap)
- Embedding model: `databricks-gte-large-en` (hosted, in-perimeter)
- Metadata attached per chunk: `set_id`, `drug_name`, `label_section`, `label_version`, `effective_date`
- Index: Databricks Vector Search, delta-sync mode (auto-updates when Gold table changes)

---

### Agent Layer

**Architecture:** two-tool RAG agent

```
User question
    │
    ▼
Router (LLM)
    │
    ├── [structured query] ──► Tool 1: sql_lookup(Gold tables)
    │                               Returns structured rows
    │
    └── [semantic query]  ──► Tool 2: vector_search(index)
                                    Returns top-k chunks with citations
    │
    ▼
Synthesis (LLM with citations)
    │
    ▼
Answer + source citations
```

**Why two tools?**
The SQL tool handles aggregation questions ("which drugs have > 5 adverse events in the cardiovascular category") that vector search handles poorly. The vector tool handles semantic questions ("what does the label say about renal dosing in elderly patients") where SQL can't help. Routing between them is the "context engineering" layer.

---

## Failure Modes and Mitigations

| Failure | Mitigation |
|---|---|
| PDF parse fails | Route to quarantine; alert on quarantine rate > 5% |
| LLM extraction returns malformed JSON | Retry with stricter prompt; quarantine after 2 retries |
| Confidence score below threshold | Quarantine with reason; human review queue |
| Vector search returns stale embeddings | Delta-sync mode; freshness SLA alert if lag > 4h |
| Cost spike from LLM calls | Per-pipeline DBU budget alert; batch extraction, not streaming |
| Schema drift in source documents | Auto Loader schema evolution + rescued data column |

---

## Unity Catalog Governance

Every table in Silver and Gold has:
- `description`: plain-English explanation of what the table contains
- `owner`: team responsible for SLA
- `quality`: bronze / silver / gold
- `sla_freshness_hours`: maximum acceptable lag from source event
- `pii`: false (explicitly tagged; all data is public regulatory content)
- Column-level descriptions on every non-obvious field

---

## Cost Model (estimates at 5,000 labels)

| Operation | Unit cost | Volume | Estimated total |
|---|---|---|---|
| Auto Loader ingestion | ~$0.001/file | 5,000 | ~$5 |
| `ai_parse_document` | ~$0.005/page × 30 pages avg | 5,000 × 30 | ~$750 |
| `ai_query` extraction | ~$0.01/label | 5,000 | ~$50 |
| Embedding generation | ~$0.0001/chunk × 60 chunks avg | 300,000 | ~$30 |
| Vector Search (hosted) | ~$0.08/hour | always-on | ~$60/month |
| **Total one-time** | | | **~$835** |
| **Total monthly (refresh)** | | new + changed labels only | **~$100–200** |

Re-extraction is the dominant cost. The quarantine layer and confidence threshold exist specifically to avoid re-running expensive LLM calls on already-good rows.
