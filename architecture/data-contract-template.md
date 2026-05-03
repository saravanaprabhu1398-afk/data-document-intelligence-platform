# Data Contract Template

Use this template for every Gold table. Fill in one file per table in `phase5_trust/contracts/`.

---

```yaml
# Data Contract: <table_name>
# Version: 1.0
# Last updated: YYYY-MM-DD

table:
  catalog: clinical_docs
  schema: gold
  name: <table_name>
  description: >
    One sentence describing what this table contains and the grain of one row.

metadata:
  owner: <team-name>
  steward: <person or alias>
  created: YYYY-MM-DD
  quality_tier: gold
  pii: false
  source_tables:
    - silver.drug_label_extracted

sla:
  freshness_hours: 24       # max acceptable lag from source event
  completeness_pct: 99.0    # minimum non-null rate for required fields
  row_count_lower_bound: 1000
  alert_channel: "#data-alerts"

schema:
  columns:
    - name: <column_name>
      type: STRING
      nullable: false
      description: >
        Plain-English description. Include units, valid values, or example.
    # repeat for each column

business_rules:
  - "drug_sk must reference a valid row in gold.dim_drug"
  - "effective_date must not be in the future"
  # add table-specific rules

expectations:
  - type: not_null
    columns: [drug_sk, adverse_event, label_version]
  - type: unique
    columns: [drug_sk, adverse_event, label_section, label_version]
  - type: accepted_values
    column: event_category
    values: [cardiovascular, neurological, gastrointestinal, dermatological, other]
  - type: row_count
    min: 1000

changelog:
  - version: "1.0"
    date: YYYY-MM-DD
    author: <name>
    changes: "Initial contract"
```
