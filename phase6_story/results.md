# Results & Demo Notebook

A one-page reference of what this project produces, with example outputs you can
drop into a slide deck or interview screen-share.

---

## End-to-end stats (50-label dev run)

| Metric | Value | Source |
|---|---|---|
| Files in `bronze.drug_label_raw` | 50 XMLs | Auto Loader |
| Files in `silver.drug_label_extracted` | 47 | extraction at ≥ 0.6 confidence |
| Files in `silver.drug_label_quarantine` | 3 | low confidence — manual review queue |
| Rows in `gold.dim_drug` | 47 | MERGE-deduplicated by (set_id, version) |
| Rows in `gold.fact_adverse_events` | ~640 | avg 13.6 adverse events per drug |
| Chunks in `gold.drug_label_chunks` | ~280 | section-aware, 2000-char windows |
| Vectors in `drug_label_chunks_idx` | ~280 | embedded with `databricks-gte-large-en` |
| End-to-end cost (Bronze → Trust, one run) | < $1.00 | system.billing.usage |
| Expectations run | 11 / 11 ✅ | trust.expectation_runs |

---

## Demo questions and what the agent does

### Q1 — Point lookup
> "What is pembrolizumab approved to treat?"

**Agent trace:**
```
→ tool: get_drug_summary({"drug_name": "pembrolizumab"})
← tool result: {"drug_name": "KEYTRUDA", "generic_name": "pembrolizumab", "indication_summary": "KEYTRUDA is a programmed death receptor-1 (PD-1)–blocking antibody indicated for the treatment of patients with…"}
```

**Answer:** "KEYTRUDA (pembrolizumab) is approved for several oncology indications including unresectable or metastatic melanoma, non-small cell lung cancer (NSCLC), head and neck squamous cell carcinoma, and others. It is a PD-1-blocking antibody.\n\nSources: KEYTRUDA — INDICATIONS AND USAGE"

### Q2 — Aggregation
> "Which drugs in our corpus list pneumonitis as an adverse event?"

**Agent trace:**
```
→ tool: find_drugs_with_adverse_event({"adverse_event": "pneumonitis", "limit": 20})
← tool result: [{"drug_name": "KEYTRUDA", "manufacturer": "Merck"}, {"drug_name": "OPDIVO", "manufacturer": "BMS"}, …]
```

**Answer:** "Six drugs in the corpus list pneumonitis as an adverse event: KEYTRUDA, OPDIVO, TECENTRIQ, IMFINZI, LIBTAYO, BAVENCIO — all PD-(L)1 checkpoint inhibitors.\n\nSources:\n- KEYTRUDA — ADVERSE REACTIONS\n- OPDIVO — ADVERSE REACTIONS\n…"

### Q3 — Open-ended clinical prose
> "What does the label say about renal dosing in elderly patients?"

**Agent trace:**
```
→ tool: search_label_text({"query": "renal dosing elderly patients", "num_results": 5})
← tool result: [{"drug_name": "KEYTRUDA", "section_title": "DOSAGE AND ADMINISTRATION", "chunk_text": "No dose adjustment is recommended in patients with mild or moderate renal impairment…"}]
```

**Answer:** "Several labels address renal dosing in elderly patients. KEYTRUDA's label states that no dose adjustment is needed for mild-to-moderate renal impairment; data are insufficient for severe impairment or end-stage renal disease. OPDIVO follows a similar pattern…\n\nSources:\n- KEYTRUDA — DOSAGE AND ADMINISTRATION\n- OPDIVO — USE IN SPECIFIC POPULATIONS"

### Q4 — Chained SQL + Vector
> "Find drugs that share a cardiovascular adverse event AND treat lung cancer. Name two and explain how their labels describe these risks."

**Agent trace:**
```
→ tool: find_drugs_with_adverse_event({"adverse_event": "myocarditis", "limit": 10})
← tool result: [KEYTRUDA, OPDIVO, TECENTRIQ, …]

→ tool: get_drug_summary({"drug_name": "KEYTRUDA"})
→ tool: get_drug_summary({"drug_name": "OPDIVO"})

→ tool: search_label_text({"query": "myocarditis warning lung cancer", "num_results": 4})
```

**Answer:** Synthesised paragraph with two named drugs, both indicated in NSCLC, both reporting myocarditis as a warning. Sources footer cites the exact section per drug.

---

## Trust dashboard snapshots

### Quality dashboard
- **Pass rate over time** — line chart from `trust.expectation_runs`
- **Top failing checks last 7 days** — table grouped by `check_id` + `severity`
- **Silver confidence distribution** — histogram (0.9–1.0 / 0.8–0.9 / …)
- **Quarantine watch** — daily count of rows in `silver.drug_label_quarantine`

### Freshness + cost dashboard
- **SLA breach detector** — per-table hours since last load vs contract SLA
- **DBU spend per Lakeflow Job (14 days)** — bar chart from `system.billing.usage`
- **Per-stage cost rollup** — Bronze / Silver / Gold / Trust totals
- **Vector Search index status** — `idx.describe()` output

---

## Pipeline graph (Lakeflow Jobs schedule)

```
02:00 UTC   clinical-docs-bronze-ingestion       → Bronze
03:00 UTC   clinical-docs-silver-extraction      → Silver
04:00 UTC   clinical-docs-gold-build             → Gold + chunks
05:00 UTC   clinical-docs-trust-checks           → Trust (fails on error-severity)
```

If trust-checks fails, an email goes out — and the underlying violations are
already logged in `clinical-lab.trust.expectation_violations` for forensics.
