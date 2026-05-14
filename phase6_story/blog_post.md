# I Built a Clinical Document Intelligence Platform in 8 Weeks. Here's What I Learned.

*A medallion lakehouse, structured-output LLM extraction, a RAG agent with two SQL tools and one vector tool, plus the part most portfolio projects skip — a real trust layer.*

---

## The problem

Regulatory and clinical teams spend hours hunting through PDFs and SPL XML documents for answers that should take ten seconds.

> *"Which approved oncology drugs in our corpus list pneumonitis as an adverse event, and what does the label actually say about it?"*

That question touches a structured aggregation (count drugs with event X), a join (which drug has which event), and unstructured retrieval (what does the label *say*). No single tool does all three well. So I built a platform that does, and I'm sharing the design.

---

## What I built

A Databricks lakehouse that:

1. Ingests FDA SPL XML files from a Unity Catalog Volume using **Auto Loader**.
2. Extracts 14 structured fields per label using **`ai_query` with a strict JSON-schema `responseFormat`**, branching low-confidence rows into a quarantine table.
3. Builds a dimensional model — `dim_drug`, `fact_adverse_events` — plus a chunked text table that backs a **Databricks Vector Search** delta-sync index.
4. Exposes everything through a **LangGraph ReAct agent** with three tools: two SQL lookups against the Gold layer, one semantic search against the vector index. The agent picks the tool based on the question and always cites its sources.
5. Sits on top of a **trust layer** — three YAML data contracts, eleven SQL expectations that fail the daily job on regressions, a quality dashboard, and a cost dashboard pulled from `system.billing.usage`.

Total cost to run the dev pipeline once: about $1. Projected steady-state at 5,000 labels: ~$150/month, dominated by the always-on vector search endpoint.

---

## Three design decisions worth defending

### 1. I never used `ai_parse_document`

DailyMed bulk downloads are SPL XML, not PDFs. The naive instinct is to scrape PDFs from each label page so you can demo `ai_parse_document`. I didn't, and the project is better for it.

XML is deeply nested, full of HL7 namespaces, and has tagged sections you can verify against. That makes it a stronger extraction demo, not a weaker one — because the ground truth is right there in the source. The trade-off: I can't claim a "PDF parsing" headline. But extraction quality is higher and reruns cost nothing.

### 2. Tools take an injected SQL runner

The agent has three tools. They look like simple Python functions, but the SQL runner is a dependency:

```python
@dataclass
class DrugLabelTools:
    sql:        Callable[[str], list[dict]]   # injected
    vs_client:  Any                           # injected
    ...
```

In a Databricks notebook the SQL runner is `lambda q: [r.asDict() for r in spark.sql(q).collect()]`. In the Streamlit app deployed on Databricks Apps, it's a wrapper around the Statement Execution API and a SQL warehouse. One agent codebase, two runtime environments, zero duplication. This is what's meant by "context engineering" in the practical sense.

### 3. The trust layer is its own pipeline

Most portfolio projects bolt on a few `not null` checks at the end and call it observability. I gave the trust layer its own dedicated schema, its own tables, its own dashboards, and its own daily Lakeflow Job that runs **after** Gold and **fails hard** on error-severity violations.

Three YAML data contracts (`dim_drug`, `fact_adverse_events`, `drug_label_chunks`) declare ownership, SLA hours, completeness targets, and assertions. The runner reads them, executes SQL checks, lands violations in `trust.expectation_violations`, and raises an `AssertionError` on errors so Lakeflow's on-failure email fires.

The cost dashboard joins `system.billing.usage` with `system.billing.list_prices` to produce per-job USD spend, broken down by stage. If you don't have system tables enabled on your workspace, you're flying blind.

---

## What surprised me

**Serverless compute does not support `.cache()`.** I learned this the painful way when my Silver extraction ran `ai_query` twice — once for the "good" branch and once for the "quarantine" branch — doubling the LLM cost. The fix: materialize the `ai_query` results to a scratch Delta table immediately, then drop it at the end of the notebook.

**Catalog names with hyphens break unquoted SQL identifiers.** My workspace had a pre-existing `clinical-lab` catalog. Every `spark.sql(f"... {catalog} ...")` blew up until I introduced `cat = f"\`{catalog}\`"` and used it everywhere.

**The DailyMed API endpoint that returns SPL ZIPs is not the same one the docs suggest.** `/services/v2/spls/{id}.zip` returns an HTML 404 page with HTTP 200. The real download path is `getFile.cfm?uniqid={id}&type=zip`. Better yet, fetch the XML directly via `/services/v2/spls/{id}.xml` — no ZIP extraction needed.

**`drug_class_moa` is not the right filter for "antineoplastic agents".** That parameter is for mechanism-of-action terms like "EGFR Inhibitor". For therapeutic classes use `drug_class_epc` (Established Pharmacologic Class). All my early downloads returned 404s because the API was returning withdrawn label IDs.

---

## What I'd do with another month

- **`MERGE` for fact tables** to preserve adverse-event history per label version.
- **A `fact_dosage` table** parsing the free-text dosage instructions into (drug × route × population × frequency).
- **Replace the three hand-built SQL tools with a SQL-generating agent tool** so the agent can answer arbitrary aggregation questions, not just the three I anticipated.
- **Lakehouse Monitoring** on top of the SQL expectation framework for drift detection on the Silver extraction quality.

---

## How to run it yourself

The repo is on GitHub: `<your-handle>/data-document-intelligence-platform`. README has a six-step reproduction guide. You'll need a Databricks workspace with Unity Catalog and a Vector Search endpoint enabled. Total time to populate Bronze → Silver → Gold → Vector Search → Trust on 50 labels: under 15 minutes.

---

*Built on Databricks. Code, decisions, and the cost model are all in the repo. If you want to chat about lakehouse architecture, GenAI extraction, or how to make a trust layer that hiring managers actually care about — find me on LinkedIn.*
