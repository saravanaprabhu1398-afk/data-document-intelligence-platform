# 5-Minute Video Walkthrough Script

Target audience: senior data + ML engineers, hiring managers in life sciences.
Tone: confident, specific, no hand-waving on trade-offs.
Tools to capture: screen recorder + headset mic. Show real Databricks UI throughout.

---

## 0:00 – 0:30  Hook

> **On screen:** open the Streamlit app, type the question, hit enter.

> "Imagine asking *'which oncology drugs in our corpus list pneumonitis as an adverse event, and what does the label say about it?'* across 5,000 FDA drug labels — and getting back a cited answer in under 10 seconds. That's what this platform does."

[Show the agent answer with the `Sources:` footer rendering.]

> "I built this on Databricks in six phases, from raw ingestion to a governed, observable lakehouse with a RAG agent on top. Let me walk you through it."

---

## 0:30 – 1:30  Architecture overview (60 seconds)

> **On screen:** the Mermaid architecture diagram from the README.

> "The pipeline is the standard medallion plus three Databricks-native things on top.
>
> 1. **Bronze**: Auto Loader watches a Unity Catalog Volume and lands raw SPL XML into Delta.
> 2. **Silver**: a Python UDF parses the XML, then `ai_query` extracts 14 structured fields with a JSON-schema `responseFormat`. Anything that scores below 0.6 confidence goes to a quarantine table.
> 3. **Gold**: a `MERGE`-based `dim_drug`, a `fact_adverse_events` for analytics, and a chunks table that backs a Databricks Vector Search delta-sync index.
> 4. **Agent**: a LangGraph ReAct agent with three tools — two SQL lookups and one vector search — that picks which one to use based on the question.
> 5. **Trust**: this is the part most demos skip — three YAML data contracts, eleven SQL expectations that fail the job on regressions, and a cost dashboard pulling from `system.billing.usage`."

---

## 1:30 – 3:00  Live demo (90 seconds)

> **On screen:** the Streamlit chat UI side-by-side with the Catalog Explorer.

> "Let me run three questions, one per tool, then a chained one."

**Demo Q1 — point lookup:**
> "*What is pembrolizumab approved to treat?*"

[Show the tool-call panel: `get_drug_summary({"drug_name": "pembrolizumab"})`. Highlight the cited drug name + indication in the response.]

> "That's a structured row from `gold.dim_drug`. No LLM hallucination — the agent went to SQL."

**Demo Q2 — aggregation:**
> "*Which drugs in our corpus list pneumonitis?*"

[Show `find_drugs_with_adverse_event` firing. Point at the result list.]

> "Join across the `dim_drug` and `fact_adverse_events` tables. Same agent, different tool, picked automatically."

**Demo Q3 — open-ended clinical:**
> "*What does the label say about renal dosing in elderly patients?*"

[Show `search_label_text` returning chunks with `drug_name` and `section_title`.]

> "Now the agent reaches for Vector Search because the answer isn't in any single column — it's in label prose. Notice every chunk comes back with the drug name and section title, which becomes a citation in the final answer."

---

## 3:00 – 4:00  Behind the scenes (60 seconds)

> **On screen:** the trust layer dashboard.

> "Three things I want to point out that you usually don't see in portfolio projects.
>
> **First**, every Gold table has a written contract. Here's `dim_drug.yaml` — owner, SLA, completeness target, business rules, expectations. The runner reads these and fails the job on error-severity violations.

[Cut to the quality dashboard: pass-rate trend chart, top failing checks table.]

> **Second**, this dashboard. Pass rate over time, top failing checks, confidence-score distribution from Silver, quarantine watch. If anything regresses, the team sees it the next morning.

[Cut to the cost dashboard.]

> **Third**, real cost tracking. This pulls from `system.billing.usage` joined to `list_prices`, broken down by Lakeflow Job name. At dev scale this whole pipeline costs about a dollar per run; at 5,000 labels we project a hundred dollars one-time plus about a hundred fifty a month for the always-on Vector Search endpoint."

---

## 4:00 – 5:00  Trade-offs and close (60 seconds)

> **On screen:** the README "What I'd do differently" section.

> "Three trade-offs worth being honest about.
>
> 1. I use SPL XML, not PDF. DailyMed bulk downloads are XML — and frankly, structured XML noise is a better extraction demo than PDF parsing because we have ground truth tagged in the source.
> 2. `fact_adverse_events` is rebuilt with `TRUNCATE+INSERT`. Cheap and idempotent, but loses adverse-event history. With another sprint I'd switch to `MERGE`.
> 3. The trust framework is hand-rolled SQL. Works fine here; in a real environment I'd layer Lakehouse Monitoring on top for drift detection.
>
> The repo is at github.com/<your-handle>/data-document-intelligence-platform — README has the full architecture, decision log, and reproduction steps. Thanks for watching."

[End on the README, scroll briefly through the phase tracker showing all ✅.]

---

## Capture checklist

- [ ] Streamlit app loaded with at least 50 labels in Bronze, Silver, Gold
- [ ] Vector Search index status `ONLINE`
- [ ] Trust dashboards have at least one full run rendered
- [ ] Cost dashboard has 1–2 days of `system.billing.usage` data
- [ ] Audio recorded separately on a USB mic, NOT laptop mic
- [ ] 1080p capture, then upload to YouTube unlisted + LinkedIn native
