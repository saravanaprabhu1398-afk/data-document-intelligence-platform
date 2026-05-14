# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 — Test the RAG Agent
# MAGIC
# MAGIC Wires up the `DrugLabelTools` to the live Spark session and Vector Search
# MAGIC index, builds the LangGraph agent, and runs four example questions that
# MAGIC exercise each tool.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Phase 3 fully run: Gold tables populated, Vector Search index `ONLINE`.
# MAGIC - Vector Search endpoint exists: `clinical_docs_vs` (or pass another name).

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade \
# MAGIC     langgraph                 \
# MAGIC     langchain                 \
# MAGIC     langchain-databricks      \
# MAGIC     databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",        "clinical-lab",                             "Catalog")
dbutils.widgets.text("vs_endpoint",    "clinical_docs_vs",                         "Vector Search endpoint")
dbutils.widgets.text("index_name",     "clinical-lab.gold.drug_label_chunks_idx",  "Vector Search index name")
dbutils.widgets.text("model_endpoint", "databricks-meta-llama-3-3-70b-instruct",   "LLM endpoint for the agent")

catalog         = dbutils.widgets.get("catalog")
vs_endpoint     = dbutils.widgets.get("vs_endpoint")
index_name      = dbutils.widgets.get("index_name")
model_endpoint  = dbutils.widgets.get("model_endpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Make the agent code importable
# MAGIC
# MAGIC The agent module lives at `phase4_agent/agent/`. We append the repo root
# MAGIC to `sys.path` so it can be imported with `from agent import build_agent`.
# MAGIC When this notebook runs as part of a Databricks Asset Bundle, the repo
# MAGIC is mounted under `/Workspace/Users/<you>/.bundle/...`.

# COMMAND ----------

import os
import sys

# Walk up from the notebook path until we find the repo root (databricks.yml)
def _find_repo_root(start: str) -> str:
    p = os.path.abspath(start)
    while p != "/" and not os.path.exists(os.path.join(p, "databricks.yml")):
        p = os.path.dirname(p)
    return p

REPO_ROOT = _find_repo_root(os.path.dirname(os.path.abspath("__file__")) or os.getcwd())
sys.path.insert(0, os.path.join(REPO_ROOT, "phase4_agent"))
print(f"repo_root: {REPO_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the agent

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

from agent import build_agent, DrugLabelTools, make_spark_sql_runner

tools_obj = DrugLabelTools(
    catalog     = catalog,
    vs_endpoint = vs_endpoint,
    index_name  = index_name,
    sql         = make_spark_sql_runner(spark),
    vs_client   = VectorSearchClient(disable_notice=True),
)

agent = build_agent(tools_obj=tools_obj, model_endpoint=model_endpoint)
print("Agent ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper — pretty-print a streamed run

# COMMAND ----------

from langchain_core.messages import HumanMessage

def ask(question: str) -> None:
    print(f"\n{'='*80}\nQ: {question}\n{'='*80}")
    final = None
    for step in agent.stream(
        {"messages": [HumanMessage(content=question)]},
        stream_mode = "values",
    ):
        msg = step["messages"][-1]
        # Surface tool calls so we can see the agent's reasoning
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                print(f"  → tool: {tc['name']}({tc.get('args')})")
        elif msg.type == "tool":
            preview = (msg.content or "")[:200].replace("\n", " ")
            print(f"  ← tool result: {preview}…")
        final = msg
    print(f"\nA: {final.content}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Example questions — one per tool, plus a multi-tool case

# COMMAND ----------

ask("What is pembrolizumab approved to treat?")           # → get_drug_summary

# COMMAND ----------

ask("Which drugs in our corpus list pneumonitis as an adverse event?")  # → find_drugs_with_adverse_event

# COMMAND ----------

ask("What does the label say about renal dosing in elderly patients?")  # → search_label_text

# COMMAND ----------

ask(
    "Are there any drugs that share both a cardiovascular adverse event AND "
    "an indication in non-small cell lung cancer? Name two and explain how "
    "their labels describe these risks."
)  # → SQL + vector chained
