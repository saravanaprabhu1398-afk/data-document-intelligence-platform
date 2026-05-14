"""
Streamlit chat UI for the FDA Drug Label RAG agent.

Runs as a Databricks App (recommended) or locally:

    # Local
    export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
    export DATABRICKS_TOKEN=<pat>
    export WAREHOUSE_ID=<sql_warehouse_id>
    streamlit run app.py

Inside Databricks Apps, all four env vars are populated automatically
when the app is granted access to a SQL warehouse and the Vector
Search endpoint.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make the sibling `agent/` package importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
from databricks.sdk                 import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from langchain_core.messages         import HumanMessage

from agent import build_agent, DrugLabelTools, make_sdk_sql_runner


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title = "Drug Label Intelligence",
    page_icon  = "💊",
    layout     = "wide",
)
st.title("💊 Drug Label Intelligence")
st.caption("Ask questions across FDA drug labels — answers cite their sources.")


# ── Required configuration ───────────────────────────────────────────────────

CATALOG         = os.getenv("CATALOG",        "clinical-lab")
VS_ENDPOINT     = os.getenv("VS_ENDPOINT",    "clinical_docs_vs")
INDEX_NAME      = os.getenv("INDEX_NAME",     "clinical-lab.gold.drug_label_chunks_idx")
MODEL_ENDPOINT  = os.getenv("MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
WAREHOUSE_ID    = os.getenv("WAREHOUSE_ID")

if not WAREHOUSE_ID:
    st.error(
        "WAREHOUSE_ID environment variable is not set. "
        "When running locally, export it. When running in Databricks Apps, "
        "grant the app access to a SQL warehouse in App settings."
    )
    st.stop()


# ── Build the agent once per session ─────────────────────────────────────────

@st.cache_resource(show_spinner="Wiring up agent + tools…")
def get_agent():
    w  = WorkspaceClient()
    vs = VectorSearchClient(disable_notice=True)
    tools_obj = DrugLabelTools(
        catalog     = CATALOG,
        vs_endpoint = VS_ENDPOINT,
        index_name  = INDEX_NAME,
        sql         = make_sdk_sql_runner(w, WAREHOUSE_ID),
        vs_client   = vs,
    )
    return build_agent(tools_obj=tools_obj, model_endpoint=MODEL_ENDPOINT)

agent = get_agent()


# ── Chat state ───────────────────────────────────────────────────────────────

if "history" not in st.session_state:
    st.session_state.history = []   # list of {"role": "user"|"assistant", "content": str, "tool_calls": [...]}

# Render history
for msg in st.session_state.history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("tool_calls"):
            with st.expander(f"Tool calls ({len(msg['tool_calls'])})"):
                for tc in msg["tool_calls"]:
                    st.code(f"{tc['name']}({tc['args']})", language="python")
                    st.text(tc["result"][:1500])

# ── Sidebar with example questions ───────────────────────────────────────────

with st.sidebar:
    st.subheader("Try one of these")
    examples = [
        "What is pembrolizumab approved to treat?",
        "Which drugs list pneumonitis as an adverse event?",
        "What does the label say about renal dosing in elderly patients?",
        "Show me drugs that share a cardiovascular adverse event AND treat lung cancer.",
    ]
    for ex in examples:
        if st.button(ex, key=f"ex_{ex[:20]}"):
            st.session_state.pending = ex
            st.rerun()

    st.divider()
    st.caption(f"Catalog: `{CATALOG}`")
    st.caption(f"Index:   `{INDEX_NAME}`")
    st.caption(f"Model:   `{MODEL_ENDPOINT}`")


# ── New user message ─────────────────────────────────────────────────────────

user_msg = st.chat_input("Ask about a drug, an adverse event, or a clinical question…")
if not user_msg and st.session_state.get("pending"):
    user_msg = st.session_state.pop("pending")

if user_msg:
    st.session_state.history.append({"role": "user", "content": user_msg})
    with st.chat_message("user"):
        st.markdown(user_msg)

    with st.chat_message("assistant"):
        tool_calls_view: list[dict] = []
        tool_call_index: dict[str, dict] = {}
        placeholder = st.empty()

        with st.spinner("Thinking…"):
            for step in agent.stream(
                {"messages": [HumanMessage(content=user_msg)]},
                stream_mode = "values",
            ):
                msg = step["messages"][-1]
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tc in msg.tool_calls:
                        entry = {"name": tc["name"], "args": tc.get("args", {}), "result": ""}
                        tool_calls_view.append(entry)
                        tool_call_index[tc["id"]] = entry
                elif msg.type == "tool":
                    entry = tool_call_index.get(msg.tool_call_id)
                    if entry is not None:
                        entry["result"] = msg.content or ""
                final = msg

            placeholder.markdown(final.content)

        if tool_calls_view:
            with st.expander(f"Tool calls ({len(tool_calls_view)})"):
                for tc in tool_calls_view:
                    st.code(f"{tc['name']}({tc['args']})", language="python")
                    st.text(tc["result"][:1500])

        st.session_state.history.append({
            "role":       "assistant",
            "content":    final.content,
            "tool_calls": tool_calls_view,
        })
