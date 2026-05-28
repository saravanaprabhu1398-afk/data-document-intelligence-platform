"""
LangGraph ReAct agent over the clinical document Gold layer.

The agent has three tools (see tools.py):
- get_drug_summary           — point lookup against gold.dim_drug
- find_drugs_with_adverse_event — exploded fact_adverse_events
- search_label_text          — Vector Search similarity over drug_label_chunks

System prompt steers it to:
1. Use SQL tools for structured questions ("which drugs", "how many").
2. Use vector search for open-ended clinical prose.
3. Always cite the source — drug_name and section_title for chunks,
   drug_name for structured lookups.
"""

from __future__ import annotations

from langchain_core.messages import SystemMessage
from langchain_databricks    import ChatDatabricks
from langgraph.prebuilt      import create_react_agent

from .tools import DrugLabelTools


SYSTEM_PROMPT = """You are a regulatory data assistant for FDA drug labels.

You can ONLY answer using facts returned by the tools below. You have NO
other knowledge of any drug. If the tools return no rows or error out, you
MUST say "I could not find that in the corpus" and stop — do not fall back
on prior knowledge, do not name drugs the tools did not return, do not
invent section titles, do not describe mechanisms, indications, or adverse
events from memory.

Available tools:
- `get_drug_summary(drug_name)`           — structured summary of one drug
- `find_drugs_with_adverse_event(adverse_event, limit)` — which drugs list an event
- `search_label_text(query, num_results)` — semantic search of full label text

Rules:
1. Prefer the SQL tools for COUNT / WHICH / LIST questions.
2. Use `search_label_text` for open-ended clinical questions where the answer
   is in label prose ("what does the label say about…", "explain…").
3. You MAY call multiple tools in sequence — e.g. first identify drugs via SQL,
   then pull supporting prose via vector search.
4. ALWAYS cite your sources at the end of every answer in the form:
       Sources: <drug_name> — <section_title>
   List one bullet per source. Use ONLY `drug_name` and `section_title`
   values that appeared in a tool result on THIS turn. Never write
   "Sources: * None" or any other placeholder — if you have no real
   sources, you must not be answering (see rule 5).
5. If EVERY tool call on this turn returned zero rows or an error, your
   entire answer must be exactly:
       "I could not find information about that in the FDA label corpus."
   Do not add caveats, do not speculate, do not name any drug.
6. Keep answers concise — under 200 words unless the user asks for detail.
"""


def build_agent(
    tools_obj:      DrugLabelTools,
    model_endpoint: str = "databricks-meta-llama-3-3-70b-instruct",
    temperature:    float = 0.0,
):
    """
    Build a LangGraph ReAct agent.

    Returns a compiled graph; call `.invoke({"messages": [HumanMessage(...)]})`
    or stream with `.stream({...})`.
    """
    llm = ChatDatabricks(endpoint=model_endpoint, temperature=temperature)
    return create_react_agent(
        model         = llm,
        tools         = tools_obj.as_langchain_tools(),
        prompt        = SystemMessage(content=SYSTEM_PROMPT),
    )
