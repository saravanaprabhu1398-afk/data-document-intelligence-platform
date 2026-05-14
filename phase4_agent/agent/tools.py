"""
Tools exposed to the RAG agent.

Two execution contexts are supported:

1. **Databricks notebook**: pass `spark.sql` via `make_spark_sql_runner` —
   tools execute against the live Spark session.
2. **Databricks App / Model Serving**: pass `make_sdk_sql_runner(w, warehouse_id)`
   — tools execute SQL through the Statement Execution API.

Both runners return a list of plain dicts so the rest of the agent doesn't
care which one is wired in.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing      import Any, Callable

from langchain_core.tools import StructuredTool
from pydantic             import BaseModel, Field

# Type for any callable that takes a SQL string and returns rows-as-dicts
SqlRunner = Callable[[str], list[dict[str, Any]]]


# ── SQL runner factories ──────────────────────────────────────────────────────

def make_spark_sql_runner(spark) -> SqlRunner:
    """Wrap a SparkSession into the SqlRunner contract."""
    def run(query: str) -> list[dict[str, Any]]:
        return [r.asDict(recursive=True) for r in spark.sql(query).collect()]
    return run


def make_sdk_sql_runner(w, warehouse_id: str) -> SqlRunner:
    """Wrap a Databricks WorkspaceClient + SQL warehouse into the SqlRunner contract."""
    def run(query: str) -> list[dict[str, Any]]:
        resp = w.statement_execution.execute_statement(
            statement     = query,
            warehouse_id  = warehouse_id,
            wait_timeout  = "30s",
        )
        result = resp.result
        if result is None or result.data_array is None:
            return []
        cols = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in result.data_array]
    return run


# ── Tool argument schemas (Pydantic, so LangChain emits proper JSON schemas) ──

class DrugSummaryArgs(BaseModel):
    drug_name: str = Field(..., description="Brand or generic name of the drug, e.g. 'Keytruda' or 'pembrolizumab'.")

class AdverseEventArgs(BaseModel):
    adverse_event: str = Field(..., description="Adverse event to search for, e.g. 'rash' or 'pneumonitis'.")
    limit:         int = Field(20, description="Maximum number of drugs to return.")

class SemanticSearchArgs(BaseModel):
    query:       str = Field(..., description="Free-text question to search the label corpus for.")
    num_results: int = Field(5,  description="Number of relevant chunks to return.")


# ── Tool implementation ───────────────────────────────────────────────────────

@dataclass
class DrugLabelTools:
    catalog:          str           # e.g. "clinical-lab"
    vs_endpoint:      str           # e.g. "clinical_docs_vs"
    index_name:       str           # e.g. "clinical-lab.gold.drug_label_chunks_idx"
    sql:              SqlRunner
    vs_client:        Any           # databricks.vector_search.client.VectorSearchClient

    # --- bound helpers ---

    @property
    def _cat(self) -> str:
        # backtick-quoted for SQL identifiers (handles hyphens like clinical-lab)
        return f"`{self.catalog}`"

    # --- tool 1: structured summary of a single drug ---

    def get_drug_summary(self, drug_name: str) -> str:
        rows = self.sql(f"""
            SELECT
                d.drug_name,
                d.generic_name,
                d.manufacturer,
                d.effective_date,
                d.indication_summary,
                d.mechanism_of_action,
                (SELECT count(*)
                   FROM {self._cat}.gold.fact_adverse_events f
                  WHERE f.drug_sk = d.drug_sk) AS adverse_event_count
            FROM {self._cat}.gold.dim_drug d
            WHERE lower(d.drug_name)    LIKE lower('%{drug_name}%')
               OR lower(d.generic_name) LIKE lower('%{drug_name}%')
            ORDER BY d.is_current_version DESC, d.effective_date DESC
            LIMIT 5
        """)
        if not rows:
            return f"No drug found matching '{drug_name}' in the Gold dim_drug table."
        return json.dumps(rows, default=str, indent=2)

    # --- tool 2: which drugs report a given adverse event ---

    def find_drugs_with_adverse_event(self, adverse_event: str, limit: int = 20) -> str:
        rows = self.sql(f"""
            SELECT d.drug_name,
                   d.generic_name,
                   d.manufacturer,
                   f.adverse_event
            FROM        {self._cat}.gold.fact_adverse_events f
            INNER JOIN  {self._cat}.gold.dim_drug             d ON d.drug_sk = f.drug_sk
            WHERE lower(f.adverse_event) LIKE lower('%{adverse_event}%')
            GROUP BY d.drug_name, d.generic_name, d.manufacturer, f.adverse_event
            ORDER BY d.drug_name
            LIMIT {limit}
        """)
        if not rows:
            return f"No drugs found whose label lists '{adverse_event}'."
        return json.dumps(rows, default=str, indent=2)

    # --- tool 3: semantic retrieval from the Vector Search index ---

    def search_label_text(self, query: str, num_results: int = 5) -> str:
        idx = self.vs_client.get_index(endpoint_name=self.vs_endpoint, index_name=self.index_name)
        res = idx.similarity_search(
            query_text  = query,
            columns     = ["chunk_id", "set_id", "drug_name", "section_title", "chunk_text"],
            num_results = num_results,
        )
        cols = [c["name"] for c in res.get("manifest", {}).get("columns", [])]
        data = res.get("result", {}).get("data_array", [])
        hits = [dict(zip(cols, row)) for row in data]
        if not hits:
            return f"No relevant chunks found for '{query}'."
        # Trim chunk_text so the model has room to think
        for h in hits:
            ct = h.get("chunk_text", "")
            if ct and len(ct) > 1200:
                h["chunk_text"] = ct[:1200] + " …"
        return json.dumps(hits, default=str, indent=2)

    # --- LangChain wiring ---

    def as_langchain_tools(self) -> list[StructuredTool]:
        return [
            StructuredTool.from_function(
                name        = "get_drug_summary",
                description = (
                    "Look up a structured summary of one drug from the Gold dim_drug table. "
                    "Returns drug name, generic name, manufacturer, indication summary, "
                    "mechanism of action, and adverse event count. "
                    "Use this when the user asks ABOUT a specific drug by name."
                ),
                func        = self.get_drug_summary,
                args_schema = DrugSummaryArgs,
            ),
            StructuredTool.from_function(
                name        = "find_drugs_with_adverse_event",
                description = (
                    "Find drugs whose FDA labels list a given adverse event. "
                    "Returns up to `limit` drug names. "
                    "Use this when the user asks WHICH drugs cause / mention a specific side effect."
                ),
                func        = self.find_drugs_with_adverse_event,
                args_schema = AdverseEventArgs,
            ),
            StructuredTool.from_function(
                name        = "search_label_text",
                description = (
                    "Semantic search across the full text of all FDA drug labels in the corpus. "
                    "Returns the top `num_results` most relevant chunks with drug name, section title, "
                    "and chunk text. "
                    "Use this for open-ended clinical questions where the answer is in label prose "
                    "rather than a structured field."
                ),
                func        = self.search_label_text,
                args_schema = SemanticSearchArgs,
            ),
        ]
