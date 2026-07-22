# Databricks notebook source
# MAGIC %pip install --upgrade mlflow databricks-vectorsearch databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "insurance"
VS_ENDPOINT_NAME = "rag_insurance_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.policy_chunks_index"
SQL_WAREHOUSE_ID = "3a9e8611697fbf8a"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.insurance_rag_router"

print(f"LLM endpoint: {LLM_ENDPOINT}")
print(f"Vector index: {VS_INDEX_NAME}")
print(f"SQL warehouse: {SQL_WAREHOUSE_ID}")
print(f"UC model name: {UC_MODEL_NAME}")

# COMMAND ----------

import re
import json
import time
import mlflow
from mlflow.pyfunc import PythonModel
from mlflow.models.resources import (
    DatabricksVectorSearchIndex,
    DatabricksSQLWarehouse,
    DatabricksServingEndpoint,
)

FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|MERGE)\b", re.IGNORECASE
)

TABLE_SCHEMA_CONTEXT = """
Table: workspace.insurance.customers
  Description: Health insurance customers, one row per policyholder, linked to a policy_number.
  Columns: customer_id (string, PK, e.g. C0001), name (string), dob (date), policy_number (string, FK to policy),
           city (string), phone (string), email (string), signup_date (date)

Table: workspace.insurance.transactions
  Description: Financial transactions per customer: premium payments, claim payouts, and refunds.
  Columns: transaction_id (string, PK), customer_id (string, FK), policy_number (string, FK),
           transaction_type (string: premium_payment | claim_payout | refund),
           amount (double), transaction_date (date), status (string: completed | pending | failed)

Known policy_numbers: SL-IND-1001 (Individual Basic), SL-FAM-2002 (Family Floater),
SL-SEN-3003 (Senior Citizen), SL-CI-4004 (Critical Illness), SL-MAT-5005 (Maternity Plus),
SL-TOP-6006 (Super Top-Up), SL-DIA-7007 (Diabetes Safe), SL-GRP-8008 (Corporate Group),
SL-ACC-9009 (Personal Accident), SL-INT-1010 (International Travel Health)
"""

print("Config and schema context loaded")

# COMMAND ----------

class InsuranceRAGRouter(PythonModel):

    def load_context(self, context):
        from databricks.sdk import WorkspaceClient
        from databricks.vector_search.client import VectorSearchClient

        self.ws = WorkspaceClient()
        self.vsc = VectorSearchClient()
        self.index = self.vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)

    def _call_llm(self, system_prompt: str, user_prompt: str, max_tokens: int = 600) -> str:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

        resp = self.ws.serving_endpoints.query(
            name=LLM_ENDPOINT,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
                ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
            ],
            max_tokens=max_tokens,
            temperature=0.1,
        )
        return resp.choices[0].message.content.strip()

    def _classify_intent(self, question: str) -> str:
        system_prompt = (
            "You classify a user's question about health insurance into exactly one label: "
            "SQL, VECTOR, or HYBRID.\n"
            "- SQL: needs structured customer/transaction data (counts, sums, lookups by name/id, "
            "'how much', 'how many', 'list of customers who...').\n"
            "- VECTOR: needs policy document knowledge (coverage, exclusions, waiting periods, "
            "claim process, definitions).\n"
            "- HYBRID: needs both (e.g. checking if a specific customer's claim is covered under "
            "their policy's terms).\n"
            "Reply with exactly one word: SQL, VECTOR, or HYBRID."
        )
        label = self._call_llm(system_prompt, question, max_tokens=10).upper()
        for candidate in ("HYBRID", "SQL", "VECTOR"):
            if candidate in label:
                return candidate
        return "VECTOR"

    def _run_sql(self, question: str):
        system_prompt = (
            "You are a SQL expert for Databricks SQL (Spark SQL syntax). Given the table schema "
            "below, write ONE read-only SELECT query that answers the user's question. "
            "Only return the raw SQL, no markdown, no explanation.\n\n" + TABLE_SCHEMA_CONTEXT
        )
        sql_query = self._call_llm(system_prompt, question, max_tokens=400)
        sql_query = re.sub(r"^```sql|```$", "", sql_query, flags=re.IGNORECASE).strip("` \n")

        if not re.match(r"^\s*(SELECT|WITH)\b", sql_query, re.IGNORECASE) or FORBIDDEN_SQL_KEYWORDS.search(sql_query):
            return {"sql_query": sql_query, "rows": [], "error": "Generated query failed safety check; refused to execute."}

        try:
            stmt = self.ws.statement_execution.execute_statement(
                warehouse_id=SQL_WAREHOUSE_ID, statement=sql_query, wait_timeout="30s"
            )
            statement_id = stmt.statement_id
            for _ in range(30):
                stmt = self.ws.statement_execution.get_statement(statement_id)
                if stmt.status.state.value in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                    break
                time.sleep(2)

            if stmt.status.state.value != "SUCCEEDED":
                return {"sql_query": sql_query, "rows": [], "error": f"state={stmt.status.state.value}, detail={stmt.status.error}"}

            columns = [c.name for c in stmt.manifest.schema.columns]
            rows = [dict(zip(columns, row)) for row in (stmt.result.data_array or [])]
            return {"sql_query": sql_query, "rows": rows, "error": None}
        except Exception as e:
            return {"sql_query": sql_query, "rows": [], "error": f"exception: {e}"}

    def _run_vector_search(self, question: str, k: int = 5):
        try:
            results = self.index.similarity_search(
                query_text=question,
                columns=["policy_name", "source_file", "page_number", "content"],
                num_results=k,
            )
        except Exception as e:
            return {"chunks": [], "error": str(e)}
        rows = results.get("result", {}).get("data_array", [])
        chunks = [
            {"policy_name": r[0], "source_file": r[1], "page_number": r[2], "content": r[3], "score": r[4]}
            for r in rows
        ]
        return {"chunks": chunks, "error": None}

    def _generate_answer(self, question: str, sql_context, vector_context) -> str:
        context_parts = []
        if sql_context is not None:
            context_parts.append(f"STRUCTURED DATA (from SQL query):\n{json.dumps(sql_context['rows'], default=str)[:3000]}")
        if vector_context is not None and vector_context["chunks"]:
            excerpts = "\n\n".join(
                f"[{c['policy_name']}, page {c['page_number']}]: {c['content'][:400]}" for c in vector_context["chunks"]
            )
            context_parts.append(f"POLICY DOCUMENT EXCERPTS:\n{excerpts}")

        system_prompt = (
            "You are a helpful health insurance assistant. Answer the user's question using ONLY "
            "the context provided below. If policy excerpts are used, mention which policy they come "
            "from. If the context doesn't contain the answer, say so plainly. Be concise."
        )
        user_prompt = f"QUESTION: {question}\n\n" + "\n\n".join(context_parts)
        return self._call_llm(system_prompt, user_prompt, max_tokens=500)

    def predict(self, context, model_input):
        import pandas as pd

        if isinstance(model_input, pd.DataFrame):
            questions = model_input.iloc[:, 0].tolist()
        elif isinstance(model_input, dict):
            questions = [model_input.get("question", model_input.get("messages", ""))]
        else:
            questions = [str(model_input)]

        outputs = []
        for question in questions:
            intent = self._classify_intent(question)

            sql_result = None
            vector_result = None

            if intent in ("SQL", "HYBRID"):
                sql_result = self._run_sql(question)
            if intent in ("VECTOR", "HYBRID"):
                vector_result = self._run_vector_search(question)

            answer = self._generate_answer(question, sql_result, vector_result)

            outputs.append({
                "question": question,
                "source_used": intent,
                "answer": answer,
                "sql_query": sql_result["sql_query"] if sql_result else None,
                "sql_rows_returned": len(sql_result["rows"]) if sql_result else None,
                "sql_error": sql_result["error"] if sql_result else None,
                "vector_chunks_used": len(vector_result["chunks"]) if vector_result else None,
                "vector_search_error": vector_result["error"] if vector_result else None,
            })

        return outputs if len(outputs) > 1 else outputs[0]


print("InsuranceRAGRouter class defined")

# COMMAND ----------

# Quick functional test before logging to MLflow
agent = InsuranceRAGRouter()
agent.load_context(None)

test_questions = [
    "What is the waiting period for pre-existing diseases under the Family Floater policy?",
    "How many customers are on the Senior Citizen policy?",
    "What is excluded under the Critical Illness policy?",
    "How much total claim payout has been made for the Diabetes Safe policy?",
]

for q in test_questions:
    print(f"\n{'='*90}\nQ: {q}")
    try:
        result = agent.predict(None, {"question": q})
        print(f"Source used: {result['source_used']}")
        print(f"Answer: {result['answer']}")
        if result["sql_query"]:
            print(f"SQL: {result['sql_query']}")
        if result.get("vector_search_error"):
            print(f"(vector search degraded: {result['vector_search_error']})")
    except Exception as e:
        print(f"Question failed (continuing): {e}")

print("\n\nFunctional test complete.")

# COMMAND ----------

import pandas as pd

input_example = pd.DataFrame({"question": ["What is the waiting period for maternity coverage?"]})
signature = mlflow.models.infer_signature(
    input_example,
    [{"question": "x", "source_used": "VECTOR", "answer": "x", "sql_query": None,
      "sql_rows_returned": None, "sql_error": None, "vector_chunks_used": 3, "vector_search_error": None}],
)

resources = [
    DatabricksVectorSearchIndex(index_name=VS_INDEX_NAME),
    DatabricksSQLWarehouse(warehouse_id=SQL_WAREHOUSE_ID),
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
]

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="insurance_rag_router") as run:
    model_info = mlflow.pyfunc.log_model(
        name="router_agent",
        python_model=InsuranceRAGRouter(),
        input_example=input_example,
        signature=signature,
        resources=resources,
        pip_requirements=["mlflow", "databricks-vectorsearch", "databricks-sdk", "pandas"],
        registered_model_name=UC_MODEL_NAME,
    )

print(f"\nModel logged and registered: {UC_MODEL_NAME}")
print(f"Run ID: {run.info.run_id}")
print(f"Model URI: {model_info.model_uri}")
