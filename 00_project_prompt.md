# Prompt: Multi-Source RAG Chatbot (PDF Policies + Delta Tables) on Databricks

## Context
Build an end-to-end Databricks Gen AI application that answers user questions by
routing to one of two data sources:
1. **Unstructured source** — health insurance policy PDFs, searched via Vector Search.
2. **Structured source** — customer & transaction data in Delta tables, queried via
   auto-generated SQL.

The system must decide per-question which source (or both) to use, retrieve the
answer, and be testable in the AI Playground and deployable as a shared Model
Serving endpoint / Databricks App.

Target catalog/schema for everything below: `rag_demo.insurance` (adjust names as needed).

---

## Phase 1 — Generate Dummy PDF Policy Documents

**Notebook:** `01_generate_policy_pdfs.py`

- Create **10 distinct health insurance policy PDFs**, one policy per file, each
  with realistic structured sections:
  - Policy name, policy number, insurer name
  - Coverage type (Individual / Family / Senior Citizen / Critical Illness / Maternity, etc. — vary across the 10)
  - Sum insured, premium amount, premium frequency
  - Waiting periods (initial, pre-existing disease, specific illness)
  - Inclusions (hospitalization, day-care procedures, ambulance, pre/post hospitalization)
  - Exclusions (cosmetic, self-inflicted, war, specific named exclusions)
  - Claim process steps and required documents
  - Network hospital / cashless claim notes
  - Contact/support details
- Use a PDF library (e.g., `reportlab` or `fpdf2`) to generate real, parseable PDF
  binaries — not plain text saved with a `.pdf` extension.
- Vary length/structure slightly per policy (some with tables, some with bullet
  lists) so downstream chunking/parsing is realistically tested.
- Write all 10 PDFs to a **Unity Catalog Volume**, e.g.
  `/Volumes/rag_demo/insurance/policy_docs/`.
- Print a manifest (filename → policy name → coverage type) at the end for reference.

---

## Phase 2 — Parse PDFs and Build Vector Search Index

**Notebook:** `02_pdf_to_vector_index.py`

- Read all PDFs from the Volume path above.
- Extract text per file (page-aware), then **chunk** (e.g. ~500–800 tokens per
  chunk with slight overlap) — keep `source_file`, `policy_name`, `page_number`,
  `chunk_id` as metadata columns.
- Write chunks to a Delta table: `rag_demo.insurance.policy_chunks`.
- Enable Change Data Feed on that table (required for Delta Sync Vector Search index).
- Create a **Databricks Vector Search endpoint** (if one doesn't already exist) and
  a **Delta Sync Index** on `policy_chunks`, embedding the `content` column using a
  Databricks-hosted embedding model (e.g. `databricks-gte-large-en`).
- Run a couple of test similarity queries (e.g. "What is the waiting period for
  pre-existing diseases?") and print top-k results to confirm retrieval quality.

---

## Phase 3 — Create Structured Data (Customers & Transactions) as Delta Tables

**Notebook:** `03_generate_structured_data.py`

- Generate a dummy **customers** DataFrame (~50–100 rows): `customer_id`, `name`,
  `dob`, `policy_number` (FK to the 10 policies above), `city`, `phone`, `email`,
  `signup_date`.
- Generate a dummy **transactions** DataFrame (~300–500 rows): `transaction_id`,
  `customer_id` (FK), `policy_number`, `transaction_type` (premium_payment /
  claim_payout / refund), `amount`, `transaction_date`, `status`.
- Ensure referential integrity: every `policy_number` in both tables matches one
  of the 10 policies from Phase 1, so a cross-source question ("what does my
  policy cover and what have I paid so far?") is answerable by joining structured
  data with the PDF-derived policy name.
- Write both as managed Delta tables: `rag_demo.insurance.customers` and
  `rag_demo.insurance.transactions`.
- Add table + column comments in Unity Catalog describing each field (this
  metadata will help the SQL-generation step later).

---

## Phase 4 — Routing Agent / Chatbot

**Notebook:** `04_rag_router_agent.py`

Build an agent that, given a natural-language question:

1. **Classifies intent** — does the question need:
   - (a) structured data (aggregates, lookups, "how much", "how many", "list of
     customers who...") → **SQL path**, or
   - (b) unstructured policy knowledge ("what's covered", "what's excluded",
     "waiting period", "how do I claim") → **Vector Search path**, or
   - (c) both (hybrid — e.g., "Is customer X's claim for Y covered under their
     policy?") → run both and merge context.
   - Use an LLM call (Databricks Foundation Model API, e.g. Llama or DBRX) with a
     small few-shot prompt for this classification step — keep it cheap/fast.
2. **SQL path:** pass the question + `customers`/`transactions` table schemas
   (and UC column comments) to the LLM, have it generate a SQL query, execute it
   via Spark SQL / Databricks SQL warehouse, and pass results back to the LLM to
   phrase a final answer.
3. **Vector Search path:** embed the question, query the `policy_chunks` index
   for top-k chunks, pass retrieved context to the LLM to generate the answer
   with citations (source PDF + policy name).
4. **Hybrid path:** run both retrievals, combine context, generate one answer.
5. Return a structured response: `{answer, source_used, retrieved_context, sql_query_if_any}`.

Wrap this as a single Python function/class (`answer_question(question: str) -> dict`)
so it can be logged as an MLflow pyfunc model in the next phase.

---

## Phase 5 — Test in AI Playground

- Log the router agent as an **MLflow model** (pyfunc or `ChatModel` /
  `ResponsesAgent` interface, whichever Databricks Playground supports for
  registered models at the time of the workspace's DBR version).
- Register it to Unity Catalog Model Registry.
- Open the **AI Playground**, load the registered model, and manually test with a
  mix of questions:
  - Pure structured: "How many premium payments has customer C001 made?"
  - Pure unstructured: "What is excluded under the Critical Illness policy?"
  - Hybrid: "Was customer C001's last claim covered under their policy's exclusions?"
- Capture/save a few sample transcripts as a quick eval set.

---

## Phase 6 — Deploy & Share

- Create a **Model Serving endpoint** from the registered model (scale-to-zero
  enabled for a demo).
- Confirm the endpoint responds correctly via a REST call / SDK call.
- Package a minimal front end — either:
  - A **Databricks App** (Lakehouse Apps) with a simple chat UI calling the
    serving endpoint, or
  - A notebook-based chat widget for quick internal sharing.
- Grant appropriate Unity Catalog permissions so intended users can query the
  endpoint/app without needing workspace admin access.

---

## Deliverables Checklist

- [ ] 10 policy PDFs in a UC Volume
- [ ] `policy_chunks` Delta table + Vector Search index (tested with sample queries)
- [ ] `customers` and `transactions` Delta tables with FK-consistent dummy data
- [ ] Router agent notebook with working SQL-path, vector-path, and hybrid-path logic
- [ ] Agent logged + registered in MLflow/Unity Catalog
- [ ] Verified in AI Playground with at least 3 sample questions per path
- [ ] Deployed Model Serving endpoint
- [ ] Shareable front end (Databricks App or notebook UI)

## Constraints / Notes
- Use only Databricks-native components where possible (Foundation Model APIs,
  Vector Search, Unity Catalog, Model Serving) — avoid external vector DBs unless
  specifically requested later.
- All table/volume/index names should live under one catalog+schema
  (`rag_demo.insurance`) for easy cleanup/teardown.
- Treat this as a demo/sandbox build — no production PII, all data synthetic.
