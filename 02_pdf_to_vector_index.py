# Databricks notebook source
# MAGIC %pip install pypdf databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "insurance"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/policy_docs"
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.policy_chunks"
VS_ENDPOINT_NAME = "rag_insurance_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.policy_chunks_index"
EMBEDDING_MODEL_ENDPOINT = "databricks-gte-large-en"

print(f"Reading PDFs from: {VOLUME_PATH}")
print(f"Chunks table: {CHUNKS_TABLE}")
print(f"Vector Search endpoint: {VS_ENDPOINT_NAME}")
print(f"Vector Search index: {VS_INDEX_NAME}")

# COMMAND ----------

import os
from pypdf import PdfReader

CHUNK_SIZE = 400
CHUNK_OVERLAP = 50

def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return [c for c in chunks if c.strip()]


pdf_files = [f for f in os.listdir(VOLUME_PATH) if f.lower().endswith(".pdf")]
print(f"Found {len(pdf_files)} PDF files")

rows = []
for filename in sorted(pdf_files):
    full_path = os.path.join(VOLUME_PATH, filename)
    reader = PdfReader(full_path)

    # first non-empty line of page 1 = policy name (per generator layout)
    first_page_text = reader.pages[0].extract_text() or ""
    policy_name = next((l.strip() for l in first_page_text.split("\n") if l.strip()), filename)

    chunk_idx = 0
    for page_num, page in enumerate(reader.pages, start=1):
        page_text = page.extract_text() or ""
        for chunk in chunk_text(page_text):
            rows.append({
                "chunk_id": f"{filename}_p{page_num}_c{chunk_idx}",
                "source_file": filename,
                "policy_name": policy_name,
                "page_number": page_num,
                "content": chunk,
                "char_count": len(chunk),
            })
            chunk_idx += 1

print(f"Generated {len(rows)} chunks from {len(pdf_files)} PDFs")

# COMMAND ----------

chunks_df = spark.createDataFrame(rows)
display(chunks_df.limit(5))

(chunks_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CHUNKS_TABLE))

spark.sql(f"ALTER TABLE {CHUNKS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"Wrote {chunks_df.count()} rows to {CHUNKS_TABLE}, CDF enabled")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

existing_endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if VS_ENDPOINT_NAME not in existing_endpoints:
    print(f"Creating Vector Search endpoint: {VS_ENDPOINT_NAME} (this can take several minutes)")
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT_NAME, endpoint_type="STANDARD")
else:
    print(f"Endpoint {VS_ENDPOINT_NAME} already exists, waiting for it to be ready if needed")
    vsc.wait_for_endpoint(VS_ENDPOINT_NAME)

print("Vector Search endpoint ready.")

# COMMAND ----------

try:
    existing_index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
    print(f"Index {VS_INDEX_NAME} already exists. Syncing...")
    existing_index.sync()
except Exception:
    print(f"Creating Delta Sync index: {VS_INDEX_NAME} (this can take a few minutes)")
    vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
        source_table_name=CHUNKS_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="chunk_id",
        embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
    )

print("Vector Search index ready.")

# COMMAND ----------

index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)

test_queries = [
    "What is the waiting period for pre-existing diseases?",
    "Is maternity covered and what is the waiting period?",
    "What is excluded under the critical illness policy?",
    "How do I file a cashless claim?",
]

for q in test_queries:
    print(f"\n{'='*80}\nQUERY: {q}\n{'='*80}")
    results = index.similarity_search(
        query_text=q,
        columns=["chunk_id", "policy_name", "source_file", "page_number", "content"],
        num_results=3,
    )
    for row in results.get("result", {}).get("data_array", []):
        chunk_id, policy_name, source_file, page_number, content, score = row
        print(f"\n[score={score:.4f}] {policy_name} ({source_file}, page {page_number})")
        print(content[:250].replace("\n", " ") + "...")

print("\n\nPhase 2 complete: PDFs parsed, chunked, indexed, and test queries verified.")
