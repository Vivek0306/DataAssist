# DataAssist — Database Intelligence Agent

A lightweight, schema-aware natural language agent built on top of a PostgreSQL data warehouse. Part of a complete end-to-end data engineering portfolio project.

---

## Project Overview

DataAssist consists of two layers:

**1. ELT Pipeline** — extracts supply chain data from Snowflake (TPC-H benchmark), processes it with PySpark, and loads it incrementally into a local PostgreSQL warehouse orchestrated by Apache Airflow.

**2. Intelligence Agent** — sits on top of the warehouse and understands the database schema automatically. Ask it a question in plain English — it figures out whether to answer from schema context or generate SQL, execute it, and return a business-friendly answer.

---

## Architecture

```
Snowflake (TPC-H)
      │
      │  PySpark extraction
      ▼
PostgreSQL (local)
  ├── retail_raw       ← full load landing zone
  └── retail_staging   ← clean incremental layer
            │
            │  Agent reads from retail_staging
            ▼
┌──────────────────────────────────┐
│          DataAssist Agent        │
│                                  │
│  metadata_generator.py           │
│  prompt_builder.py               │
│  llm_client.py                   │
│  query_runner.py                 │
│  agent.py                        │
└──────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source | Snowflake (TPC-H benchmark) |
| Extraction & Load | PySpark |
| Warehouse | PostgreSQL (local) |
| Orchestration | Apache Airflow |
| Incremental Load | Python + psycopg2 (watermark pattern) |
| LLM  Model | qwen2.5-coder-32b-instruct |

---

## Agent Module Breakdown

```
agent/
├── metadata_generator.py   ← introspects PostgreSQL and builds metadata.json
├── metadata.json           ← auto-generated schema snapshot (gitignored)
├── business_context.json   ← encoded values, derived metrics, domain gotchas
├── llm_client.py           ← LLM API wrapper
├── prompt_builder.py       ← assembles schema context + question into prompt
├── query_runner.py         ← executes SQL safely (read-only, row-limited)
└── agent.py                ← main entry point, orchestrates the full flow
```

### How the agent works

```
User question
      │
      ▼
Is this a schema/metadata question?
      │
   YES│                      NO
      │                       │
      ▼                       ▼
Answer directly         Send to LLM with
from metadata           full schema context
                               │
                               ▼
                        Did LLM return SQL?
                               │
                        YES│        NO
                           │         │
                           ▼         ▼
                       Execute    Return answer
                        query       as-is
                           │
                           ▼
                     Format results
                           │
                           ▼
                     Second LLM call
                     for natural language
                     summary
                           │
                           ▼
                     Return to user
```

1. **Schema introspection** — `metadata_generator.py` connects to PostgreSQL and extracts table names, column names, data types, primary keys, foreign keys, row counts, and sample values into `metadata.json`.

2. **Prompt assembly** — `prompt_builder.py` renders the schema and business context into a compact, token-efficient system prompt injected into every LLM call.

3. **Question routing** — `agent.py` classifies the question. Schema and metadata questions are answered directly from injected context. Data questions go to the LLM for SQL generation.

4. **SQL execution** — `query_runner.py` validates (read-only check), injects a row limit, executes against PostgreSQL, and returns structured results.

5. **Natural language answer** — a second LLM call takes the query results and returns a concise, business-friendly summary.

### Safety constraints in query_runner

- Blocks all write operations — `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`
- Only `SELECT` statements are permitted
- Enforces a 100-row result cap to prevent runaway queries on large tables

---

## Example Interactions

```
You: what tables are in the database?
Agent: The database contains 8 tables in the retail_staging schema:
       orders, lineitem, customer, supplier, part, partsupp, nation, region.

You: which customer segment generates the most revenue?
Agent: The BUILDING segment generates the most revenue with approximately
       $44.14 billion in net revenue, followed by AUTOMOBILE and MACHINERY.

You: how many orders are in each status?
Agent: There are 3 order statuses — Fulfilled (F) leads with ~730K orders,
       followed by Open (O) with ~726K, and In Progress (P) with a smaller share.
```

---

## Setup

### Prerequisites

- Python 3.10+
- PostgreSQL running locally
- Snowflake account with TPC-H sample data
- LLM API key

### Installation

```bash
git clone https://github.com/Vivek0306/DataAssist.git
cd data-assist
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

Create a `.env` file in the project root:

```
# PostgreSQL
PG_DBNAME=retail
PG_USER=postgres
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432

# LLM
LLM_API_KEY=your_api_key
LLM_BASE_URL=your_base_url
LLM_MODEL=your_model
```

### Generate schema metadata

```bash
python agent/metadata_generator.py
```

Introspects your PostgreSQL database and writes `agent/metadata.json`.

### Run the agent

```bash
python agent/agent.py
```

---

## Pipeline DAGs

Airflow DAGs in `dags/` orchestrate the full ELT flow — from Snowflake extraction through incremental PostgreSQL loading using a watermark pattern.

---

## Design Decisions

**No LangChain, no vector databases, no embeddings** — the agent is built entirely on raw API calls and plain Python. Schema context fits comfortably in a single prompt, so retrieval-augmented generation is unnecessary overhead for this use case.

**Two-stage LLM calls** — the first call generates SQL, the second converts query results into a natural language answer. Keeping these separate produces more reliable SQL and cleaner summaries than combining both into one prompt.

**Metadata-first routing** — schema and structural questions are answered directly from injected context without touching the database, making them instantaneous and reliable.

**Schema introspection via system tables** — `metadata_generator.py` uses PostgreSQL's `information_schema` and `referential_constraints` to extract the full FK graph including composite foreign keys, without relying on any ORM or schema inspection library.

**Read-only query execution** — the query runner enforces a keyword blocklist and only permits `SELECT` statements, ensuring the agent can never mutate the warehouse.

---

## Project Structure

```
data-assist/
├── agent/
│   ├── metadata_generator.py
│   ├── metadata.json       
│   ├── business_context.json
│   ├── llm_client.py
│   ├── prompt_builder.py
│   ├── query_runner.py
│   └── agent.py
├── dags/                       ← Airflow DAGs
├── pipelines/                  ← PySpark pipeline scripts
├── sql/                        ← SQL scripts
├── snowflake_connector.py
├── download_stage.py
├── requirements.txt
└── .env                       
```

---

## Author

Built as part of a data engineering portfolio — feedback and questions welcome via GitHub issues.