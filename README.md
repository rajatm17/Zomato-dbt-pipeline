# Zomato Food Delivery Data Pipeline — Architecture Overview

## Pipeline Flow

### Zomato Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI Layer (Gemini)

## How It Works

Raw food delivery data is first deposited into an Amazon S3 bucket, which acts as the project's data lake. From there, a Snowflake storage integration establishes a secure connection to S3, allowing the data to be ingested directly into the warehouse without manual file handling.

Once inside Snowflake, dbt takes over the transformation logic and structures the data using a **medallion architecture** made up of three progressive layers:

- **RAW (Bronze) layer** — Source data is ingested as-is into raw tables using Snowflake's `COPY INTO` command, preserving the original structure with minimal alteration.
- **STAGING (Silver) layer** — Lightweight views clean, standardize, and prepare the raw data, correcting types, naming conventions, and inconsistencies before it's used downstream.
- **MARTS (Gold) layer** — Business-ready models are built here, including dimension tables, incremental fact tables (which only process new or changed records rather than reprocessing everything), and pre-aggregated summary tables for reporting.

**Apache Airflow** sits above the transformation layer and orchestrates the entire workflow as a single daily DAG (Directed Acyclic Graph), automating the sequence and timing of each step from ingestion through transformation.

## AI Layer

Layered on top of the warehouse is an AI-powered extension built with **OpenAI**, adding three capabilities:

1. **LLM Enrichment** — Unstructured, free-text customer reviews are parsed and converted into structured, queryable fields.
2. **RAG (Retrieval-Augmented Generation)** — Enables natural-language conversations with the review data, letting users ask questions and get contextual answers.
3. **Text-to-SQL** — Translates plain-English questions into SQL queries, allowing anyone to query the warehouse without writing SQL directly.

## Presentation Layer

**Streamlit** powers the front end, hosting both the analytics dashboards and the AI-driven applications (chat and text-to-SQL interfaces) in one accessible place.

# Zomato Data Pipeline — Full Project Breakdown

## What Gets Built

| Layer | Where | What |
|---|---|---|
| Source | `data/` (local) | 4 real dimension CSVs (restaurants, users, food, menu) plus 3 generated fact files: 10M orders, ~23M order items, 300K free-text reviews |
| Lake | Amazon S3 | A single bucket with one `raw/<table>/` folder per CSV |
| Bronze | Snowflake `ZOMATO.RAW` | Loaded via `COPY INTO` from S3 using a keyless storage integration |
| Silver | Snowflake `ZOMATO.STAGING` | dbt staging views that clean, type-cast, and rename each source |
| Gold | Snowflake `ZOMATO.MARTS` | Dimension tables, incremental fact tables (via `MERGE`), business marts, and an SCD2 snapshot |
| AI | Snowflake `ZOMATO.AI` | LLM-enriched reviews (sentiment/topic), RAG-based chat, and text-to-SQL |
| Orchestration | Airflow (Docker) | A single daily DAG covering load → transform → enrich → AI mart |

## Tech Stack

Python · Pandas · Amazon S3 · Snowflake · dbt (`dbt-snowflake`) · Apache Airflow 3 (Docker) · OpenAI (`gemini-3.1-flash-lite`, `gemini-embedding-001`) · Streamlit

## Repository Structure

    ├── airflow/                  # Airflow 3 running on Docker
    │   ├── Dockerfile            #   Snowflake + OpenAI providers, dbt in its own venv
    │   ├── docker-compose.yaml   #   postgres + api-server + scheduler; credentials via env vars
    │   ├── example.env           #   template for SNOWFLAKE_* / GEMINI_API_KEY
    │   └── dags/zomato_batch.py  #   the pipeline DAG (4 tasks)
    ├── zomato/                   # dbt project
    │   ├── models/staging/       #   7 staging views (Silver) + sources + tests
    │   ├── models/marts/         #   dimensions, incremental facts, business marts (Gold)
    │   └── macros/               #   custom schema-name macro
    ├── ai/                       # AI layer
    │   ├── enrich_reviews.py     #   LLM enrichment → ZOMATO.AI.ENRICHED_REVIEWS
    │   ├── rag_chat.py           #   RAG — "chat with your reviews" (Streamlit)
    │   ├── text_to_sql.py        #   text-to-SQL — "chat with your warehouse" (Streamlit)
    │   └── example.env           #   template for AI credentials
    ├── snowflake/                # Snowflake setup SQL (run in Snowsight, in order)
    │   ├── 01_setup.sql          #   warehouse ZOMATO_WH, database ZOMATO, schemas, role
    │   ├── 02_storage_integration.sql  # keyless S3 link (pairs with aws/iam/)
    │   ├── 03_stage_and_formats.sql    # external stage + CSV file format
    │   ├── 04_raw_tables.sql     #   RAW (Bronze) table DDL, matching CSV column order
    │   └── 05_copy_into.sql      #   COPY INTO RAW from the stage
    ├── aws/iam/                  # IAM policy + role trust policies for the S3 ↔ Snowflake handshake
    └── docs/architecture.png     # architecture diagram

> `data/` (roughly 2.3 GB of CSVs), logs, and dbt's `target/` artifacts are intentionally excluded from version control — the dataset and slides are available separately via Google Drive.

## How the Pipeline Works

### 1 · Data Lands in S3

The seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/`, with one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2 · S3 → Snowflake: A Keyless Handshake

Snowflake reads from the bucket without storing any access keys, relying instead on a storage integration paired with an IAM role. The Snowflake-side setup lives in `[snowflake/02_storage_integration.sql]`, while the AWS-side JSON documents are under `aws/iam/`:

| File | Purpose |
|---|---|
| `s3-read-policy.json` | IAM policy `zomato-s3-read` — grants read-only access to the bucket |
| `snowflake-role-trust-policy-initial.json` | IAM role `snowflake-s3-role` — a placeholder trust policy used at creation |
| `snowflake-role-trust-policy-final.json` | The finalized trust policy, using Snowflake's IAM user ARN and external ID from `DESC INTEGRATION` |

The setup order matters: first create the AWS policy and role, then create the Snowflake `STORAGE INTEGRATION` pointing at that role's ARN, then run `DESC INTEGRATION` to retrieve `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`, and finally paste both values into the role's trust policy.

Two things learned the hard way: the trust policy's Principal must reference Snowflake's specific IAM user ARN rather than `:root`, and re-running `CREATE OR REPLACE` on the integration afterward regenerates the external ID and breaks the existing trust relationship.

### 3 · Load — `COPY INTO`

Table definitions in `snowflake/04_raw_tables.sql` mirror each CSV's column ordering, and `snowflake/05_copy_into.sql` then pulls each file from the stage into `ZOMATO.RAW` tables — covering 10M orders, ~23M order items, and 300K reviews.

### 4 · Transform — dbt (Medallion Architecture)

- **Staging (Silver)** — One view per source table: parsing messy restaurant fields (turning `--` into null, stripping `₹` from cost values like `₹ 200` → `200`), lowercasing emails, deriving fields like `is_delivered`, and similar cleanup.
- **Dimensions (Gold)** — `dim_restaurants`, `dim_customer` (including age segmentation), `dim_food`, and a generated `dim_date` calendar table.
- **Facts (Gold, incremental)** — `fact_orders` and `fact_order_items` use `materialized='incremental'` with a `MERGE` strategy, so subsequent runs only process new rows instead of rebuilding all 10M+ records each time.
- **Marts (Gold)** — One table per business question: daily city-level revenue (GMV, AOV, cancellation rate), restaurant performance, delivery SLA metrics (p50/p90 by city and hour), and review insights.
- **Tests** — `unique`, `not_null`, `relationships`, and `accepted_values` tests, plus a custom singular reconciliation test. `dbt build` runs all models and tests in dependency order.

### 5 · Orchestrate — Airflow

A single daily DAG, `zomato_batch`, runs the entire pipeline as one connected graph:
Credentials are never hardcoded into the code — `docker-compose` injects `SNOWFLAKE_*` environment variables (read by dbt's `profiles.yml` through `env_var()`) along with an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection used by the COPY task.

### 6 · AI Layer — Three Capabilities

- **LLM enrichment** (`ai/enriched_reviews.py`) — Uses an LLM as a transformation step: reads review text, prompts `gemini-3.1-flash-lite` for structured JSON output (sentiment and topic), and writes the result to `ZOMATO.AI.REVIEW_ENRICHED`, which dbt then models into `mart_review_insights` just like any other table. It's idempotent and capped by a sample size (`SAMPLE_N`) so the same review is never paid for twice.
- **RAG** (`ai/rag_chat.py`) — Powers a "chat with your reviews" experience: reviews are embedded, the most relevant ones are retrieved for a given question, and an answer is generated that's grounded in actual reviews, with sources cited.
- **Text-to-SQL** (`ai/text_to_sql.py`) — Powers a "chat with your warehouse" experience: the LLM is given the marts' schema, generates Snowflake SQL for an English-language question, and a SELECT-only guard validates the query before it runs under `DBT_ROLE`.

## Running It

```bash
# Snowflake objects (warehouse ZOMATO_WH, database ZOMATO, schemas RAW/STAGING/MARTS/SNAPSHOTS/AI, role DBT_ROLE)
# + the S3 storage integration: run snowflake/01→05 in Snowsight — see aws/iam/ for the AWS side.

# dbt
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# Airflow
cd airflow
cp example.env .env          # fill SNOWFLAKE_* , OPENAI_API_KEY, SAMPLE_N
docker compose build && docker compose up -d
# http://localhost:8080 → un-pause zomato_batch → Trigger

# AI apps
export GEMINI_API_KEY=sk-...
python ai/enriched_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```
