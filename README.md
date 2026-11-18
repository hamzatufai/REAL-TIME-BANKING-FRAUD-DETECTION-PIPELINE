# Real-Time Banking Fraud Detection Pipeline

A learning and portfolio project. It streams **synthetic** banking transactions through Kafka, scores them with an explainable rule set, stores raw and curated data in AWS S3, loads the curated data into Snowflake, and builds tested analytics models with dbt Core.

> Synthetic data only. The risk score is a demonstration heuristic, not a production fraud model.

## Stack

Python · Apache Kafka (KRaft, single broker) · AWS S3 · Snowflake · dbt Core (dbt-snowflake) · Apache Superset (optional)

## Architecture

```
producer/            generate_transactions.py
   |
   v
Kafka topic: banking.transactions
   |
   v
consumer/            validate -> enrich -> score -> write batches
   |   \
   |    +--> Kafka topic: banking.invalid-transactions (bad events)
   v
S3 Bronze (raw JSONL)   and   S3 Silver (scored Parquet)
   |
   v
loader/              load_s3_to_snowflake.py  (MERGE, idempotent)
   |
   v
Snowflake  BANKING_FRAUD.RAW.TRANSACTIONS
   |
   v
dbt_fraud/           staging view -> mart_daily_fraud_summary table
   |
   v
Superset dashboard (optional)
```

## Folder layout

| Folder | Purpose | Run with |
|---|---|---|
| `config/` | Shared settings loaded from `.env` | (imported by all) |
| `producer/` | Synthetic transaction generator | `python -m producer.generate_transactions` |
| `consumer/` | Validation, enrichment, scoring, S3 writer | `python -m consumer.fraud_consumer` |
| `loader/` | S3 Silver to Snowflake loader | `python -m loader.load_s3_to_snowflake` |
| `snowflake/` | SQL to create warehouse, database, tables, roles | Snowsight or `snowsql` |
| `dbt_fraud/` | dbt project: staging and mart models with tests | `./scripts/run_dbt.sh build` |
| `scripts/` | Kafka topic setup, dbt wrapper, Snowflake check | see below |
| `tests/` | pytest unit tests for validation and scoring | `python -m pytest` |
| `docs/` | Screenshots and notes | — |

Each folder that runs code has its own `requirements.txt`. The root `requirements.txt` installs the Python parts into one virtual environment. dbt has its own requirements file.

## Quick start

Full instructions are in **[SETUP_GUIDE.md](SETUP_GUIDE.md)**. The short version:

```bash
cp .env.example .env            # then fill in your values
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# One-time Snowflake setup: run snowflake/01_create_objects.sql, then 02_security_roles.sql
python -m scripts.check_snowflake

# Terminal 1: Kafka broker (see SETUP_GUIDE.md step 7)
# Terminal 2: consumer
python -m consumer.fraud_consumer
# Terminal 3: producer
python -m producer.generate_transactions

# After some S3 files exist:
python -m loader.load_s3_to_snowflake

# dbt (separate venv, see SETUP_GUIDE.md step 9)
./scripts/run_dbt.sh debug
./scripts/run_dbt.sh build

# Tests
python -m pytest -q
```

## Risk rules

| Rule | Points | Reason code |
|---|---|---|
| Amount ≥ 5000 | 30 | `high_amount` |
| Country differs from home country | 30 | `country_mismatch` |
| Category is cash_advance or electronics | 20 | `sensitive_merchant_category` |
| Customer risk profile is high | 20 | `high_customer_risk_profile` |

Bands: LOW 0–29, MEDIUM 30–59, HIGH 60–100 (`is_fraud_flag` = HIGH).

## Design decisions

- **Snowflake instead of PostgreSQL.** Snowflake is the warehouse. The loader uses `write_pandas` plus `MERGE`, and dbt uses the `dbt-snowflake` adapter.
- **Idempotent loading.** `RAW.TRANSACTIONS` is keyed on `TRANSACTION_ID`, and `RAW.LOADED_FILES` records every S3 file loaded. Running the loader twice does not create duplicates.
- **Offsets commit after S3 writes.** The consumer commits Kafka offsets only after a batch reaches S3, so a crash replays events instead of losing them.
- **Batches, not single objects.** One S3 object per batch keeps request counts and costs low.
- **Least privilege.** A dedicated `FRAUD_ROLE` and `FRAUD_APP` user. No ACCOUNTADMIN in application configuration.
- **UTC everywhere.** Timestamps are stored as `TIMESTAMP_NTZ` in UTC.

## Known limitations

- Single Kafka broker and single partition. This is a development setup, not high availability.
- Customer profiles are a small in-code dictionary, not a real customer table.
- Rules are fixed. There is no velocity rule (for example, transactions per five minutes).
- The consumer does not handle non-JSON messages specially. A malformed payload stops the consumer. Add a parser guard before using this beyond learning.
- The loader lists all Silver files each run. It skips loaded files through `RAW.LOADED_FILES`, but listing still grows with the bucket.

## Future improvements

- Velocity rules with a Kafka Streams or Python sliding-window state.
- Orchestration with Airflow once the manual pipeline is stable.
- Snowpipe or an external stage on S3 instead of the Python loader.
- Customer profile table in Snowflake, joined in dbt.
- dbt snapshots and freshness tests on `RAW.TRANSACTIONS`.

## Security

- Use synthetic data only.
- Keep the S3 bucket private with Block Public Access enabled and SSE-S3 encryption.
- Never commit `.env`, AWS keys, or Snowflake passwords.
- Use the `FRAUD_ROLE` for the app. Use ACCOUNTADMIN only for the one-time setup in `snowflake/`.
