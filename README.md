# Real-Time Healthcare Claims Pipeline

A production-grade streaming data platform built on Azure, migrating legacy nightly batch jobs to near real-time ingestion for healthcare claims and eligibility analytics.

Built to solve a 4-hour data latency problem in claims adjudication. The pipeline now delivers data in under 30 minutes, enabling same-day reporting across 50+ downstream consumers.

## Architecture

Source Systems → Azure Event Hubs → Dead Letter Queue
                        │
                        ▼
        Databricks Spark Streaming (PySpark)
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
    Bronze Layer    Silver Layer    Gold Layer
    (raw/ADLS)    (cleansed/dedup) (aggregated)
                                        │
                                        ▼
                            Azure Synapse Analytics
                                        │
                                        ▼
                        BI | Compliance | Adjudication

## Stack

| Layer | Technology |
|---|---|
| Streaming Ingest | Azure Event Hubs |
| Processing | Databricks, PySpark, Spark Streaming |
| Storage | ADLS Gen2, Delta Lake |
| Warehouse | Azure Synapse Analytics |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| IaC | Terraform |
| CI/CD | GitHub Actions |
| Monitoring | Azure Monitor, Log Analytics |

## Performance Results

| Metric | Before | After |
|---|---|---|
| Data latency | 4 hours | under 30 minutes |
| Synapse compute cost | baseline | minus 20 percent |
| Avg query execution time | baseline | minus 40 percent |
| Pipeline uptime SLA | 96 percent | 99.5 percent |
| Incident resolution time | 2 plus hours | under 45 minutes |

## Project Structure

- src/ingestion — Event Hubs consumer, Airflow DAG, local producer
- src/processing — Bronze to Silver to Gold transformations
- src/quality — Great Expectations DQ checks
- src/utils — Config, logging, retry helpers
- configs — Per-environment YAML configs (dev, prod)
- infra — Terraform for all Azure resources
- tests — PySpark unit tests
- docs — Ops runbook
- .github/workflows — CI/CD pipeline

## Running Locally

Clone the repo, create a virtual environment, install requirements, start Docker Compose for local Kafka and Azurite, generate synthetic claims with local_producer.py, and run pytest for unit tests.

## Key Design Decisions

- Delta Lake throughout — ACID guarantees and time-travel for debugging
- Schema enforcement at ingest — bad records routed to DLQ, never silently dropped
- Idempotent Silver writes — MERGE INTO on claim_id, safe to re-run
- Partition-safe Gold writes — REPLACE WHERE avoids full table overwrite
- DQ quarantine not pipeline halt — isolates bad records without stopping the pipeline

## Lessons Learned

1. Event Hubs partition count is immutable — size for peak throughput from day one
2. Delta OPTIMIZE timing matters — running during peak ingestion causes cluster contention
3. Checkpoint corruption is real — added validation step on every job startup
4. Great Expectations with nested JSON — flatten to structs before validation

## Author

Vikas Reddy Amaravathi — Data Engineer | AWS, Azure, GCP

LinkedIn: https://linkedin.com/in/vikas-reddy-a-avr03
GitHub: https://github.com/vkreddy241-data
