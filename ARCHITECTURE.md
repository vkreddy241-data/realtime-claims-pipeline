# Architecture — Real-Time Claims Pipeline

## Overview

A HIPAA-compliant streaming pipeline that ingests insurance claim events from Azure Event Hubs and transforms them through a Bronze → Silver → Gold Delta Lake medallion architecture, landing analytics-ready Gold tables for Synapse Analytics.

```
Azure Event Hubs (claim events)
        │
        ▼
Bronze Layer (ADLS Gen2 / Delta Lake)
  event_hubs_consumer.py
  - Parses CLAIM_EVENT_SCHEMA (claim_id, member_id, payer_id, provider_npi,
    claim_amount, service_date, diagnosis_codes, procedure_codes, status)
  - Good events  ──► bronze_path   (partitioned by claim_type, service_date)
  - Bad events   ──► bronze_dlq    (schema validation failures)
  - Trigger: every 60 seconds
        │
        ▼
Silver Layer (Delta Lake — SCD merge)
  bronze_to_silver.py (batch, Airflow-triggered)
  - Standardises claim_type (MEDICAL/PHARMACY/DENTAL/VISION/BEHAVIORAL/OTHER)
  - Parses pipe-delimited diagnosis_codes / procedure_codes → arrays
  - Deduplicates on claim_id (keep latest by received_at)
  - Validates required fields + claim_amount range
  - Valid    ──► MERGE INTO silver (upsert on claim_id)
  - Invalid  ──► quarantine table
  - Partitioned by claim_month, claim_type
        │
        ▼
Gold Layer (Delta Lake — analytics-ready)
  silver_to_gold.py
  ├── claims_daily_summary        (payer × claim_type × date: counts, totals, p95, denial rate)
  ├── member_monthly_utilization  (member × month: spend, service types, provider count)
  └── provider_scorecard          (NPI × month: volume, denial rate, adjustment rate)
        │
        ▼
Azure Synapse Analytics (consumption)
```

## Key Design Decisions

**Why Event Hubs over Kafka for ingestion?**  
The source system (Cigna-style payer platform) is Azure-native. Event Hubs provides a managed Kafka-compatible endpoint with built-in HIPAA BAA coverage, removing the need to operate a Kafka cluster in a regulated environment.

**Why a Dead-Letter Queue (DLQ) at the Bronze layer?**  
Claim events with missing claim_id or member_id cannot be recovered downstream — they need human review. Writing them to a separate DLQ at ingest time makes the failure visible without polluting the Silver layer and triggering false data quality alerts.

**Why MERGE (upsert) into Silver instead of append?**  
Claims are frequently amended (resubmission, status updates). An append-only Silver table would require downstream deduplication at every query. The SCD-style merge keeps Silver as a single source of truth with one row per claim_id.

**Why three separate Gold tables instead of one wide table?**  
Each Gold table serves a different consumer: claims_daily_summary for operational dashboards, member_monthly_utilization for actuarial models, provider_scorecard for network management. Separate tables avoid fan-out joins and allow independent refresh schedules.

**Why dynamic partition overwrite for Gold?**  
Gold tables are recomputed from Silver on a schedule. Dynamic partition overwrite replaces only the affected claim_month partitions, making reruns idempotent without dropping the entire table.

## Data Layers

| Layer | Format | Location | Partition | Latency |
|---|---|---|---|---|
| Bronze | Delta | ADLS Gen2 | claim_type, service_date | ~60 s |
| Bronze DLQ | Delta | ADLS Gen2 | — | ~60 s |
| Silver | Delta | ADLS Gen2 | claim_month, claim_type | ~15 min (Airflow) |
| Quarantine | Delta | ADLS Gen2 | — | with Silver run |
| Gold | Delta | ADLS Gen2 | claim_month, payer_id | ~30 min (Airflow) |

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Event Hubs + PySpark Structured Streaming |
| Storage | Delta Lake on Azure ADLS Gen2 |
| Processing | PySpark (batch jobs) |
| Orchestration | Apache Airflow |
| Consumption | Azure Synapse Analytics |
| Config | YAML (dev / prod profiles) |
| Infra | Docker Compose (local), Terraform (cloud) |

## HIPAA Considerations

- PII fields (member_id, provider_npi, diagnosis_codes) are never logged in plain text
- Bronze DLQ retains failed events for audit; access is role-gated
- All Delta tables are encrypted at rest via ADLS Gen2 storage account keys
- No PHI leaves the Azure tenant boundary
