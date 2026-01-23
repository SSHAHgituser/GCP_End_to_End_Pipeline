For detailed explanation, please refer the medium article: [https://medium.com/@ganeshnasrikrishna/building-a-real-end-to-end-data-pipeline-on-gcp-yes-from-scratch-04b5e78ecbf6](https://medium.com/@ganeshnasrikrishna/building-a-real-end-to-end-data-pipeline-on-gcp-yes-from-scratch-04b5e78ecbf6)

## 🌍 End-to-End Streaming Data Pipeline on GCP

This project demonstrates a real-world, production-style streaming data pipeline built from scratch on Google Cloud Platform.

The goal is simple:
ingest real-time air quality data → process it reliably → model it for analytics.

No CSVs. No fake generators. Real APIs. Real problems.

## 🏗️ High-Level Architecture

WAQI API -> Cloud Function (Gen2) -> Pub/Sub -> Dataflow (Apache Beam – Streaming, Flex Template) -> BigQuery (Bronze) -> dbt (Silver & Gold models)


Cloud Scheduler is used to poll the API hourly (simulating streaming).

Each layer is decoupled, scalable, and failure-isolated.

## 🔄 Process Overview (Tool-Agnostic)

Ingestion

Fetch air-quality data from a public API

Push raw events into a message queue

Buffering & Decoupling

Pub/Sub absorbs spikes and isolates failures

Producers never wait for consumers

Streaming Processing

Events are parsed, validated, and structured

Late data and retries are handled correctly

Storage

Clean, flat records are stored in BigQuery (Bronze layer)

Transformation

dbt applies business logic:

Deduplication

SCD Type 2 dimensions

Analytics-ready aggregates

## 📦 Tech Stack

Cloud Functions (Gen2) – API ingestion

Cloud Scheduler – Hourly triggers

Pub/Sub – Messaging backbone

Apache Beam – Stream processing logic

Cloud Dataflow – Managed Beam execution

BigQuery – Data warehouse

dbt (BigQuery adapter) – Transformations (Bronze → Silver → Gold)

GCS & Artifact Registry – Templates & images

## 🧠 Key Design Decisions

Schema-first ingestion
Raw payloads are not dumped blindly.
Fields are explicitly extracted and typed in Beam.

Streaming-first mindset
Event time is preserved. Pipelines resume after failure.

Flex Templates for Dataflow
Pipelines are deployed as reusable artifacts — not laptop scripts.

Medallion Architecture

Bronze: structured raw events

Silver: deduplicated, cleaned data

Gold: business-ready metrics

🗂️ BigQuery Models
Bronze

One row per event

No deduplication

Minimal logic

Silver

Deduplicated using business keys

Incremental models

Stable hashes

Gold

Daily aggregates

Dimension joins

Analytics-friendly tables

## 🧩 dbt Highlights

Incremental models

SCD Type 2 dimension for stations

Data quality tests (not_null, unique)

dbt_utils for surrogate keys

## 🚀 Deployment Notes

Dataflow pipelines are deployed using Flex Templates

dbt orchestration is intentionally left open-ended

Cloud Composer + service accounts are the intended next step

(Left as an exercise… and a lesson in IAM patience 😅)
