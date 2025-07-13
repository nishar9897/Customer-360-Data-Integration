# Customer data integration (Cloud Composer / Airflow)

## Overview

This version of the project fully automates the end-to-end data pipeline using **Cloud Composer** (Google Cloud's managed Apache Airflow).

**What I did:**
- Designed and implemented a DAG (Directed Acyclic Graph) in Cloud Composer to automate each step of the analytics pipeline.
- The pipeline triggers:
  1. **Data cleaning on Dataproc** (runs PySpark/cleaning job on new data arrival).
  2. **Loads cleaned data to BigQuery Silver Layer** (uses BashOperator or BigQueryOperator).
  3. **Runs gold layer aggregation queries** (BigQueryOperator).
  4. **Notifies or triggers Looker Studio refresh** (optional, e.g., via webhook or scheduled refresh).

## Benefits

- **Full automation:** No manual steps. Orchestrates all compute and data transfer jobs on schedule or data arrival.
- **Robustness:** Airflow retry logic, error notification, and clear logs for troubleshooting.
- **Extensible:** Easy to add more ETL steps, new tables, or notification logic.
- **Scalable:** Serverless execution on Google Cloud infrastructure.

## How It Works

- Cloud Composer DAG monitors GCS bucket or runs on schedule.
- Each DAG task is a managed, reproducible cloud operation:
    - Dataproc job → Bash/BigQuery load → Aggregation queries → Notification.
- All data transformations and loads are logged and can be re-run on demand.

## Summary

This approach demonstrates best-practice, production-grade orchestration for data analytics pipelines on GCP, reducing manual work and enabling seamless, repeatable insights.

---

