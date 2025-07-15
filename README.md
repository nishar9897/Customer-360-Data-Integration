# Customer Data Integration with Cloud Composer (Airflow)

## Overview

This project showcases a **fully automated, end-to-end customer analytics pipeline** on Google Cloud Platform, orchestrated using **Cloud Composer** (Google Cloud’s managed Apache Airflow service).

The pipeline ingests raw customer transaction data, cleans and transforms it, loads it into BigQuery’s Silver Layer, executes advanced aggregations for reporting (Gold Layer), and can optionally refresh dashboards in Looker Studio. All of this is automated, monitored, and repeatable with zero manual intervention.

---

## What I Did

### 1. Designed the Cloud Composer DAG

- Created an Airflow DAG that defines the **entire workflow** as modular, independent tasks.
- DAG was scheduled to run **daily** (can also be triggered by file arrival in GCS).

### 2. Data Cleaning with Dataproc

- Used **Dataproc** (managed Spark/Hadoop) to run a **PySpark job** for data cleaning and preprocessing.
- The job is triggered via Airflow’s `DataprocSubmitJobOperator` whenever new data is detected.
- Ensures incoming data is validated, cleaned, and formatted consistently.

### 3. Load Cleaned Data to BigQuery (Silver Layer)

- The cleaned output is loaded from Dataproc (or GCS) into the **BigQuery Silver Layer**.
- Utilized Airflow’s `BigQueryInsertJobOperator` or `BashOperator` for flexible data ingestion.

### 4. Aggregation & Analytics in BigQuery (Gold Layer)

- Automated execution of **Gold Layer** aggregation queries using the `BigQueryOperator`.
- Created summary tables (AOV, segmentation, peak hour, agent performance) in the `goldlayer` dataset.

### 5. Dashboard Refresh / Notification

- (Optional) Triggered a Looker Studio data source refresh or sent notification emails using `HttpOperator` or `EmailOperator`.
- Ensures analytics dashboards always show up-to-date data.

### 6. Logging & Monitoring

- All tasks are logged for auditing and troubleshooting.
- Failures trigger retries and can send alerts via email/Slack.

---

## Challenges Faced

### 1. Dependency Management & Environment Issues

- Ensuring all Python and PySpark dependencies are available in Cloud Composer (solved via custom PyPI packages and environment variables).
- Handling different Python/Spark versions between local development, Dataproc, and Composer.

### 2. Data Quality & Schema Drift

- Incoming raw data formats can change, causing schema mismatches or ingestion failures.
- Added data validation and schema inference steps to make ingestion robust.

### 3. Task Failures & Retries

- Dataproc jobs can fail due to resource limits or bad input data.
- Used Airflow’s retry and alerting mechanisms to reduce downtime.

### 4. BigQuery Table Locking & Quotas

- Running concurrent jobs on large datasets may hit BigQuery quotas or cause table locks.
- Scheduled intensive aggregations during off-peak hours and used partitioned tables.

### 5. Orchestrator Complexity

- Setting up Cloud Composer’s permissions, networking, and service account roles can be complex for first-time users.
- Documented IAM roles and network configs for future scaling.

### 6. Monitoring & Debugging

- Airflow logs can be verbose and hard to parse when multiple DAGs run in parallel.
- Used Airflow UI, task-specific logs, and custom alerting for visibility.

---

## How It Works

1. **Trigger:**  
   The Cloud Composer DAG is triggered on a schedule (e.g., daily at midnight) or by a file upload to a GCS bucket.

2. **Data Cleaning (Dataproc):**  
   - Raw customer data is processed by a PySpark job on Dataproc.
   - Data is cleaned and validated, with logs written to GCS for auditing.

3. **Load to Silver Layer (BigQuery):**  
   - Cleaned data is ingested into BigQuery using the `BigQueryOperator`.
   - Schema is auto-detected or explicitly set for consistency.

4. **Gold Layer Aggregation:**  
   - BigQuery aggregation queries run to produce analytics tables for reporting.
   - Tables are created or replaced in the `goldlayer` dataset.

5. **Dashboard Refresh / Notification:**  
   - Optionally, a webhook or email notifies stakeholders or triggers a Looker Studio dashboard refresh.

6. **Monitoring:**  
   - Airflow logs every step, retries on failure, and sends error notifications if tasks fail repeatedly.

---

## Benefits

- **Full Automation:** No manual intervention needed after setup—everything from data arrival to analytics is automatic.
- **Scalability:** Easily adapts to growing data and new analytics requirements.
- **Observability:** Transparent task logs and monitoring ensure problems are caught early.
- **Best Practice:** Follows GCP and data engineering orchestration standards.

---

## Summary

This project demonstrates a **production-grade, automated customer data analytics pipeline** on GCP, orchestrated using Cloud Composer. All data transformations and analytics are reproducible, logged, and scalable, enabling rapid, reliable business insights with minimal human effort.

---
