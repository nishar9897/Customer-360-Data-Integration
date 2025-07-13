# Retail Data Analytics Pipeline (DataProc + BigQuery + Looker Studio)

## Overview

This project demonstrates a cloud-native data analytics workflow for a retail business using Google Cloud Platform.

**Steps in this pipeline:**
1. **Data Cleaning:**  
   Used Google Cloud Dataproc to clean raw CSV data stored in Cloud Storage.
2. **Load to Silver Layer:**  
   Loaded cleaned data from Cloud Storage into BigQuery's `silverlayer` dataset using Bash shell scripts.
3. **Gold Layer Aggregation:**  
   Ran aggregation and summarization queries using Dataproc (Python with BigQuery client) to create business-ready tables in the `goldlayer` BigQuery dataset.
4. **Visualization:**  
   Connected the `goldlayer` tables directly to Looker Studio for real-time interactive dashboards and business insights.

## Components

- **Google Cloud Storage:** Raw and cleaned data storage.
- **Google Cloud Dataproc:** Data cleaning and transformation jobs.
- **BigQuery (Silver Layer):** Cleaned, structured data.
- **BigQuery (Gold Layer):** Aggregated analytics tables.
- **Bash Scripts:** For batch loading data into BigQuery.
- **Looker Studio:** Self-serve dashboarding directly on the Gold Layer.

## How to Run

1. Clean raw data with Dataproc.
2. Load to BigQuery silverlayer using provided bash scripts.
3. Run gold layer aggregation script via Dataproc.
4. Connect Looker Studio to `goldlayer` for reporting.

See individual script files and SQL for details.

---

