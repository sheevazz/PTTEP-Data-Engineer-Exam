# 📊 PTTEP Data Engineer Exam – Overview Architecture
**GCS → Cloud Run → BigQuery Ingestion Pipeline**

This repository implements an end-to-end ingestion pipeline that reads a CSV file from Google Cloud Storage (GCS), applies data validation and transformation rules in Python, and loads the curated result into Google BigQuery.

The solution follows enterprise-grade patterns with clear separation of duties between developers and ingestion runtimes.

---

## 🏗️ Architecture

```
Cloud Composer (Airflow)
        |
        | (BashOperator or CloudRunJobOperator)
        v
Cloud Run Job
        |
        v
Dockerized Python ETL
        |
        v
GCS  →  Transform  →  BigQuery
```

**Why Cloud Run Jobs?**
- Fully managed execution
- Native IAM integration
- Scales and retries automatically
- Clean separation between schema design and data movement

Cloud Run executes using a pipeline service account that has BigQuery Job User, Dataset Writer, and Service Usage permissions.  
Local development uses limited credentials.

---
