# 📊 PTTEP Data Engineer Exam – Task 2

## Overview
This project implements the ingestion and normalization pipeline for Task 2 of the PTTEP Data Engineer Exam. The objective is to transform a human-oriented Excel file containing daily operational values from oil & gas assets into a machine-friendly, analytics-ready BigQuery fact table.

## Architecture
Google Cloud Storage (Excel) -> Python ETL -> Normalized CSV -> BigQuery Fact Table

## Data Source
- GCS: gs://pttep-data-engineer-test/de-exam-task2/DE Exam raw data_20250101.xlsx
- BigQuery: gcp-test-data-engineer.exam_limpastan.task2_data_result

## Output Schema
| Column | Type | Description |
|-------|------|-------------|
| date | DATE | Calendar day of measurement |
| parameter | DATE | Batch date from filename |
| asset | STRING | Asset name |
| nomination | FLOAT | Planned value |
| load_ts | TIMESTAMP | Ingestion timestamp |

## Transformation Logic
1. Extract year from filename
2. Extract month from merged headers
3. Extract day from column A
4. Build real date = year + month + day
5. Unpivot asset columns into rows
6. Remove Reference columns and aggregates
7. Deduplicate by (date, asset)
8. Add load_ts

## Data Quality
- 120 days × 5 assets = 600 rows
- No nulls
- No duplicates
- Each date has 5 assets
- Numeric sanity checks

## How to Run
1. Run Python ETL: python3 task2_transform.py
2. Authenticate service account
3. Load CSV to BigQuery using bq load

## Verification
Run:
SELECT COUNT(*) FROM gcp-test-data-engineer.exam_limpastan.task2_data_result

Expected result: 600

## 🚀 Cloud Run Job

### Build image

```
gcloud builds submit --tag asia-southeast1-docker.pkg.dev/gcp-test-data-engineer/exam/task2-ingest
```

### Create job

```
gcloud run jobs create task2-ingest \
  --image asia-southeast1-docker.pkg.dev/gcp-test-data-engineer/exam/task2-ingest \
  --region asia-southeast1 \
  --service-account exam-pipeline-sa@gcp-test-data-engineer.iam.gserviceaccount.com
```

### Execute

```
gcloud run jobs execute task2-ingest --region asia-southeast1
