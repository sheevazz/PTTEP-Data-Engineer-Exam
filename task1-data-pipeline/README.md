# 📊 PTTEP Data Engineer Exam – Task 1  
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

## 📥 Data Source

| Item | Value |
|------|------|
| GCS Path | `gs://pttep-data-engineer-test/de-exam-task1/data_storytelling.csv` |
| BigQuery Project | `gcp-test-data-engineer` |
| Dataset | `exam_<last_name>` |
| Table | `task1_data_result` |

---

## 🧪 Data Processing Rules

Each row is validated independently.  
If a value does not match the required data type or range, it is stored as **NULL**.

### 1. `row_id`
Generated as a running number starting from 1.

---

### 2. `integer_col` → INT64

Rules:
- Commas allowed ("1,234" → 1234)
- Whole numbers allowed (180, 180.0)
- Decimals not allowed (1.23 → NULL)
- Scientific notation not allowed
- Must fit BigQuery INT64 range

| Input | Output |
|------|--------|
| "1,234" | 1234 |
| "180.0" | 180 |
| "1.23" | NULL |
| "1e10" | NULL |

---

### 3. `decimal_col` → BIGNUMERIC

NUMERIC is not sufficient due to extremely large values in the data.

Rules:
- Parsed with Python Decimal
- Must fit BigQuery BIGNUMERIC range (±5.78960446186581 × 10³⁸)
- Out-of-range values → NULL

---

### 4. `timestamp_col` → TIMESTAMP

Accepted formats:
- YYYY-MM-DD
- YYYY-MM-DD HH:MM:SS
- YYYYMMDDHHMMSS
- DD-Mon-YY (assumed 2026)

All invalid formats → NULL.

---

### 5. `boolean_col` → BOOL

| Input | Output |
|------|--------|
| true, yes, ok, 1 | TRUE |
| '-' | NULL |
| Others | FALSE |

---

### 6. `holiday_name`

Extracted from free text ending with "Day".  
Leading context words are removed.

| Input | Output |
|------|--------|
| On Constitution Day | Constitution Day |
| During Labour Day | Labour Day |
| Buddhists  Makha Bucha Day | Makha Bucha Day |

---

### 7. `business_datetime`
Set to current time in **Asia/Bangkok** timezone.

### 8. `created_datetime`
Set to current time in **UTC**.

BigQuery stores TIMESTAMP in UTC; timezone is applied at query time.

---

## ⏱️ Timezone Validation

```sql
SELECT
  DATETIME(created_datetime, "UTC") AS created_utc,
  DATETIME(business_datetime, "Asia/Bangkok") AS business_bkk
FROM `gcp-test-data-engineer.exam_<last_name>.task1_data_result`
LIMIT 5;
```

---

## 🚀 Cloud Run Job

### Build image

```
gcloud builds submit --tag asia-southeast1-docker.pkg.dev/gcp-test-data-engineer/exam/task1-ingest
```

### Create job

```
gcloud run jobs create task1-ingest \
  --image asia-southeast1-docker.pkg.dev/gcp-test-data-engineer/exam/task1-ingest \
  --region asia-southeast1 \
  --service-account exam-pipeline-sa@gcp-test-data-engineer.iam.gserviceaccount.com
```

### Execute

```
gcloud run jobs execute task1-ingest --region asia-southeast1
```

---

## 🧠 Enterprise Design

| Area | Design |
|------|-------|
| IAM | Developers define schemas; Cloud Run executes data jobs |
| Security | No credentials stored in code |
| Data Quality | Type, range, and format enforcement |
| Observability | Logs available in Cloud Run & BigQuery |

---

## ✅ Output

All valid records are loaded into:

```
gcp-test-data-engineer.exam_<last_name>.task1_data_result
```

Invalid or out-of-range values are stored as NULL, ensuring reliable analytics-ready data.
