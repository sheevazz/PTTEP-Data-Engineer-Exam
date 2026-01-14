# 📊 PTTEP Data Engineer Exam – Task 1  


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

For most fields, if a value does not match the required data type or allowed range, it is stored as **NULL**.

For `decimal_col`, values that exceed BigQuery’s **allowed scale (more than 9 decimal places)** are **rounded** to 9 decimal places.  
Values that exceed BigQuery’s **allowed precision or numeric range** are stored as **NULL**.

All transformations are deterministic and do not affect other columns in the same row.


### 1. `row_id`
Generated as a running number starting from 1.

---

### 2. `integer_col` → INT64

Rules:
- Parsed as string first (no float inference)
- Commas are allowed (`"1,234"` → `1234`)
- Whole numbers are allowed (`"180"`, `"180.0"` → `180`)
- Any value with a fractional component → `NULL` (`"1.23"` → `NULL`)
- Scientific notation is not allowed (`"1e10"` → `NULL`)
- Must fit within BigQuery INT64 range (−9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
- Output is always an integer string (no `.0`) for BigQuery ingestion

| Input | Output |
|--------|--------|
| `"1,234"` | `1234` |
| `"180"` | `180` |
| `"180.0"` | `180` |
| `"1.23"` | `NULL` |
| `"1e10"` | `NULL` |
| `"9223372036854775808"` | `NULL` |


---

### 3. `decimal_col` → BIGNUMERIC

The source data contains extremely large and high-precision decimal values that cannot be loaded directly into BigQuery without normalization.

BigQuery BIGNUMERIC constraints:
- Maximum **38 digits before** the decimal point
- Maximum **9 digits after** the decimal point
- Absolute value ≤ 5.78960446186581 × 10³⁸

Rules:
- Parsed using Python `Decimal` (no floating-point loss)
- Values with more than **38 integer digits** → `NULL`
- Values with more than **9 fractional digits** → rounded to 9 decimal places
- Values exceeding the **BIGNUMERIC numeric range** → `NULL`
- All valid values are emitted in fixed-point string form for BigQuery ingestion


---

### 4. `timestamp_col` → TIMESTAMP

Accepted formats:
- YYYY-MM-DD
- YYYY-MM-DD HH:MM:SS
- YYYYMMDDHHMMSS
- DD-Mon-YY (assumed 2026)
- DD/MM/YYYY

All invalid formats → NULL.

---

### 5. `boolean_col` → BOOL

The column may contain arbitrary values.

Rules:
- **True** if value ∈ (`true`, `yes`, `ok`, `1`) (case-insensitive)
- **False** for all other values
- `"-"` is treated as an **invalid placeholder** → stored as `NULL`

Examples:

| Input | Output |
|------|--------|
| `true` | TRUE |
| `yes` | TRUE |
| `ok` | TRUE |
| `1` | TRUE |
| `no` | FALSE |
| `0` | FALSE |
| `2026-01-06 07:48:25` | FALSE |
| `random` | FALSE |
| `-` | NULL |

---

### 6. `holiday_name` → STRING

The source column contains **unstructured narrative text**, not just holiday names.

Examples:
- "On Constitution Day, citizens engage in activities..."
- "Every year, during Makha Bucha Day, a peaceful atmosphere fills the air..."
- "The King's Birthday is celebrated with grand parades..."

The pipeline extracts a normalized holiday name using pattern recognition.

Supported patterns:
- `<Holiday> Day` → e.g. "Makha Bucha Day", "Constitution Day"
- `<Holiday> Festival` → e.g. "Songkran Festival"
- `<Holiday> Ceremony` → e.g. "Royal Ploughing Ceremony"
- `<Person>'s Birthday` → e.g. "King's Birthday", "Queen's Birthday"
- Standalone Thai holidays → "Makha Bucha", "Visakha Bucha", "Asalha Bucha"

Leading narrative phrases are removed:
- "On ..."
- "After ..."
- "During ..."
- "The day of ..."

If no recognizable holiday is found → **NULL**


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

## ✅ Output

All valid records are loaded into:

```
gcp-test-data-engineer.exam_<last_name>.task1_data_result
```

Invalid or out-of-range values are stored as NULL, ensuring reliable analytics-ready data.

## 🔍 Data Quality & Auditing

The pipeline does not drop rows when values are invalid or exceed BigQuery constraints.

Instead, invalid values are:
- Set to `NULL`
- Counted per rule (e.g., `decimal.out_of_range`, `integer.invalid`, `timestamp.invalid`)
- Logged with up to 5 example values per category for forensic auditing

This allows:
- Full row preservation
- Transparent justification for every NULL
- Reproducible data quality metrics
