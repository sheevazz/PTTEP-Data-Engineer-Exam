import pandas as pd
import re
from datetime import datetime, UTC
import subprocess

RAW_FILE = "data/DE Exam raw data_20250101.xlsx"
EXPECTED_FILE = "data/DE Exam result.csv"
OUTPUT_FILE = "data/task2_result.csv"

# -------------------------------------------------
# 1) Extract parameter from filename
# -------------------------------------------------
parameter = pd.to_datetime(
    re.search(r"_(\d{8})", RAW_FILE).group(1),
    format="%Y%m%d"
).date()

year = parameter.year
print("Parameter:", parameter)

# -------------------------------------------------
# 2) Load Excel
# -------------------------------------------------
df = pd.read_excel(RAW_FILE, header=None)

MONTH_ROW = 11     # row with merged month headers
ASSET_ROW = 12     # row with asset names
DATA_START = 13    # first row of daily data

month_row = df.iloc[MONTH_ROW]
asset_row = df.iloc[ASSET_ROW]
data = df.iloc[DATA_START:].copy()

# First column contains day-of-month
data["day"] = pd.to_numeric(data.iloc[:, 0], errors="coerce")
data = data[data["day"].notna()]

# -------------------------------------------------
# 3) Build column → month mapping (handle merged cells)
# -------------------------------------------------
col_month = {}

for col in range(1, len(month_row)):
    cell = month_row[col]
    if pd.notna(cell):
        try:
            col_month[col] = pd.to_datetime(cell).month
        except:
            pass

# forward-fill merged months
last_month = None
for col in range(1, len(month_row)):
    if col in col_month:
        last_month = col_month[col]
    else:
        col_month[col] = last_month

# -------------------------------------------------
# 4) Unpivot Excel into normalized records
# -------------------------------------------------
records = []

for col in range(1, len(asset_row)):
    asset = str(asset_row[col]).strip()

    # Skip Reference and blank columns
    if asset.lower() == "reference" or asset == "" or asset == "nan":
        continue

    month = col_month.get(col)
    if month is None:
        continue

    for _, r in data.iterrows():
        day = int(r["day"])

        # Build real date
        try:
            date_val = datetime(year, month, day).date()
        except:
            continue

        try:
            val = float(r[col])
        except:
            continue

        records.append({
            "date": date_val,
            "parameter": parameter,
            "asset": asset,
            "nomination": val
        })

result = pd.DataFrame(records)

# -------------------------------------------------
# 5) Remove Excel duplicate blocks
# -------------------------------------------------
result = result.groupby(["date","parameter","asset"], as_index=False)["nomination"].first()

# -------------------------------------------------
# 6) Add load_ts (current UTC)
# -------------------------------------------------
result["load_ts"] = datetime.now(UTC)
result = result[["date","parameter","asset","nomination","load_ts"]]

# -------------------------------------------------
# 7) Write CSV
# -------------------------------------------------
result.to_csv(OUTPUT_FILE, index=False)
print("Generated rows:", len(result))

# -------------------------------------------------
# 8) Validate Data Quality
# -------------------------------------------------

print("\n=== DATA QUALITY REPORT ===")

# 1. Basic row count
total_rows = len(result)
print(f"Total rows: {total_rows}")

# 2. Date & asset coverage
dates = result["date"].nunique()
assets = result["asset"].nunique()

print(f"Unique dates: {dates}")
print(f"Unique assets: {assets}")

# Expectation: 120 days × 5 assets = 600 rows
expected_rows = dates * assets
print(f"Expected rows (dates × assets): {expected_rows}")

if total_rows == expected_rows:
    print("✅ Row count matches expected shape")
else:
    print("❌ Row count mismatch")

# 3. Assets per day (completeness)
asset_per_day = result.groupby("date")["asset"].nunique()

min_assets = asset_per_day.min()
max_assets = asset_per_day.max()

print(f"Assets per day: min={min_assets}, max={max_assets}")

if min_assets == max_assets == assets:
    print("✅ Every date has complete asset coverage")
else:
    print("❌ Some dates have missing or extra assets")
    print(asset_per_day[asset_per_day != assets])

# 4. Null checks
nulls = result.isnull().sum()
print("\nNull values per column:")
print(nulls)

if nulls.sum() == 0:
    print("✅ No null values found")
else:
    print("❌ Nulls detected")

# 5. Value sanity checks
print("\nNomination value ranges by asset:")
stats = result.groupby("asset")["nomination"].agg(["min","max","mean"])
print(stats)

# Example sanity rules (customize in real life)
if (stats["min"] < 0).any():
    print("❌ Negative values detected")
else:
    print("✅ No negative values")

# 6. Duplicate check
dup_count = result.duplicated(subset=["date","asset"]).sum()
print(f"\nDuplicate (date, asset) rows: {dup_count}")

if dup_count == 0:
    print("✅ No duplicate facts")
else:
    print("❌ Duplicate records found")

print("\n=== END DATA QUALITY REPORT ===\n")



# -------------------------------------------------
# 9) BQ Load
# -------------------------------------------------
BQ_TABLE = "gcp-test-data-engineer:exam_limpastan.task2_data_result"
OUTPUT_CSV = "data/task2_result.csv"

subprocess.run([
    "bq", "load",
    "--source_format=CSV",
    "--skip_leading_rows=1",
    "--replace",
    BQ_TABLE,
    OUTPUT_CSV,
    "date:DATE,parameter:DATE,asset:STRING,nomination:FLOAT,load_ts:TIMESTAMP"
], check=True)
