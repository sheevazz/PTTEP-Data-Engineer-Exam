import os
import subprocess
import pandas as pd
import re
from datetime import datetime
import pytz

# =====================================================
# Config
# =====================================================

BKK = pytz.timezone("Asia/Bangkok")

CSV_PATH = "/home/ap_limpastan/task1-data-pipeline/data/data_storytelling.csv"
OUTPUT_CSV = "/home/ap_limpastan/task1-data-pipeline/result/task1_out.csv"
BQ_TABLE = "gcp-test-data-engineer:exam_limpastan.task1_data_result"

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

# =====================================================
# Parsers
# =====================================================

from decimal import Decimal, InvalidOperation

BIGNUMERIC_MAX = Decimal("5.78960446186581E+38")
BIGNUMERIC_MIN = -BIGNUMERIC_MAX

def parse_decimal(v):
    if v is None or pd.isna(v):
        return None
    try:
        d = Decimal(str(v))
        if BIGNUMERIC_MIN <= d <= BIGNUMERIC_MAX:
            return str(d)
        return None
    except InvalidOperation:
        return None


def parse_timestamp(v):
    if pd.isna(v):
        return None
    v = str(v).strip()

    try:
        if re.match(r"\d{4}-\d{2}-\d{2}$", v):
            return v + " 00:00:00"

        if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v):
            return v

        if re.match(r"\d{14}$", v):
            return datetime.strptime(v, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")

        if re.match(r"\d{2}-[A-Za-z]{3}-\d{2}", v):
            day, mon, yr = v.split("-")
            if yr == "06":
                month = datetime.strptime(mon, "%b").month
                return f"2026-{month:02d}-{day} 00:00:00"
    except:
        return None

    return None

def parse_boolean(v):
    if pd.isna(v):
        return None
    v = str(v).strip().lower()
    if v in ["true", "yes", "ok", "1"]:
        return True
    if v == "-":
        return None
    return False

def extract_holiday(text):
    if not text or pd.isna(text):
        return None

    # normalize spaces
    text = re.sub(r"\s+", " ", text).strip()

    # Find something that ends with "Day"
    match = re.search(r"([A-Z][A-Za-z']*(?: [A-Z][A-Za-z']*)*) Day", text)
    if not match:
        return None

    holiday = match.group(0)  # e.g. "Buddhists Makha Bucha Day"

    # Remove known leading context words
    for prefix in [
        "On ", "on ", "During ", "during ",
        "Buddhists ", "Buddhist ",
        "National ", "Royal "
    ]:
        if holiday.startswith(prefix):
            holiday = holiday[len(prefix):]

    return holiday.strip()



# =====================================================
# Bullet-proof integer_col normalization
# =====================================================

INT64_MIN = -9223372036854775808
INT64_MAX =  9223372036854775807

def normalize_integer_col(series):
    cleaned = []

    for v in series:
        if v is None or pd.isna(v):
            cleaned.append(pd.NA)
            continue

        s = str(v).replace(",", "").strip()

        # pure integer
        if re.fullmatch(r"-?\d+", s):
            try:
                i = int(s)
                if INT64_MIN <= i <= INT64_MAX:
                    cleaned.append(i)
                else:
                    cleaned.append(pd.NA)
            except:
                cleaned.append(pd.NA)
            continue

        # float that is actually integer (180.0)
        try:
            f = float(s)
            if f.is_integer():
                i = int(f)
                if INT64_MIN <= i <= INT64_MAX:
                    cleaned.append(i)
                else:
                    cleaned.append(pd.NA)
            else:
                cleaned.append(pd.NA)
        except:
            cleaned.append(pd.NA)

    return pd.Series(cleaned, dtype="Int64")


# =====================================================
# Main
# =====================================================

def main():
    print("Reading CSV from /home/ap_limpastan/task1-data-pipeline/data/data_storytelling.csv ...")

    raw = pd.read_csv(
        CSV_PATH,
        engine="python",
        on_bad_lines="skip",
        dtype=str
    )

    print(f"Read {len(raw)} valid rows")

    results = []

    for idx, row in enumerate(raw.itertuples(index=False), start=1):
        full_text = f"{getattr(row,'boolean_col','')} {getattr(row,'holiday_name','')}"

        results.append({
            "row_id": idx,
            "integer_col": getattr(row, "integer_col", None),
            "decimal_col": parse_decimal(getattr(row, "decimal_col", None)),
            "timestamp_col": parse_timestamp(getattr(row, "timestamp_col", None)),
            "boolean_col": parse_boolean(getattr(row, "boolean_col", None)),
            "holiday_name": extract_holiday(full_text),
            "business_datetime": datetime.now(BKK),
            "created_datetime": datetime.utcnow()
        })

    df_out = pd.DataFrame(results)

    # Fix integer_col
    df_out["integer_col"] = normalize_integer_col(df_out["integer_col"])

    print("Schema:")
    print(df_out.dtypes)

    print("Writing CSV...")
    df_out.to_csv(OUTPUT_CSV, index=False)

    print("Loading into BigQuery via bq CLI...")

    subprocess.run([
        "bq", "load",
        "--source_format=CSV",
        "--skip_leading_rows=1",
        "--replace",
        BQ_TABLE,
        OUTPUT_CSV,
        "row_id:INTEGER,integer_col:INTEGER,decimal_col:BIGNUMERIC,timestamp_col:TIMESTAMP,boolean_col:BOOL,holiday_name:STRING,business_datetime:TIMESTAMP,created_datetime:TIMESTAMP"
    ], check=True)

    print("BigQuery load completed successfully")

if __name__ == "__main__":
    main()
