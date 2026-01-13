import os
import subprocess
import pandas as pd
import re
from datetime import datetime
import pytz
import logging
import sys
from decimal import Decimal, InvalidOperation
from collections import Counter

# =====================================================
# Logging 
# =====================================================

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)]
)

log = logging.getLogger("task1-pipeline")

# =====================================================
# Config
# =====================================================

BKK = pytz.timezone("Asia/Bangkok")

CSV_PATH = "/home/ap_limpastan/task1-data-pipeline/data/data_storytelling.csv"
OUTPUT_CSV = "/home/ap_limpastan/task1-data-pipeline/result/task1_out.csv"
BQ_TABLE = "gcp-test-data-engineer:exam_limpastan.task1_data_result"

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

# =====================================================
# Data quality counters
# =====================================================

dq = Counter()

# =====================================================
# Decimal parser (BIGNUMERIC safe)
# =====================================================

BIGNUMERIC_MAX = Decimal("5.78960446186581E+38")
BIGNUMERIC_MIN = -BIGNUMERIC_MAX

def parse_decimal(v):
    if v is None or pd.isna(v):
        dq["decimal.null"] += 1
        return None
    try:
        d = Decimal(str(v))
        if BIGNUMERIC_MIN <= d <= BIGNUMERIC_MAX:
            return str(d)
        dq["decimal.out_of_range"] += 1
        return None
    except InvalidOperation:
        dq["decimal.invalid"] += 1
        return None

# =====================================================
# Timestamp parser
# =====================================================

def parse_timestamp(v):
    if pd.isna(v):
        dq["timestamp.null"] += 1
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

    except Exception:
        pass

    dq["timestamp.invalid"] += 1
    return None

# =====================================================
# Boolean parser
# =====================================================

def parse_boolean(v):
    if pd.isna(v):
        dq["boolean.null"] += 1
        return None

    v = str(v).strip().lower()

    if v in ["true", "yes", "ok", "1"]:
        return True
    if v == "-":
        dq["boolean.placeholder"] += 1
        return None

    dq["boolean.invalid"] += 1
    return False

# =====================================================
# Holiday extractor
# =====================================================

def extract_holiday(text):
    if not text or pd.isna(text):
        dq["holiday.null"] += 1
        return None

    text = re.sub(r"\s+", " ", text).strip()

    # match = re.search(r"([A-Z][A-Za-z']*(?: [A-Z][A-Za-z']*)*) Day", text)
    match = re.search(
        r"([A-Z][A-Za-z']*(?: [A-Z][A-Za-z']*)*) (Day|Festival)",
        text
    )

    if not match:
        dq["holiday.not_found"] += 1
        return None

    # holiday = match.group(0)
    holiday = match.group(0).strip()

    # remove leading "The "
    holiday = re.sub(r"^The\s+", "", holiday, flags=re.IGNORECASE)


    for prefix in [
        "On ", "on ", "During ", "during ",
        "Buddhists ", "Buddhist ",
        "National ", "Royal "
    ]:
        if holiday.startswith(prefix):
            holiday = holiday[len(prefix):]

    return holiday.strip()

# =====================================================
# Integer normalization (BigQuery INT64 safe)
# =====================================================

INT64_MIN = -9223372036854775808
INT64_MAX =  9223372036854775807

def normalize_integer_col(series):
    cleaned = []

    for v in series:
        if v is None or pd.isna(v):
            dq["integer.null"] += 1
            cleaned.append(pd.NA)
            continue

        s = str(v).replace(",", "").strip()

        if re.fullmatch(r"-?\d+", s):
            try:
                i = int(s)
                if INT64_MIN <= i <= INT64_MAX:
                    cleaned.append(i)
                else:
                    dq["integer.out_of_range"] += 1
                    cleaned.append(pd.NA)
            except:
                dq["integer.invalid"] += 1
                cleaned.append(pd.NA)
            continue

        try:
            f = float(s)
            if f.is_integer():
                i = int(f)
                if INT64_MIN <= i <= INT64_MAX:
                    cleaned.append(i)
                else:
                    dq["integer.out_of_range"] += 1
                    cleaned.append(pd.NA)
            else:
                dq["integer.invalid"] += 1
                cleaned.append(pd.NA)
        except:
            dq["integer.invalid"] += 1
            cleaned.append(pd.NA)

    return pd.Series(cleaned, dtype="Int64")

# =====================================================
# Main
# =====================================================

def main():
    log.info("Reading CSV from %s", CSV_PATH)

    raw = pd.read_csv(
        CSV_PATH,
        engine="python",
        on_bad_lines="skip",
        dtype=str
    )

    log.info("Read %s valid rows", len(raw))

    results = []

    for idx, row in enumerate(raw.itertuples(index=False), start=1):
        try:
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
        except Exception as e:
            dq["row.failed"] += 1
            log.error("Row %s failed: %s", idx, e, exc_info=True)

    df_out = pd.DataFrame(results)

    df_out["integer_col"] = normalize_integer_col(df_out["integer_col"])

    log.info("Output schema:")
    log.info("\n%s", df_out.dtypes)

    log.info("Data quality summary:")
    for k, v in dq.items():
        log.info("  %s = %s", k, v)

    log.info("Writing CSV to %s", OUTPUT_CSV)
    df_out.to_csv(OUTPUT_CSV, index=False)

    log.info("Loading %s rows into %s", len(df_out), BQ_TABLE)

    try:
        subprocess.run([
            "bq", "load",
            "--source_format=CSV",
            "--skip_leading_rows=1",
            "--replace",
            BQ_TABLE,
            OUTPUT_CSV,
            "row_id:INTEGER,integer_col:INTEGER,decimal_col:BIGNUMERIC,timestamp_col:TIMESTAMP,boolean_col:BOOL,holiday_name:STRING,business_datetime:TIMESTAMP,created_datetime:TIMESTAMP"
        ], check=True)

        log.info("BigQuery load completed successfully")

    except subprocess.CalledProcessError as e:
        log.critical("BigQuery load failed")
        raise

if __name__ == "__main__":
    main()
