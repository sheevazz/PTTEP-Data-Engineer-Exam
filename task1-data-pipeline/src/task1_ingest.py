import os
import subprocess
import pandas as pd
import re
from datetime import datetime, timezone
import pytz
import logging
import sys
from decimal import Decimal, ROUND_HALF_UP
from collections import Counter
from collections import defaultdict

dq = Counter()
dq_samples = defaultdict(list)   # keep examples of bad values
MAX_SAMPLES = 5                  # don't spam logs

def record_dq(key, value):
    dq[key] += 1
    if len(dq_samples[key]) < MAX_SAMPLES:
        dq_samples[key].append(value)


# =====================================================
# Logging
# =====================================================

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("task1-pipeline")

# =====================================================
# Config
# =====================================================

BKK = pytz.timezone("Asia/Bangkok")

CSV_PATH = "/home/ap_limpastan/task1-data-pipeline/data/data_storytelling.csv"
OUTPUT_CSV = "/home/ap_limpastan/task1-data-pipeline/result/task1_out.csv"
BQ_TABLE = "gcp-test-data-engineer:exam_limpastan.task1_data_result"

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

dq = Counter()

# =====================================================
# CSV reader (deterministic, no regex, no guessing)
# =====================================================

def read_rows(path):
    rows = []

    with open(path, "r", encoding="utf-8") as f:
        f.readline()  # skip header

        for line_no, line in enumerate(f, start=2):
            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split(",", 4)

            if len(parts) < 5:
                dq["row.unrecoverable"] += 1
                continue

            rows.append({
                "integer_col": parts[0].strip(),
                "decimal_col": parts[1].strip(),
                "timestamp_col": parts[2].strip(),
                "boolean_col": parts[3].strip(),
                "holiday_name": parts[4].strip()
            })

    return rows


# =====================================================
# Integer → INT64
# =====================================================

INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807

def normalize_integer(v):
    if not v:
        record_dq("integer.null", v)
        return None

    s = v.replace(",", "").strip()

    if re.fullmatch(r"-?\d+", s):
        i = int(s)
        if INT64_MIN <= i <= INT64_MAX:
            return str(i)
        record_dq("integer.out_of_range", v)
        return None

    record_dq("integer.invalid", v)
    return None


# =====================================================
# Timestamp
# =====================================================

def parse_timestamp(v):
    if not v:
        dq["timestamp.invalid"] += 1
        return None

    try:
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", v):
            d, m, y = v.split("/")
            return f"{y}-{m}-{d} 00:00:00"

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            return v + " 00:00:00"

        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v):
            return v

        if re.fullmatch(r"\d{14}", v):
            return datetime.strptime(v, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")

        if re.fullmatch(r"\d{2}-[A-Za-z]{3}-\d{2}", v):
            day, mon, yr = v.split("-")
            if yr == "06":
                month = datetime.strptime(mon, "%b").month
                return f"2026-{month:02d}-{day} 00:00:00"

    except:
        pass

    dq["timestamp.invalid"] += 1
    return None

# =====================================================
# Boolean
# =====================================================

def parse_boolean(v):
    if v is None or str(v).strip() == "":
        return False

    s = str(v).strip().lower()

    # invalid placeholder
    if s == "-":
        record_dq("boolean.invalid", v)
        return None

    # TRUE values
    if s in ["true", "yes", "ok", "1"]:
        return True

    # everything else → False (valid)
    return False


# =====================================================
# Holiday
# =====================================================

def extract_holiday(text):
    if not text:
        record_dq("holiday.null", text)
        return None

    text = re.sub(r"\s+", " ", text).strip()

    # Remove narrative prefixes
    text = re.sub(r"^(On|After|During|The day of|the day of)\s+", "", text, flags=re.IGNORECASE)

    patterns = [
        # Makha Bucha Day, Songkran Festival, Royal Ploughing Ceremony
        r"([A-Z][A-Za-z']*(?: [A-Z][A-Za-z']*)*) (Day|Festival|Ceremony)",

        # King's Birthday, Queen's Birthday
        r"([A-Z][A-Za-z']*'s Birthday)",

        # Makha Bucha (without "Day")
        r"(Makha Bucha|Visakha Bucha|Asalha Bucha)"
    ]

    for p in patterns:
        m = re.search(p, text)
        if m:
            return m.group(1)

    record_dq("holiday.not_found", text)
    return None



# =====================================================
# BIGNUMERIC
# =====================================================

BQ_MAX_INT = 38
BQ_SCALE = 9

def to_bq_bignumeric(v):
    if not v:
        return None
    try:
        d = Decimal(v)
        sign, digits, exp = d.as_tuple()
        int_digits = len(digits) + exp if exp >= 0 else len(digits[:exp])

        if int_digits > BQ_MAX_INT:
            record_dq("decimal.out_of_range", v)
            return None

        if -exp > BQ_SCALE:
            d = d.quantize(Decimal("1." + "0" * BQ_SCALE), rounding=ROUND_HALF_UP)

        return format(d, "f")
    except:
        record_dq("decimal.invalid", v)
        return None

# =====================================================
# Main
# =====================================================

def main():
    raw_rows = read_rows(CSV_PATH)
    log.info("Recovered %s rows", len(raw_rows))

    records = []
    for i, row in enumerate(raw_rows, start=1):
        records.append({
            "row_id": i,
            "integer_col": normalize_integer(row["integer_col"]),
            "decimal_col": to_bq_bignumeric(row["decimal_col"]),
            "timestamp_col": parse_timestamp(row["timestamp_col"]),
            "boolean_col": parse_boolean(row["boolean_col"]),
            "holiday_name": extract_holiday(row["holiday_name"]),
            "business_datetime": datetime.now(BKK),
            "created_datetime": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(records)

    log.info("Writing %s rows to CSV", len(df))

    log.info("========== DATA QUALITY REPORT ==========")
    for k, v in dq.items():
        log.info("%s = %s", k, v)
        if k in dq_samples:
            for s in dq_samples[k]:
                log.info("   sample → %s", s[:200])
    log.info("=========================================")

    df.to_csv(OUTPUT_CSV, index=False)

    subprocess.run([
        "bq", "load",
        "--source_format=CSV",
        "--skip_leading_rows=1",
        "--replace",
        BQ_TABLE,
        OUTPUT_CSV,
        "row_id:INTEGER,integer_col:INTEGER,decimal_col:BIGNUMERIC,timestamp_col:TIMESTAMP,boolean_col:BOOL,holiday_name:STRING,business_datetime:TIMESTAMP,created_datetime:TIMESTAMP"
    ], check=True)

    log.info("BigQuery load complete")

if __name__ == "__main__":
    main()
