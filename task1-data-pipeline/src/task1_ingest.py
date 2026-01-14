import os
import subprocess
import pandas as pd
import re
from datetime import datetime, timezone
import pytz
import logging
import sys
from decimal import Decimal, ROUND_HALF_UP
from collections import Counter, defaultdict

# =====================================================
# Data quality tracking
# =====================================================

dq = Counter()
dq_samples = defaultdict(list)
MAX_SAMPLES = 5

def record_dq(key, value, line_no=None):
    dq[key] += 1
    if len(dq_samples[key]) < MAX_SAMPLES:
        if line_no:
            # dq_samples[key].append(f"[line {line_no}] {value}")
            dq_samples[key].append(f"[line {line_no}] {repr(value)}")

        else:
            dq_samples[key].append(str(value))


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

# =====================================================
# CSV reader (correct)
# =====================================================

import csv

import csv

import csv

def read_rows(path):
    rows = []

    ts_patterns = [
        r"\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?",
        r"\d{2}/\d{2}/\d{4}",
        r"\d{14}",
        r"\d{2}-[A-Za-z]{3}-\d{2}",
        r"unknown"
    ]

    ts_re = "(" + "|".join(ts_patterns) + ")"
    bool_re = r"(true|false|yes|ok|1|0|-|no)"

    pattern = re.compile(rf",\s*{ts_re}\s*,\s*{bool_re}\s*,", re.IGNORECASE)

    with open(path, "r", encoding="utf-8") as f:
        f.readline()

        for line_no, line in enumerate(f, start=2):
            line = line.rstrip("\n")
            if not line:
                continue

            m = pattern.search(line)
            if not m:
                # still keep row but everything except holiday is unknown
                record_dq("row.bad_structure", line, line_no)
                parts = line.split(",", 4)
                while len(parts) < 5:
                    parts.append("")
                rows.append({
                    "integer_col": parts[0].strip(),
                    "decimal_col": parts[1].strip(),
                    "timestamp_col": parts[2].strip(),
                    "boolean_col": parts[3].strip(),
                    "holiday_name": parts[4].strip(),
                    "_line_no": line_no
                })
                continue

            ts = m.group(1)
            boolean = m.group(2)

            left = line[:m.start()]
            right = line[m.end():]

            # left = integer , decimal
            left_parts = left.split(",", 1)
            integer = left_parts[0]
            decimal = left_parts[1] if len(left_parts) == 2 else ""

            rows.append({
                "integer_col": integer.strip(),
                "decimal_col": decimal.strip(),
                "timestamp_col": ts.strip(),
                "boolean_col": boolean.strip(),
                "holiday_name": right.strip(),
                "_line_no": line_no
            })

    return rows


# =====================================================
# Integer → INT64
# =====================================================

INT64_MIN = -9223372036854775808
INT64_MAX =  9223372036854775807

def normalize_integer(v, line_no):
    if not v:
        record_dq("integer.null", v, line_no)
        return None

    s = v.replace(",", "").strip()

    if re.fullmatch(r"-?\d+", s):
        i = int(s)
        if INT64_MIN <= i <= INT64_MAX:
            return str(i)
        record_dq("integer.out_of_range", v, line_no)
        return None

    record_dq("integer.invalid", v, line_no)
    return None


# =====================================================
# Timestamp
# =====================================================

def parse_timestamp(v, line_no):
    if not v:
        record_dq("timestamp.invalid", v, line_no)
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

    record_dq("timestamp.invalid", v, line_no)
    return None


# =====================================================
# Boolean
# =====================================================

def parse_boolean(v, line_no):
    if v is None or str(v).strip() == "":
        return False

    s = str(v).strip().lower()

    if s == "-":
        record_dq("boolean.invalid", v, line_no)
        return None

    if s in ["true", "yes", "ok", "1"]:
        return True

    return False


# =====================================================
# Holiday
# =====================================================

def extract_holiday(text, line_no):
    if not text:
        record_dq("holiday.null", text, line_no)
        return None

    clean = re.sub(r"\s+", " ", text).strip()

    patterns = [
        r"(Songkran Festival)",
        r"([A-Z][A-Za-z']*(?: [A-Z][A-Za-z']*)*) (Day|Festival|Ceremony)",
        r"([A-Z][A-Za-z']*'s Birthday)",
        r"(Makha Bucha|Visakha Bucha|Asalha Bucha|Labour Day|Labor Day|Buddhist Lent)"
    ]

    for p in patterns:
        m = re.search(p, clean)
        if m:
            return m.group(0)

    record_dq("holiday.not_found", clean, line_no)
    return None



# =====================================================
# BIGNUMERIC
# =====================================================

BQ_MAX_INT = 38
BQ_SCALE = 9

def to_bq_bignumeric(v, line_no):
    if not v:
        return None
    try:
        d = Decimal(v)
        sign, digits, exp = d.as_tuple()
        int_digits = len(digits) + exp if exp >= 0 else len(digits[:exp])

        if int_digits > BQ_MAX_INT:
            record_dq("decimal.out_of_range", v, line_no)
            return None

        if -exp > BQ_SCALE:
            d = d.quantize(Decimal("1." + "0" * BQ_SCALE), rounding=ROUND_HALF_UP)

        return format(d, "f")
    except:
        record_dq("decimal.invalid", v, line_no)
        return None


# =====================================================
# Main
# =====================================================

def main():
    raw_rows = read_rows(CSV_PATH)
    log.info("Recovered %s rows", len(raw_rows))

    records = []
    for i, row in enumerate(raw_rows, start=1):
        ln = row["_line_no"]

        records.append({
            "row_id": i,
            "integer_col": normalize_integer(row["integer_col"], ln),
            "decimal_col": to_bq_bignumeric(row["decimal_col"], ln),
            "timestamp_col": parse_timestamp(row["timestamp_col"], ln),
            "boolean_col": parse_boolean(row["boolean_col"], ln),
            "holiday_name": extract_holiday(row["holiday_name"], ln),
            "business_datetime": datetime.now(BKK),
            "created_datetime": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(records)

    log.info("Writing %s rows to CSV", len(df))

    log.info("========== DATA QUALITY REPORT ==========")
    for k, v in dq.items():
        log.info("%s = %s", k, v)
        for s in dq_samples[k]:
            log.info("   sample → %s", s)
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
