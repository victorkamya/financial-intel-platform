"""
End-to-end validation script for the financial intelligence platform.

Checks, in order:
  1. Row counts across Bronze/Silver/Gold (fails if any expected table is
     completely empty AND has no reasonable excuse -- e.g. Gold depending on
     Bronze data that hasn't been captured yet is reported, not hard-failed)
  2. Data lineage recorded end-to-end into Gold
  3. DLT pipeline data-quality expectation pass/fail counts
  4. The Model Serving endpoint responds to a real query
  5. The Lambda DLQ path: inject a malformed Kinesis record, confirm it's
     routed to the S3 DLQ prefix

Run: infra-deploy/venv/Scripts/python.exe ml-pipeline/scripts/validate_pipeline.py
Requires: DATABRICKS_HOST/DATABRICKS_TOKEN (local user env vars) and the
financial-intel-deploy AWS credentials (source infra-deploy/set-env.ps1 first).
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "9e3e387ce215ee34"
PIPELINE_ID = "f9afbce9-bc67-4272-b132-87daffd30823"
SERVING_ENDPOINT = "financial-analyst-agent"
AWS_REGION = "us-west-2"
KINESIS_STREAM = "financial-intel-platform-z7oxnm-market-data"
BRONZE_BUCKET = "financial-intel-platform-z7oxnm-bronze"
DLQ_PREFIX = "dlq"

w = WorkspaceClient()
kinesis = boto3.client("kinesis", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

results = []


def check(name: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    results.append((name, status, detail))
    print(f"[{status}] {name}{' — ' + detail if detail else ''}")


def run_sql(statement: str):
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=WAREHOUSE_ID, wait_timeout="30s"
    )
    if resp.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    return resp.result.data_array or []


# ── 1. Row counts ────────────────────────────────────────────────────────────
print("\n--- 1. Table row counts ---")
TABLES = [
    "bronze.market_data.raw_trades",
    "silver.market_data.silver_trades",
    "silver.market_data.silver_ohlcv",
    "silver.market_data.silver_positions_current",
    "gold.market_data.gold_dim_symbol",
    "gold.market_data.gold_dim_date",
    "gold.market_data.gold_fact_daily_ohlcv",
]
counts = {}
for table in TABLES:
    rows = run_sql(f"SELECT COUNT(*) FROM {table}")
    n = int(rows[0][0])
    counts[table] = n
    print(f"  {table}: {n} rows")

check("bronze.market_data.raw_trades has data", counts["bronze.market_data.raw_trades"] > 0)
check(
    "silver.market_data.silver_positions_current has data (synthetic CDC)",
    counts["silver.market_data.silver_positions_current"] > 0,
)
if counts["bronze.market_data.raw_trades"] > 0:
    check("silver.market_data.silver_trades populated from Bronze", counts["silver.market_data.silver_trades"] > 0,
          "re-run the DLT pipeline if this is 0 with nonzero Bronze data")
    check("gold.market_data.gold_fact_daily_ohlcv populated", counts["gold.market_data.gold_fact_daily_ohlcv"] > 0,
          "re-run the DLT pipeline if this is 0 with nonzero Bronze data")
else:
    print("  (Bronze is empty -- Silver/Gold trade tables expected empty too, not a failure)")

# ── 2. Lineage ───────────────────────────────────────────────────────────────
print("\n--- 2. Lineage into Gold ---")
lineage_rows = run_sql("""
    SELECT COUNT(*) FROM system.access.table_lineage
    WHERE target_table_full_name LIKE 'gold.%'
""")
lineage_count = int(lineage_rows[0][0])
check("Lineage recorded for Gold tables", lineage_count > 0, f"{lineage_count} lineage edge(s)")

# ── 3. DLT expectations ──────────────────────────────────────────────────────
print("\n--- 3. DLT data-quality expectations ---")
try:
    exp_rows = run_sql(f"""
        SELECT
            exp.value:name::string AS name,
            SUM(exp.value:passed_records::bigint) AS passed,
            SUM(exp.value:failed_records::bigint) AS failed
        FROM event_log('{PIPELINE_ID}'),
        LATERAL VARIANT_EXPLODE(PARSE_JSON(details):flow_progress.data_quality.expectations) AS exp
        WHERE event_type = 'flow_progress'
        GROUP BY 1
    """)
    if exp_rows:
        for name, passed, failed in exp_rows:
            print(f"  {name}: passed={passed} failed={failed}")
        check("DLT expectations recorded", True, f"{len(exp_rows)} expectation(s) tracked")
    else:
        check("DLT expectations recorded", False, "no expectation events found -- pipeline may not have run against real data yet")
except Exception as e:
    check("DLT expectations recorded", False, f"query failed: {e}")

# ── 4. Model Serving endpoint ────────────────────────────────────────────────
print("\n--- 4. Model Serving endpoint ---")
try:
    ep = w.serving_endpoints.get(SERVING_ENDPOINT)
    is_ready = ep.state.ready.value == "READY"
    check("Serving endpoint is READY", is_ready, f"state={ep.state.ready.value}")

    if is_ready:
        resp = w.serving_endpoints.query(
            name=SERVING_ENDPOINT,
            dataframe_records=[{"query": "What are my current portfolio positions?"}],
        )
        answer = resp.predictions
        check("Serving endpoint answers a real query", bool(answer), str(answer)[:150])
except Exception as e:
    check("Serving endpoint responds", False, str(e)[:200])

# ── 5. Lambda DLQ path ───────────────────────────────────────────────────────
print("\n--- 5. Lambda DLQ routing ---")
marker = f"validation-{int(time.time())}"
malformed = {"exchange": "TEST", "note": marker}
kinesis.put_record(
    StreamName=KINESIS_STREAM,
    Data=json.dumps(malformed).encode("utf-8"),
    PartitionKey="VALIDATION",
)
print(f"  injected malformed record (marker={marker}), waiting for Lambda to route it...")

found = False
now = datetime.now(timezone.utc)
prefix = f"{DLQ_PREFIX}/{now:%Y/%m/%d}"
for attempt in range(6):
    time.sleep(5)
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    for obj in resp.get("Contents", []):
        body = s3.get_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])["Body"].read()
        if marker in body.decode("utf-8", errors="ignore"):
            found = True
            check("Malformed record routed to S3 DLQ", True, obj["Key"])
            break
    if found:
        break
if not found:
    check("Malformed record routed to S3 DLQ", False, "not found after 30s -- check Lambda logs")

# ── Summary ──────────────────────────────────────────────────────────────────
print("\n=== SUMMARY ===")
passed = sum(1 for _, status, _ in results if status == "PASS")
print(f"{passed}/{len(results)} checks passed")
for name, status, detail in results:
    print(f"  [{status}] {name}")
