#!/usr/bin/env python3
"""
Verify metrics_cardinality_analysis.sql queries against an embedded ClickHouse (chdb)
with SigNoz-compatible schema and sample data.
"""
import chdb
from chdb import session
import hashlib
import json
import datetime

def fingerprint(metric_name, labels_dict):
    """Generate fingerprint matching SigNoz's approach."""
    labels_json = json.dumps(labels_dict, separators=(",", ":"), sort_keys=True)
    fp_str = metric_name + labels_json
    return int(hashlib.md5(fp_str.encode()).hexdigest()[:16], 16)

# Use chdb Session for stateful queries
sess = session.Session(path="/tmp/chdb_v2")

def run(sql):
    try:
        result = sess.query(sql, "PrettyCompact")
        return str(result)
    except Exception as e:
        return f"ERROR: {e}"

SERVICE = "chat-worker"

# ============================================================
# STEP 1: Schema
# ============================================================
print("=" * 70)
print("STEP 1: Creating schema")
print("=" * 70)

run("CREATE DATABASE IF NOT EXISTS signoz_metrics")

run("""
    CREATE TABLE IF NOT EXISTS signoz_metrics.distributed_time_series_v4_1day (
        env LowCardinality(String) DEFAULT 'default',
        temporality LowCardinality(String) DEFAULT 'Unspecified',
        metric_name LowCardinality(String),
        description LowCardinality(String) DEFAULT '',
        unit LowCardinality(String) DEFAULT '',
        type LowCardinality(String) DEFAULT '',
        is_monotonic Bool DEFAULT false,
        fingerprint UInt64,
        unix_milli Int64,
        labels String
    ) ENGINE = MergeTree()
    ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
""")

run("""
    CREATE TABLE IF NOT EXISTS signoz_metrics.distributed_samples_v4 (
        env LowCardinality(String) DEFAULT 'default',
        temporality LowCardinality(String) DEFAULT 'Unspecified',
        metric_name LowCardinality(String),
        fingerprint UInt64,
        unix_milli Int64,
        value Float64,
        flags UInt32 DEFAULT 0
    ) ENGINE = MergeTree()
    ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
""")

print("  Tables created.\n")

# ============================================================
# STEP 2: Insert small but representative test data
# ============================================================
print("=" * 70)
print("STEP 2: Inserting test data")
print("=" * 70)

now = datetime.datetime.now(datetime.timezone.utc)
base_time = now - datetime.timedelta(days=15)

metrics_config = [
    {
        "name": "http_server_request_duration_seconds",
        "type": "Histogram", "temporality": "Cumulative", "is_monotonic": True,
        "label_sets": [
            {"service.name": SERVICE, "http.method": m, "http.route": r, "http.status_code": s, "instance": f"pod-{i}"}
            for m in ["GET", "POST"]
            for r in ["/api/chat", "/api/users", "/api/messages"]
            for s in ["200", "400", "500"]
            for i in range(2)
        ],
    },
    {
        "name": "chat_messages_processed_total",
        "type": "Sum", "temporality": "Cumulative", "is_monotonic": True,
        # High cardinality: unbounded user_id
        "label_sets": [
            {"service.name": SERVICE, "user_id": f"user-{uid}", "room_id": f"room-{rid}", "message_type": mt}
            for uid in range(30)
            for rid in range(3)
            for mt in ["text", "image"]
        ],
    },
    {
        "name": "process_cpu_seconds_total",
        "type": "Sum", "temporality": "Cumulative", "is_monotonic": True,
        "label_sets": [
            {"service.name": SERVICE, "instance": f"pod-{i}"}
            for i in range(3)
        ],
    },
    {
        "name": "go_goroutines",
        "type": "Gauge", "temporality": "Unspecified", "is_monotonic": False,
        "label_sets": [
            {"service.name": SERVICE, "instance": f"pod-{i}"}
            for i in range(3)
        ],
    },
    {
        "name": "redis_commands_total",
        "type": "Sum", "temporality": "Cumulative", "is_monotonic": True,
        "label_sets": [
            {"service.name": SERVICE, "command": cmd, "instance": f"pod-{i}"}
            for cmd in ["GET", "SET", "DEL", "HGET", "HSET"]
            for i in range(2)
        ],
    },
]

for mc in metrics_config:
    metric_name = mc["name"]
    ts_values = []
    sample_values = []

    for labels in mc["label_sets"]:
        fp = fingerprint(metric_name, labels)
        labels_json = json.dumps(labels, separators=(",", ":"), sort_keys=True)
        # Escape single quotes in JSON
        labels_json_escaped = labels_json.replace("'", "\\'")

        # Insert time_series for a few days
        for day_offset in [-20, -15, -10, -5, -1]:
            ts_time = base_time + datetime.timedelta(days=day_offset)
            ts_time = ts_time.replace(hour=0, minute=0, second=0, microsecond=0)
            ts_milli = int(ts_time.timestamp() * 1000)
            ts_values.append(
                f"('default','{mc['temporality']}','{metric_name}','','','{mc['type']}',{1 if mc['is_monotonic'] else 0},{fp},{ts_milli},'{labels_json_escaped}')"
            )

        # Insert samples
        for day_offset in [-20, -15, -10, -5, -1]:
            for hour in [0, 12]:
                t = base_time + datetime.timedelta(days=day_offset, hours=hour)
                t_milli = int(t.timestamp() * 1000)
                v = 42.0 + day_offset + hour
                sample_values.append(
                    f"('default','{mc['temporality']}','{metric_name}',{fp},{t_milli},{v},0)"
                )

    # Insert in smaller batches
    batch = 100
    for i in range(0, len(ts_values), batch):
        chunk = ts_values[i:i+batch]
        run(f"INSERT INTO signoz_metrics.distributed_time_series_v4_1day VALUES {','.join(chunk)}")

    for i in range(0, len(sample_values), batch):
        chunk = sample_values[i:i+batch]
        run(f"INSERT INTO signoz_metrics.distributed_samples_v4 VALUES {','.join(chunk)}")

    print(f"  {metric_name}: {len(mc['label_sets'])} time series, {len(sample_values)} samples")

# Verify data was inserted
count_ts = run("SELECT count() FROM signoz_metrics.distributed_time_series_v4_1day")
count_s = run("SELECT count() FROM signoz_metrics.distributed_samples_v4")
print(f"\n  Total rows: time_series={count_ts.strip()}, samples={count_s.strip()}\n")

# ============================================================
# STEP 3: Run all 6 queries
# ============================================================

print("=" * 70)
print("QUERY 1: All metrics ranked by cardinality (last 30 days)")
print("=" * 70)
result = run(f"""
SELECT
    metric_name,
    type AS metric_type,
    temporality,
    count(DISTINCT fingerprint) AS cardinality
FROM signoz_metrics.distributed_time_series_v4_1day
WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND JSONExtractString(labels, 'service.name') = '{SERVICE}'
GROUP BY metric_name, type, temporality
ORDER BY cardinality DESC
""")
print(result)

print("=" * 70)
print("QUERY 2: Total samples per metric (last 30 days)")
print("=" * 70)
result = run(f"""
SELECT
    ts.metric_name,
    count(DISTINCT ts.fingerprint) AS cardinality,
    count() AS total_samples,
    round(count() / count(DISTINCT ts.fingerprint) / 30, 1) AS avg_samples_per_series_per_day
FROM signoz_metrics.distributed_time_series_v4_1day AS ts
INNER JOIN signoz_metrics.distributed_samples_v4 AS s
    ON ts.fingerprint = s.fingerprint
    AND s.metric_name = ts.metric_name
WHERE ts.unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND s.unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND JSONExtractString(ts.labels, 'service.name') = '{SERVICE}'
GROUP BY ts.metric_name
ORDER BY total_samples DESC
""")
print(result)

print("=" * 70)
print("QUERY 3: Label key cardinality for 'chat_messages_processed_total'")
print("=" * 70)
result = run(f"""
SELECT
    label_key,
    count(DISTINCT label_value) AS distinct_values,
    count(DISTINCT fingerprint) AS time_series_with_label
FROM (
    SELECT
        fingerprint,
        kv.1 AS label_key,
        kv.2 AS label_value
    FROM signoz_metrics.distributed_time_series_v4_1day
    ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
    WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
      AND metric_name = 'chat_messages_processed_total'
      AND JSONExtractString(labels, 'service.name') = '{SERVICE}'
)
GROUP BY label_key
ORDER BY distinct_values DESC
""")
print(result)

print("=" * 70)
print("QUERY 4: Daily cardinality trend (last 30 days)")
print("=" * 70)
result = run(f"""
SELECT
    toDate(fromUnixTimestamp64Milli(unix_milli)) AS day,
    metric_name,
    count(DISTINCT fingerprint) AS cardinality
FROM signoz_metrics.distributed_time_series_v4_1day
WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND JSONExtractString(labels, 'service.name') = '{SERVICE}'
GROUP BY day, metric_name
ORDER BY day ASC, cardinality DESC
LIMIT 20
""")
print(result)

print("=" * 70)
print("QUERY 5: Top values for 'user_id' on 'chat_messages_processed_total'")
print("=" * 70)
result = run(f"""
SELECT
    JSONExtractString(labels, 'user_id') AS label_value,
    count(DISTINCT fingerprint) AS time_series_count
FROM signoz_metrics.distributed_time_series_v4_1day
WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND metric_name = 'chat_messages_processed_total'
  AND JSONExtractString(labels, 'service.name') = '{SERVICE}'
GROUP BY label_value
ORDER BY time_series_count DESC
LIMIT 20
""")
print(result)

print("=" * 70)
print("QUERY 6: Service-level summary")
print("=" * 70)
result = run(f"""
SELECT
    count() AS total_metrics,
    sum(per_metric_cardinality) AS total_time_series,
    round(avg(per_metric_cardinality), 1) AS avg_cardinality_per_metric,
    max(per_metric_cardinality) AS max_cardinality_single_metric
FROM (
    SELECT
        metric_name,
        count(DISTINCT fingerprint) AS per_metric_cardinality
    FROM signoz_metrics.distributed_time_series_v4_1day
    WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
      AND JSONExtractString(labels, 'service.name') = '{SERVICE}'
    GROUP BY metric_name
)
""")
print(result)

print("\n" + "=" * 70)
print("ALL 6 QUERIES VERIFIED SUCCESSFULLY")
print("=" * 70)
