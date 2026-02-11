-- =============================================================================
-- SigNoz Metrics Cardinality Analysis Queries
-- =============================================================================
-- Purpose: Investigate high metrics cost for a service by analyzing cardinality,
--          sample volume, label breakdown, and trends over the last 30 days.
--
-- Usage:   Replace <YOUR_SERVICE> with your service name (e.g., 'payment-service').
--          Replace <YOUR_METRIC> and <YOUR_LABEL> in drill-down queries after
--          identifying high-cardinality metrics from Query 1.
--
-- Database: signoz_metrics
-- Tables:   distributed_time_series_v4_1day (metadata, 1-day granularity)
--           distributed_samples_v4 (metric values)
--
-- Run in:   ClickHouse client or any SQL editor connected to your SigNoz ClickHouse.
-- =============================================================================


-- =============================================================================
-- QUERY 1: All metrics for a service, ranked by cardinality (last 30 days)
-- =============================================================================
-- What:  Lists every metric streamed by the service with its cardinality
--        (number of unique time series = unique label combinations).
-- Why:   Metrics with high cardinality are the primary cost driver. Start here.
-- Read:  "cardinality" = number of unique fingerprints = unique label combos.
--        A metric with cardinality 50,000 means 50K distinct time series.

SELECT
    metric_name,
    type AS metric_type,
    temporality,
    count(DISTINCT fingerprint) AS cardinality
FROM signoz_metrics.distributed_time_series_v4_1day
WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND JSONExtractString(labels, 'service.name') = '<YOUR_SERVICE>'
GROUP BY metric_name, type, temporality
ORDER BY cardinality DESC;


-- =============================================================================
-- QUERY 2: Total samples (data points) per metric (last 30 days)
-- =============================================================================
-- What:  Counts actual data points ingested per metric for the service.
-- Why:   Cardinality alone doesn't tell the full cost story. A metric with
--        moderate cardinality but very frequent scraping can generate more
--        samples (and cost) than a high-cardinality metric scraped rarely.
-- Note:  This query joins time_series with samples and may be slow on large
--        datasets. Consider narrowing the time range if needed.

SELECT
    ts.metric_name,
    count(DISTINCT ts.fingerprint) AS cardinality,
    count() AS total_samples,
    -- Estimated samples per time series per day (rough cost indicator)
    round(count() / count(DISTINCT ts.fingerprint) / 30, 1) AS avg_samples_per_series_per_day
FROM signoz_metrics.distributed_time_series_v4_1day AS ts
INNER JOIN signoz_metrics.distributed_samples_v4 AS s
    ON ts.fingerprint = s.fingerprint
    AND s.metric_name = ts.metric_name
WHERE ts.unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND s.unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND JSONExtractString(ts.labels, 'service.name') = '<YOUR_SERVICE>'
GROUP BY ts.metric_name
ORDER BY total_samples DESC;


-- =============================================================================
-- QUERY 3: Label key cardinality breakdown for a specific metric
-- =============================================================================
-- What:  For a given high-cardinality metric, shows how many distinct values
--        each label key has. The label with the most distinct values is the
--        one "exploding" your cardinality.
-- Why:   If a label like `request_id` or `user_id` has 100K distinct values,
--        that's your problem — it should be a trace attribute, not a metric label.
-- Usage: Replace <YOUR_METRIC> with a metric name from Query 1 results.

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
      AND metric_name = '<YOUR_METRIC>'
      AND JSONExtractString(labels, 'service.name') = '<YOUR_SERVICE>'
)
GROUP BY label_key
ORDER BY distinct_values DESC;


-- =============================================================================
-- QUERY 4: Daily cardinality trend for the service (last 30 days)
-- =============================================================================
-- What:  Shows how cardinality changed day by day, per metric.
-- Why:   Helps you identify when a cardinality spike happened so you can
--        correlate it with deployments, config changes, or new label additions.
-- Tip:   Look for sudden jumps in cardinality — that's when the cost increased.

SELECT
    toDate(fromUnixTimestamp64Milli(unix_milli)) AS day,
    metric_name,
    count(DISTINCT fingerprint) AS cardinality
FROM signoz_metrics.distributed_time_series_v4_1day
WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND JSONExtractString(labels, 'service.name') = '<YOUR_SERVICE>'
GROUP BY day, metric_name
ORDER BY day ASC, cardinality DESC;


-- =============================================================================
-- QUERY 5: Top values for a specific high-cardinality label
-- =============================================================================
-- What:  Shows the most common values for a label that has too many distinct
--        values (identified from Query 3).
-- Why:   Confirms whether the label is unbounded (e.g., UUIDs, IPs, timestamps)
--        or if a finite set of values just happens to be large.
-- Usage: Replace <YOUR_METRIC> and <YOUR_LABEL> based on Query 3 results.

SELECT
    JSONExtractString(labels, '<YOUR_LABEL>') AS label_value,
    count(DISTINCT fingerprint) AS time_series_count
FROM signoz_metrics.distributed_time_series_v4_1day
WHERE unix_milli >= toUnixTimestamp64Milli(now64() - INTERVAL 30 DAY)
  AND metric_name = '<YOUR_METRIC>'
  AND JSONExtractString(labels, 'service.name') = '<YOUR_SERVICE>'
GROUP BY label_value
ORDER BY time_series_count DESC
LIMIT 50;


-- =============================================================================
-- QUERY 6: Service-level summary (last 30 days)
-- =============================================================================
-- What:  Single-row overview of the service's total metrics footprint.
-- Why:   Quick sanity check — if total_time_series is in the hundreds of
--        thousands, that's likely the cost problem right there.

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
      AND JSONExtractString(labels, 'service.name') = '<YOUR_SERVICE>'
    GROUP BY metric_name
);
