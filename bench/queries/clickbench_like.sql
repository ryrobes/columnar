-- name: count_all
SELECT COUNT(*)
FROM {logical_table};

-- name: filtered_count
SELECT COUNT(*)
FROM {logical_table}
WHERE service IN ('api', 'ingest')
  AND severity >= 3;

-- name: latency_rollup
SELECT SUM(duration_ms), AVG(payload_bytes), COUNT(*)
FROM {logical_table}
WHERE status IN (200, 429, 500);

-- name: distinct_users
SELECT COUNT(DISTINCT user_id)
FROM {logical_table}
WHERE kind = 'search';

-- name: service_topn
SELECT service, COUNT(*) AS c
FROM {logical_table}
GROUP BY service
ORDER BY c DESC
LIMIT 10;

-- name: region_day_rollup
SELECT region_id, event_date, COUNT(*) AS c, AVG(duration_ms) AS avg_ms
FROM {logical_table}
GROUP BY region_id, event_date
ORDER BY c DESC
LIMIT 25;

-- name: search_phrase_topn
SELECT search_phrase, COUNT(*) AS c
FROM {logical_table}
WHERE search_phrase <> ''
GROUP BY search_phrase
ORDER BY c DESC
LIMIT 10;

-- name: url_like
SELECT COUNT(*)
FROM {logical_table}
WHERE url LIKE '%/resource/%';

-- name: recent_window
SELECT service, COUNT(*) AS c, AVG(duration_ms) AS avg_ms
FROM {logical_table}
WHERE event_time >= {recent_window_start}
  AND event_time < {recent_window_end}
GROUP BY service
ORDER BY c DESC;

-- name: wide_sum
SELECT
    SUM(duration_ms),
    SUM(duration_ms + 1),
    SUM(duration_ms + 2),
    SUM(duration_ms + 3),
    SUM(duration_ms + 4),
    SUM(duration_ms + 5),
    SUM(duration_ms + 6),
    SUM(duration_ms + 7),
    SUM(duration_ms + 8),
    SUM(duration_ms + 9),
    SUM(duration_ms + 10),
    SUM(duration_ms + 11),
    SUM(duration_ms + 12),
    SUM(duration_ms + 13),
    SUM(duration_ms + 14),
    SUM(duration_ms + 15)
FROM {logical_table};

-- name: hot_work_slice
SELECT work_id, COUNT(*) AS c, SUM(duration_ms) AS total_ms
FROM {logical_table}
WHERE work_id BETWEEN {hot_work_lower} AND {hot_work_upper}
GROUP BY work_id
ORDER BY c DESC
LIMIT 20;

-- name: tenant_error_rollup
SELECT tenant_id, service, COUNT(*) FILTER (WHERE is_error) AS errors, COUNT(*) AS total
FROM {logical_table}
GROUP BY tenant_id, service
ORDER BY total DESC
LIMIT 20;
