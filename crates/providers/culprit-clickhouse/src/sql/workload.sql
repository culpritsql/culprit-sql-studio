/* culprit */
WITH argMax((substring(query, 1, {text_bytes:UInt32}), length(query) <= {text_bytes:UInt32}), event_time_microseconds) AS sample
SELECT hostName() AS node,
    toString(normalized_query_hash) AS family,
    count() AS executions,
    countIf(type != 'QueryFinish') AS failures,
    sum(query_duration_ms) AS elapsed_ms,
    max(query_duration_ms) AS max_ms,
    sum(read_rows) AS rows_read,
    sum(read_bytes) AS bytes_read,
    sum(result_rows) AS rows_returned,
    toString(min(query_start_time_microseconds), 'UTC') AS first_started,
    toString(max(query_start_time_microseconds), 'UTC') AS last_started,
    tupleElement(sample, 1) AS sample_sql, toUInt8(tupleElement(sample, 2)) AS sample_complete,
    sum(count()) OVER () AS population_executions,
    sum(sum(query_duration_ms)) OVER () AS population_elapsed_ms,
    count() OVER () AS population_families
FROM system.query_log
PREWHERE event_date >= toDate({since:DateTime64(6, 'UTC')}, timezone()) AND event_date <= toDate({until:DateTime64(6, 'UTC')}, timezone())
WHERE event_time_microseconds >= {since:DateTime64(6, 'UTC')} AND event_time_microseconds < {until:DateTime64(6, 'UTC')}
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
    AND is_initial_query = 1 AND databases = [currentDatabase()]
    AND NOT startsWith(query, '/* culprit */')
GROUP BY normalized_query_hash
ORDER BY elapsed_ms DESC, family
LIMIT {row_limit:UInt32}
