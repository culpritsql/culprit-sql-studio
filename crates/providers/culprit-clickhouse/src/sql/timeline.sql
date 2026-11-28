/* culprit */
SELECT hostName() AS node,
    intDiv(toUnixTimestamp64Micro(event_time_microseconds) - toUnixTimestamp64Micro({since:DateTime64(6, 'UTC')}), {width_us:Int64}) AS bucket,
    if(has({families:Array(UInt64)}, normalized_query_hash), toString(normalized_query_hash), '') AS family,
    count() AS executions, sum(query_duration_ms) AS elapsed_ms
FROM system.query_log
PREWHERE event_date >= toDate({since:DateTime64(6, 'UTC')}, timezone()) AND event_date <= toDate({until:DateTime64(6, 'UTC')}, timezone())
WHERE event_time_microseconds >= {since:DateTime64(6, 'UTC')} AND event_time_microseconds < {until:DateTime64(6, 'UTC')}
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart') AND is_initial_query = 1
    AND databases = [currentDatabase()] AND NOT startsWith(query, '/* culprit */')
GROUP BY bucket, family
ORDER BY bucket, family
