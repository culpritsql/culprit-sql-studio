/* culprit */
SELECT hostName() AS node, database, table, query_id, nullIf(system.asynchronous_insert_log.flush_query_id, '') AS flush_query_id, toString(status) AS status,
    toUnixTimestamp64Micro(event_time_microseconds) AS queued_us,
    nullIf(toUnixTimestamp64Micro(flush_time_microseconds), 0) AS flushed_us, bytes
FROM system.asynchronous_insert_log
PREWHERE event_date >= toDate({since:DateTime64(6, 'UTC')}, timezone()) AND event_date <= toDate({until:DateTime64(6, 'UTC')}, timezone())
WHERE database = currentDatabase() AND event_time_microseconds >= {since:DateTime64(6, 'UTC')}
    AND event_time_microseconds < {until:DateTime64(6, 'UTC')}
ORDER BY event_time_microseconds DESC, query_id, flush_query_id
LIMIT {row_limit:UInt32}
