/* culprit */
SELECT hostName() AS node, currentDatabase() AS database, query_id, initial_query_id, toString(normalized_query_hash) AS family,
    toUnixTimestamp64Micro(query_start_time_microseconds) AS started_us,
    toUnixTimestamp64Micro(event_time_microseconds) AS ended_us,
    toString(type) AS outcome, exception_code, errorCodeToName(exception_code) AS exception_name,
    query_duration_ms, read_rows, read_bytes, written_rows, written_bytes, memory_usage,
    arraySlice(system.query_log.tables, 1, 32) AS tables, length(system.query_log.tables) AS total_tables,
    mapFilter((k, v) -> has(splitByChar(',', {profile_events:String}), k), ProfileEvents) AS profile
FROM system.query_log
PREWHERE event_date >= toDate({since:DateTime64(6, 'UTC')}, timezone()) AND event_date <= toDate({until:DateTime64(6, 'UTC')}, timezone())
WHERE event_time_microseconds >= {since:DateTime64(6, 'UTC')} AND event_time_microseconds < {until:DateTime64(6, 'UTC')}
    AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart') AND is_initial_query = 1
    AND databases = [currentDatabase()] AND NOT startsWith(query, '/* culprit */')
ORDER BY event_time_microseconds DESC, query_id, query_start_time_microseconds
LIMIT {row_limit:UInt32}
