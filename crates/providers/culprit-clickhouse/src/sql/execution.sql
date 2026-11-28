/* culprit */
SELECT hostName() AS node, query_id, initial_query_id, toString(type) AS outcome,
    toString(normalized_query_hash) AS family, toUnixTimestamp64Micro(query_start_time_microseconds) AS started_us,
    toUnixTimestamp64Micro(event_time_microseconds) AS ended_us, query_duration_ms,
    read_rows, read_bytes, written_rows, written_bytes, result_rows, memory_usage, exception_code,
    arraySlice(tables,1,32) AS tables, mapFilter((k, v) -> has(splitByChar(',', {profile_events:String}), k), ProfileEvents) AS profile
FROM system.query_log
PREWHERE event_date >= toDate({ended:DateTime64(6, 'UTC')}, timezone()) AND event_date <= toDate({ended:DateTime64(6, 'UTC')}, timezone())
WHERE hostName() = {node:String} AND query_id = {query_id:String}
    AND query_start_time_microseconds = {started:DateTime64(6, 'UTC')}
    AND event_time_microseconds = {ended:DateTime64(6, 'UTC')}
    AND is_initial_query = 1 AND databases = [currentDatabase()]
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart') AND NOT startsWith(query, '/* culprit */')
LIMIT 2
