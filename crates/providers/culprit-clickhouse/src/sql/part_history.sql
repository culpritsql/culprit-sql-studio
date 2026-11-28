/* culprit */
SELECT hostName() AS node, database, table, partition_id, part_name, query_id,
    toString(event_type) AS event, toUnixTimestamp64Micro(event_time_microseconds) AS occurred_us,
    duration_ms, rows, size_in_bytes, error
FROM system.part_log
PREWHERE event_date >= toDate({since:DateTime64(6, 'UTC')}, timezone()) AND event_date <= toDate({until:DateTime64(6, 'UTC')}, timezone())
WHERE database = currentDatabase() AND table = {table:String} AND hostName() = {node:String}
    AND event_time_microseconds >= {since:DateTime64(6, 'UTC')} AND event_time_microseconds < {until:DateTime64(6, 'UTC')}
ORDER BY event_time_microseconds DESC, part_name, event_type
LIMIT {row_limit:UInt32}
