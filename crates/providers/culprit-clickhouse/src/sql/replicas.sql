/* culprit */
SELECT hostName() AS node, database, table, replica_name, is_readonly, is_session_expired, queue_size,
    toUnixTimestamp(queue_oldest_time) AS oldest_seconds, absolute_delay
FROM system.replicas
WHERE database = currentDatabase()
ORDER BY is_readonly DESC, absolute_delay DESC, queue_size DESC, table, replica_name
LIMIT {row_limit:UInt32}
