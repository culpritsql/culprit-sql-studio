/* culprit */
SELECT hostName() AS node, database, table, hex(SHA256(data_path)) AS queue_id, is_blocked, error_count, data_files, data_compressed_bytes,
    toUnixTimestamp(last_exception_time) AS last_error_seconds
FROM system.distribution_queue
WHERE database = currentDatabase()
ORDER BY is_blocked DESC, error_count DESC, data_files DESC, table, queue_id
LIMIT {row_limit:UInt32}
