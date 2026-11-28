/* culprit */
SELECT hostName() AS node, database, table, mutation_id, toUnixTimestamp(create_time) AS created_seconds,
    parts_to_do, latest_failed_part, toUnixTimestamp(latest_fail_time) AS last_failure_seconds
FROM system.mutations
WHERE database = currentDatabase() AND NOT is_done
ORDER BY create_time, table, mutation_id
LIMIT {row_limit:UInt32}
