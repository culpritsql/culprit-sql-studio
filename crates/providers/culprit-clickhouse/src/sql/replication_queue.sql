/* culprit */
SELECT hostName() AS node, database, table, replica_name, node_name, type, num_tries, num_postponed,
    toUnixTimestamp(create_time) AS created_seconds, toUnixTimestamp(last_attempt_time) AS attempted_seconds,
    notEmpty(last_exception) AS has_exception
FROM system.replication_queue
WHERE database = currentDatabase() AND (num_tries > 0 OR num_postponed > 0)
ORDER BY create_time, table, replica_name, node_name
LIMIT {row_limit:UInt32}
