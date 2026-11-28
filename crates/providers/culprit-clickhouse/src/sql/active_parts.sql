/* culprit */
SELECT hostName() AS node, database, table, partition_id, count() AS active_parts,
    sum(rows) AS total_rows, sum(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE database = currentDatabase() AND active
GROUP BY database, table, partition_id
ORDER BY active_parts DESC, table, partition_id
LIMIT {row_limit:UInt32}
