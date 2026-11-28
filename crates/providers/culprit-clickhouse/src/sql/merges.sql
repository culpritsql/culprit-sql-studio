/* culprit */
SELECT hostName() AS node, database, table, partition_id, result_part_name, elapsed, progress, num_parts,
    is_mutation, rows_read, rows_written, memory_usage
FROM system.merges
WHERE database = currentDatabase()
ORDER BY elapsed DESC, table, result_part_name
LIMIT {row_limit:UInt32}
