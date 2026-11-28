/* culprit */
SELECT hostName() AS node, query_id, initial_query_id, user, current_database,
    elapsed, read_rows, read_bytes, written_rows, written_bytes, memory_usage,
    substring(query, 1, {text_bytes:UInt32}) AS sql_text, length(query) AS sql_bytes
FROM system.processes
WHERE current_database = currentDatabase() AND is_initial_query = 1 AND NOT startsWith(query, '/* culprit */')
ORDER BY elapsed DESC, query_id
LIMIT {row_limit:UInt32}
