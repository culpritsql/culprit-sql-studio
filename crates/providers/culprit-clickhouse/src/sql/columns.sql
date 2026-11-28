/* culprit */
SELECT hostName() AS node, database, table, name, type, position, default_kind
FROM system.columns
WHERE database = currentDatabase()
ORDER BY table, position
LIMIT {row_limit:UInt32}
