/* culprit */
SELECT hostName() AS node, database, name, engine, total_rows, total_bytes, notEmpty(primary_key) AS has_primary_key
FROM system.tables
WHERE database = currentDatabase() AND is_temporary = 0
ORDER BY name
LIMIT {row_limit:UInt32}
