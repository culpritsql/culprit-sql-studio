/* culprit */
SELECT hostName() AS node, version() AS version, name AS database, toString(uuid) AS database_uuid,
    toString(now64(6, 'UTC'), 'UTC') AS observed_at, timezone() AS timezone
FROM system.databases WHERE name = currentDatabase()
