# Simplified Chinese (zh-Hans) glossary

Read this before changing `locales/zh-Hans.json`.

## Voice and form

- Concise, technical Mainland Chinese DBA terminology (`您` only in polite guidance sentences; buttons and headings omit pronouns: `保存`, `测试连接`, `重新扫描`).
- Keep engine/product names (`Culprit`, `SQL Server`, `PostgreSQL`, `TimescaleDB`, `YugabyteDB`, `MySQL`, `MariaDB`, `ClickHouse`, `SQLite`, `Azure SQL`, `ADX`), SQL keywords, wait types, system view names and plan operator names in English.
- Plural format (`plural: "zh"`): always keep `{n, plural, other {...}}` (and exact `=0 {...}` when English has `=0`) so argument types match English.

## Terms

| English | Simplified Chinese |
|---|---|
| finding | 诊断项 / 发现 |
| check | 检查项 |
| scan / re-scan | 扫描 / 重新扫描 |
| query / statement | 查询 / 语句 |
| execution plan | 执行计划 |
| wait / lock / deadlock | 等待 / 锁 / 死锁 |
| index / primary key / foreign key | 索引 / 主键 / 外键 |
| archive / warehouse / collector | 归档 / 数据仓库 / 采集器 |
