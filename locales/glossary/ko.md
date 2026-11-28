# Korean (ko) glossary

Read this before changing `locales/ko.json`.

## Voice and form

- Polite, clear engineering Korean (`해요` / `합니다` — use concise formal `합니다`/`하세요` in full guidance sentences; noun phrases for buttons and labels: `저장`, `연결 테스트`, `다시 스캔`).
- Keep engine/product names (`Culprit`, `SQL Server`, `PostgreSQL`, `TimescaleDB`, `YugabyteDB`, `MySQL`, `MariaDB`, `ClickHouse`, `SQLite`, `Azure SQL`, `ADX`), SQL keywords, wait types, system view names and plan operator names in English.
- Plural format (`plural: "ko"`): always keep `{n, plural, other {...}}` (and exact `=0 {...}` when English has `=0`) so argument types match English.

## Terms

| English | Korean |
|---|---|
| finding | 진단 항목 |
| check | 검사 |
| scan / re-scan | 스캔 / 다시 스캔 |
| query / statement | 쿼리 / 구문 |
| execution plan | 실행 계획 |
| wait / lock / deadlock | 대기 / 잠금 / 교착 상태 |
| index / primary key / foreign key | 인덱스 / 기본 키 / 외래 키 |
| archive / warehouse / collector | 아카이브 / 웨어하우스 / 수집기 |
