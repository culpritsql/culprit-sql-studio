# Japanese (ja) glossary

Read this before changing `locales/ja.json`.

## Voice and form

- Polite, concise engineering Japanese (`です・ます` in full sentences; concise noun phrases for buttons and labels: `保存`, `接続テスト`, `再スキャン`).
- Keep engine/product names (`Culprit`, `SQL Server`, `PostgreSQL`, `TimescaleDB`, `YugabyteDB`, `MySQL`, `MariaDB`, `ClickHouse`, `SQLite`, `Azure SQL`, `ADX`), SQL keywords, wait types, system view names and plan operator names in English.
- Plural format (`plural: "ja"`): always keep `{n, plural, other {...}}` (and exact `=0 {...}` when English has `=0`) so argument types match English.

## Terms

| English | Japanese |
|---|---|
| finding | 検出項目 |
| check | チェック |
| scan / re-scan | スキャン / 再スキャン |
| query / statement | クエリ / ステートメント |
| execution plan | 実行プラン |
| wait / lock / deadlock | 待機 / ロック / デッドロック |
| index / primary key / foreign key | インデックス / 主キー / 外部キー |
| archive / warehouse / collector | アーカイブ / ウェアハウス / コレクター |
