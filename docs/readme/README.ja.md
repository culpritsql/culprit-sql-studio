<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>データベースのボトルネックを、修正のための根拠とともに特定します。</strong><br>
  macOS・Linux・Windows 対応のデータベースパフォーマンス分析デスクトップアプリおよび CLI。
</p>

<p align="center">
  <img alt="SQL Server" src="https://img.shields.io/badge/SQL%20Server-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoColor=white">
  <img alt="TimescaleDB" src="https://img.shields.io/badge/TimescaleDB-F4CE41?style=flat-square">
  <img alt="YugabyteDB" src="https://img.shields.io/badge/YugabyteDB-C94B25?style=flat-square">
  <img alt="MySQL" src="https://img.shields.io/badge/MySQL-00758F?style=flat-square&logo=mysql&logoColor=white">
  <img alt="MariaDB" src="https://img.shields.io/badge/MariaDB-003545?style=flat-square&logo=mariadb&logoColor=white">
  <img alt="ClickHouse" src="https://img.shields.io/badge/ClickHouse-FFCC01?style=flat-square&logo=clickhouse&logoColor=black">
  <img alt="SQLite" src="https://img.shields.io/badge/SQLite-003B57?style=flat-square&logo=sqlite&logoColor=white">
  <br>
  <img alt="MCP サーバー" src="https://img.shields.io/badge/MCP-ready-1b1a18?style=flat-square">
  <img alt="macOS、Linux、Windows" src="https://img.shields.io/badge/macOS%20%C2%B7%20Linux%20%C2%B7%20Windows-1b1a18?style=flat-square">
  <a href="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml/badge.svg"></a>
  <a href="../../LICENSE"><img alt="MIT ライセンス" src="https://img.shields.io/badge/license-MIT-ffcf33?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>ダウンロード</strong></a> ·
  <a href="#get-started">クイックスタート</a> ·
  <a href="#what-you-get">主な機能</a> ·
  <a href="#supported-databases">プロバイダー</a> ·
  <a href="#collector-and-culprit-fleet">コレクター &amp; Fleet</a> ·
  <a href="#coding-agents-and-ci">エージェント &amp; CI</a> ·
  <a href="../guide.md">ガイド</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">ライブ Fleet デモ</a> ·
  <a href="https://explain.culpritsql.com">実行計画 &amp; .xel 解析</a>
</p>

<p align="center">
  <sub>
    <a href="../../README.md">English</a> ·
    <a href="README.zh-Hans.md">简体中文</a> ·
    <strong>日本語</strong> ·
    <a href="README.ko.md">한국어</a> ·
    <a href="README.ru.md">Русский</a> ·
    <a href="README.de.md">Deutsch</a> ·
    <a href="README.es.md">Español</a> ·
    <a href="README.fr.md">Français</a> ·
    <a href="README.pt-BR.md">Português</a>
  </sub>
</p>

Culprit は、クエリ統計、実行プラン、待機、スキーマコンテキストを 1 つのワークフローに統合します。検出項目を影響度順にランク付けし、それぞれに根拠、推奨される修正案、検証手順を紐付けます。デスクトップアプリ、ターミナル、またはコーディングエージェントから利用できます。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="タイムライン、ランク付けされた検出項目、高負荷クエリ、待機、現在のアクティビティを表示するデータベースダッシュボード" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## はじめに

[デスクトップアプリまたはスタンドアロン CLI をダウンロード](https://github.com/culpritsql/culprit-sql-studio/releases/latest)します。アプリで接続を追加するか、SQLite ファイルを開くとダッシュボードが表示されます。

ターミナルから実行する場合:

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

接続を再度開くと、バックグラウンドで最新のスキャンを実行しながら保存済みのレポートを表示します。デスクトップアプリは署名付きアップデートに対応しており、スタンドアロン CLI は `culprit update` で更新できます。

<details>
<summary>デモデータでインターフェースを試す</summary>

[Bun](https://bun.sh) がインストールされている環境で以下を実行します:

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

ブラウザデモは同梱のスナップショットを使用するため、データベース接続は不要です。

</details>

<a id="supported-databases"></a>
## 対応データベース

| プロバイダー | ソースと対応機能 |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store、プランキャッシュ、DMV、保持されたデッドロックイベント |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`、カタログ／統計ビュー、EXPLAIN、HypoPG による仮想インデックス |
| [**TimescaleDB**](../providers/timescale.md) | PostgreSQL のソースに加え、ハイパーテーブルのサイズおよび管理／圧縮チャンク |
| [**YugabyteDB**](../providers/yugabyte.md) | ネイティブの YSQL/DocDB カウンター、DIST プラン、シャーディング、スコープ付きアクティビティソース |
| [**MySQL**](../providers/mysql.md) | `performance_schema`、`sys`、EXPLAIN、スロークエリログ |
| [**MariaDB**](../providers/mariadb.md) | Performance Schema、スローログ、ユーザー統計、MariaDB プラン、ロックビュー |
| [**ClickHouse**](../providers/clickhouse.md) | 記録された初期クエリ（`system.query_log`）、MergeTree のパート／マージ、ミューテーション、レプリケーション、`EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | 読み取り専用のファイル検査、`EXPLAIN QUERY PLAN`、フレームワークのクエリログ |

利用できる機能は、エンジン、有効化されているソース、ログインユーザーの権限によって異なります。利用できないソースには、その理由とセットアップ手順が表示されます。機能比較および検証済みバージョンについては、[エンジン別の機能比較](../parity.md)を参照してください。

<a id="what-you-get"></a>
## 主な機能

### 根拠・修正案・検証手順を備えた検出項目

各検出項目には、影響を受けるクエリ、計測期間、深刻度の根拠が明示されます。詳細を開くと、推奨される変更内容、コピーして使える SQL（該当する場合）、検証手順、チームメンバーやコーディングエージェントに共有できるブリーフを確認できます。

| 領域 | チェック対象の例 |
|---|---|
| クエリパフォーマンス | 高コストな結合、プラン退行、パラメータ依存プラン、インデックス利用を妨げる型変換、N+1 パターン、失敗、タイムアウト、スピル |
| インデックスとスキーマ | 不足・未使用・重複・包含関係にあるインデックス、インデックスのない外部キー、パーティション上のリスク |
| メンテナンスとキャパシティ | 古い統計情報、デッドタプル、テーブル肥大化、巨大な行、シーケンス枯渇、トランザクション ID 周回リスク |
| データベース健全性 | ホット行、テーブルロックエスカレーション、ストレージレイテンシ、ページチェックサム破損、接続チャーン、レプリケーション遅延、各種設定 |

たとえば SQL Server のデモには、**15 億回の行比較**を行う実行プランが含まれています。該当クエリは 5 時間で**データベース時間の 27%** を占めています。Culprit は強制されたループ結合ヒントを特定し、その削除を提案したうえで、新しい実行プランの比較と結果の検証手順を提示します。

チェック項目の一覧と必要な機能要件は `culprit checks` で確認できます。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="クエリの根拠、関連するプランステップ、推奨される修正案、検証手順を表示する検出項目" src="../images/findings-light.png">
</picture>

### 実行プランと実行時の根拠

実行プランをダイアグラム、インデント付きツリー、またはオペレーション一覧表として確認できます。インサイト機能により、高コストなルックアップ、行数見積もり誤差、非効率な結合、スピル、型変換、メモリグラント、並列処理の課題が解説され、該当するオペレーターへ直接移動できます。

推定行数と実測行数、オペレーターのプロパティ、生の XML や JSON を検査できます。対応する読み取りステートメントでは明示的な実測も可能です。SQL Server では、クエリを再実行せずに保持されている直近の実測プランを検査したり、情報が取得可能な場合には実行中クエリのオペレーター進捗を追跡したりできます。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="データベース時間順に並んだクエリ一覧と、選択したクエリの実行プラン・インサイト・オペレーター詳細" src="../images/queries-light.png">
</picture>

### デプロイ後の変化を比較

データベース時間チャートをドラッグして 2 つの期間を比較できます。遅くなったクエリ、新規クエリ、改善されたクエリ、実行されなくなったステートメントを一覧でき、プランの切り替わり、スキーマや設定の変更、再起動、デプロイマークも同じタイムライン上で確認できます。

既存の `aws` または `az` ログインを利用して、Amazon RDS の CPU クレジットや I/O 上限、Azure のティア上限などのクラウドメトリクスもコンテキストとして重ね合わせられます。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="データベース時間と実行回数の変化とともに、遅くなったクエリ・新規クエリ・改善されたクエリを表示する期間比較" src="../images/what-changed-light.png">
</picture>

### 実行プランの比較と共有

他のツールから取得した実行プランやイベントキャプチャを貼り付け・ドロップ・ファイルから開いて確認できるほか、ブラウザ上の [explain.culpritsql.com](https://explain.culpritsql.com) でも検査できます。SQL Server の `.sqlplan` と `.xel`、PostgreSQL の EXPLAIN（テキスト／JSON）、YugabyteDB の DIST 出力、MySQL／MariaDB のプラン、ClickHouse の `EXPLAIN PLAN json=1` に対応しています。2 つのバージョンを同期ダイアグラムで並べて比較し、オペレーター、アクセスパス、行数見積もり、コストの変化をハイライト表示します。

プランに情報が保持されている場合は、取得時のパラメータ、統計情報、並列度、メモリの差異も解説します。検出項目や実行プランは Markdown、単体で完結する HTML ページ、またはネイティブのプランファイルとして共有でき、デフォルトでリテラル値のマスクが有効になっています。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="2 つの実行プランを並べて差分の解説と変更されたオペレーターをハイライト表示した画面" src="../images/compare-light.png">
</picture>

### ライブセッション・ブロッキング・デッドロック

ブロッキングチェーンをたどり、後続を待たせている原因セッションを特定できます。待機、長時間実行中のステートメント、アプリケーションが開いたままにしているトランザクションを調査できます。記録されたデッドロックには、関与したステートメント、発生パターン、推奨される次の対処手順が含まれます。

ライブアクティビティは自動更新され、一時停止も可能です。非本番環境の接続では、アプリまたは CLI からステートメントのキャンセルやセッションの終了を行えます。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="開いたままのトランザクションと、その後ろで待機しているブロックされたセッションを表示するライブセッション画面" src="../images/live-light.png">
</picture>

### SQL エディタとデータベースツール群

| 機能 | 内容 |
|---|---|
| **インデックスアドバイザー** | ワークロードに基づくインデックス候補、重複・包含・未使用インデックスの検出、維持にかかる書き込みコストの評価 |
| **SQL エディタ** | 永続タブ、スキーマ対応の補完、保存済みクエリ、推定プラン・実測プランの表示、クイックフィックス付きのインラインマイグレーション警告 |
| **マイグレーションリンター** | 保存済みスナップショットに対するオフラインチェック、並行／オンライン実行に対応した DDL 書き換え |
| **時間の内訳** | クエリに紐づく待機カテゴリと、記録済みワークロードと比較できるライブサンプル |
| **ヘルスシグナル** | メモリ、コンパイル、接続数、行ロック、ファイル別のストレージレイテンシ、レプリケーション、クラウド上限 |
| **スキーマエクスプローラー** | テーブルサイズ、インデックスと使用状況の確認、環境間でのカラム・インデックス・外部キー・トリガー・制約・パーティション境界の比較と差分解消 SQL の生成 |
| **接続管理** | 接続文字列の入力に加え、Azure Data Studio、VS Code、DBeaver、DataGrip からのインポートに対応 |
| **ディスカバリと SSH** | CLI のログイン情報を使って Azure や AWS のデータベースを検出、[SSH 踏み台ホスト](../ssh.md)経由での接続 |
| **オフラインレポート** | データベース接続なしで検査できる保存済みスキャン、トレンド、履歴比較、共有スナップショット |
| **アーカイブ** | 公開済みの Azure Data Explorer (ADX) および ClickHouse テレメトリウェアハウスに対する、読み取り専用のインシデント・時間範囲・実行プラン調査 |
| **キーボード操作** | コマンドパレット（⌘K / Ctrl+K）、ビュー切り替えショートカット、クエリタブと各種アクション操作 |
| **対応言語** | 英語（English）、簡体字中国語（简体中文）、日本語、韓国語（한국어）、ロシア語（Русский）、ドイツ語（Deutsch）、スペイン語（Español）、フランス語（Français）、ポルトガル語（Português） |

<a id="collector-and-culprit-fleet"></a>
## コレクターと Culprit Fleet

負荷上限付きの読み取り専用バックグラウンドコレクターとして `culprit collect` を実行すると、対話ユーザーやエージェントを本番環境へ直接接続させることなくデータベースを継続的に監視できます。コレクターは封印されたキャプチャを永続的なローカル outbox に書き込み、生の証跡データをプライベートの **Azure Blob Storage** または **Amazon S3** にバックアップし、Culprit の純粋関数型診断エンジンをローカルで実行したうえで、インシデント（`CulpritIncidentsV1`）、検出項目（`CulpritFindingsV1`）、スキーマ／設定変更（`CulpritChangesV1`）、および 13 個の生証跡テーブルを **Azure Data Explorer (ADX)** または **ClickHouse** に公開します。

### 1. ローカルでのキャプチャ取得またはウェアハウス DDL の生成

```sh
# 負荷上限付きのスナップショットをローカルの outbox に取得（ウェアハウス不要）
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# またはコンテナイメージから直接ウェアハウスのテーブルおよびヘルパービューの DDL を生成
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. 堅牢化されたコレクターシャードを Kubernetes にデプロイ

マルチアーキテクチャイメージ（`linux/amd64` および `linux/arm64`）が `ghcr.io/culpritsql/culprit-collector` で提供されており、OCI Helm チャートと組み合わせて利用できます。このチャートはデフォルトでレプリカ数 0 に設定され、単一ライターの `StatefulSet` 所有権（`65532:65532`、読み取り専用ルートファイルシステム）を強制し、ソース・リカバリ・ウェアハウスの認証情報を分離します:

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. Culprit Fleet・Culprit SQL Studio・CLI で調査

本番データベースへのライブ接続を一切行うことなく、ADX や ClickHouse から直接フリート全体のインシデント、時間範囲、クエリタイムライン、実行プラン、設定ドリフトを調査できます:

- **Web コンソール:** **[fleet.culpritsql.com](https://fleet.culpritsql.com)**（または `culprit-archive-web` をセルフホスト）
- **デスクトップアプリ:** Culprit SQL Studio のサイドバーから **アーカイブ** を開く
- **CLI &amp; MCP:** `culprit resources`、`culprit window`、`culprit incident`、`culprit collect queries`（Grafana ダッシュボード &amp; アラートルール）、`culprit mcp --warehouse-evidence`

[コレクター &amp; ウェアハウスのアーキテクチャ →](../collector.md) · [Kubernetes Helm チャート →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## コーディングエージェントと CI

### MCP とリポジトリ設定

MCP サーバーを通じて、Claude Code、Cursor、Codex などの MCP クライアントから検出項目、実行プラン、スキーマ、マイグレーション、ブロッキング、待機、インデックス提案、検証機能にアクセスできます。エージェントは保存済みの接続名を使用し、認証情報は Culprit 内に保持されます。

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

2 つ目のコマンドはリポジトリ用の指示ファイルと MCP 設定を追加します。その他のクライアントでは、引数 `mcp` を指定して `culprit` を直接登録できます。クエリコメントに Marginalia や SqlCommenter 形式を含む `file:line` 参照が含まれている場合、検出項目のブリーフからアプリケーションコードの該当箇所を辿ることができます。

検出項目のブリーフを表示したり、実際のインデックスを作成せずに HypoPG 経由で PostgreSQL のインデックス案を検証したりできます:

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

アクセスレベルは接続ごとに設定できます: 無効、統計情報と推定プランのみ（デフォルト）、または承認済みの読み取り専用クエリ。クエリ承認画面には実行される正確なステートメントが表示され、ツール呼び出しはローカルの監査ログに記録されます。

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="正確な SQL、接続先、行数・時間の上限、および拒否または 1 回実行の操作を表示するエージェントクエリ承認画面" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[エージェントの設定・権限・監査の詳細 →](../agents.md)

### マイグレーションチェックとデプロイワークフロー

保存済みスナップショットのテーブルサイズ、カラム型、インデックス、記録済みワークロードに対してマイグレーションファイルを検証できます。ブロッキングが発生する `CREATE INDEX` や `ALTER TABLE` 操作、外部キーインデックスの不足や不一致、記録済みクエリに影響する `DROP` や `RENAME` の変更を検出します。

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

`--fail-on high` を指定すると、深刻度が高い検出項目がある場合に CI を失敗させられます。`--write` は対応する書き換えをマイグレーションファイルに適用します。スクリプト連携用の JSON 出力やコードレビューツール向けの SARIF 出力に対応しており、安定した検出項目 ID によって複数回のスキャンにまたがって同じ問題を追跡できます。

デプロイ後には、タイムラインへのマーク追加、検出項目の検証、マスク済みサマリーの生成を行えます:

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## 本番環境での利用

- **スキャンは統計情報とカタログビューのみを読み取り**、プロバイダーごとのタイムアウトとプランニング保護機構を備えています。
- **データベースへの修正は提案として提示**され、ユーザー自身が内容を確認して適用します。
- **本番環境での実行には確認が必要**です。Culprit が読み取り専用と判定できないステートメントには確認を求め、破壊的な操作には追加の確認を要求します。
- **エージェントのクエリ実行はオプトイン方式**であり、読み取り専用ステートメントごとの承認とローカル監査証跡を備えています。
- **保存されたパスワードには OS のキーチェーンを使用**します。Entra ID は Azure のサインイン情報を利用でき、共有用エクスポートではデフォルトでリテラル値がマスクされます。

ワークフローに必要な権限のみを持つデータベースログインを使用してください。収集・プラン取得・実行の境界については、[安全ガイド](../guide.md#safety)および[プロバイダードキュメント](../parity.md)を参照してください。

## 開発

Culprit は Rust、Tauri、React で構築されています。アプリ、CLI、MCP サーバーは単一の分析エンジンを共有しています。フォークすることなく Rust API からカスタムチェックやデータベースプロバイダーを追加でき、ユーザー向けのテキストは[言語カタログ](../localization.md)から提供されます。

<details>
<summary>ソースからビルド</summary>

[Rust stable](https://rustup.rs) と [Bun](https://bun.sh) をインストールします。Linux では[プラットフォーム依存パッケージ](../guide.md#platforms)も必要です。

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# デスクトップアプリ
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## 詳細ドキュメント

- **[ガイド](../guide.md):** すべてのチェック項目とコマンド、エージェント設定、実測速度、安全性の詳細、Culprit の拡張方法
- **[コレクターとウェアハウス](../collector.md):** バックグラウンド収集、ADX・ClickHouse スキーマ、Azure Blob／S3 リカバリ、[Culprit Fleet](https://fleet.culpritsql.com)
- **[アーキテクチャ](../ARCHITECTURE.md):** 分析パイプライン、すべてのチェックが従う根拠ルール、クレート構成
- **[設計](../DESIGN.md):** プロダクトの振る舞いとその理由
- **[コーディングエージェント](../agents.md):** MCP ツール、権限、承認フロー、監査ログ
- **[エンジン別の機能比較](../parity.md):** 機能比較、プロバイダーガイド、セットアップ手順、検証済みバージョン

Culprit は [MIT ライセンス](../../LICENSE)のもとで公開されているオープンソースソフトウェアです。

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>データベースが遅くなって呼び出される人たちのために作られました。</sub>
</p>
