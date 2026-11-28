<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>定位数据库性能瓶颈，并提供有据可查的修复方案。</strong><br>
  面向 macOS、Linux 和 Windows 的数据库性能分析桌面应用与 CLI 工具。
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
  <img alt="MCP server" src="https://img.shields.io/badge/MCP-ready-1b1a18?style=flat-square">
  <img alt="macOS, Linux and Windows" src="https://img.shields.io/badge/macOS%20%C2%B7%20Linux%20%C2%B7%20Windows-1b1a18?style=flat-square">
  <a href="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml/badge.svg"></a>
  <a href="../../LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-ffcf33?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>下载</strong></a> ·
  <a href="#get-started">快速开始</a> ·
  <a href="#what-you-get">核心功能</a> ·
  <a href="#supported-databases">支持的数据库</a> ·
  <a href="#collector-and-culprit-fleet">采集器与 Fleet</a> ·
  <a href="#coding-agents-and-ci">智能体与 CI</a> ·
  <a href="../guide.md">使用指南</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">Fleet 在线演示</a> ·
  <a href="https://explain.culpritsql.com">在线执行计划与 .xel 分析</a>
</p>

<p align="center">
  <sub>
    <a href="../../README.md">English</a> ·
    <strong>简体中文</strong> ·
    <a href="README.ja.md">日本語</a> ·
    <a href="README.ko.md">한국어</a> ·
    <a href="README.ru.md">Русский</a> ·
    <a href="README.de.md">Deutsch</a> ·
    <a href="README.es.md">Español</a> ·
    <a href="README.fr.md">Français</a> ·
    <a href="README.pt-BR.md">Português</a>
  </sub>
</p>

Culprit 将查询统计信息、执行计划、等待事件与表结构上下文整合到同一工作流中。它按影响程度对诊断项进行排序，并将每项结论关联到具体证据、修复建议与验证步骤。可通过桌面应用、终端或编程智能体使用。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="包含时间线、按影响排序的诊断项、高开销查询、等待事件与当前活动会话的数据库概览面板" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## 快速开始

[下载桌面应用或独立 CLI](https://github.com/culpritsql/culprit-sql-studio/releases/latest)。在桌面应用中添加数据库连接或直接打开 SQLite 文件，即可查看数据库概览面板。

在终端中使用：

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

再次打开已有连接时，会先展示上次保存的报告，同时在后台执行新一轮扫描。桌面应用支持签名自动更新；独立 CLI 可通过 `culprit update` 更新。

<details>
<summary>使用演示数据体验界面</summary>

安装 [Bun](https://bun.sh) 后执行：

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

浏览器演示模式使用内置快照数据，无需连接真实数据库。

</details>

<a id="supported-databases"></a>
## 支持的数据库

| 数据库引擎 | 数据源与支持能力 |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store、计划缓存（Plan Cache）、DMV 以及保留的死锁事件 |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`、系统目录与统计信息视图、`EXPLAIN` 以及基于 HypoPG 的虚拟索引 |
| [**TimescaleDB**](../providers/timescale.md) | PostgreSQL 全部数据源，外加超表（Hypertable）大小及受管/压缩分片（Chunk）信息 |
| [**YugabyteDB**](../providers/yugabyte.md) | 原生 YSQL/DocDB 计数器、`DIST` 执行计划、分片（Sharding）与按节点范围采集的活动数据源 |
| [**MySQL**](../providers/mysql.md) | `performance_schema`、`sys` 库、`EXPLAIN` 与慢查询日志 |
| [**MariaDB**](../providers/mariadb.md) | `performance_schema`、慢查询日志、用户统计信息（User Statistics）、MariaDB 执行计划与锁视图 |
| [**ClickHouse**](../providers/clickhouse.md) | 记录的初始查询（`system.query_log`）、MergeTree 数据分片（Part）与合并（Merge）、变更操作（Mutation）、复制状态及 `EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | 只读文件检查、`EXPLAIN QUERY PLAN` 与应用框架查询日志 |

可用功能取决于数据库引擎、已启用的数据源以及登录账号的权限。未就绪的数据源会注明原因并提供配置指引。完整功能对比与已验证版本请参阅[引擎能力对比](../parity.md)。

<a id="what-you-get"></a>
## 核心功能

### 附带证据、修复方案与验证步骤的诊断项

每条诊断项都会标明受影响的查询、统计时间窗口以及支撑严重级别判定的具体证据。展开后可查看修改建议、可直接复制的修复 SQL（如适用）、验证步骤，以及便于分享给团队成员或编程智能体的诊断简报。

| 检查领域 | Culprit 检查项示例 |
|---|---|
| 查询性能 | 高开销连接（Join）、执行计划回退、参数敏感执行计划、导致索引失效的类型转换、N+1 查询模式、执行失败、超时与磁盘溢出（Spill） |
| 索引与表结构 | 缺失索引、未使用索引、重复索引与重叠索引；缺少索引的外键；分区隐患 |
| 维护与容量 | 统计信息过期、死元组（Dead Tuple）、表膨胀、超宽行、序列耗尽与事务 ID 回卷风险 |
| 数据库健康度 | 热点行、表锁升级、存储延迟、页校验和损坏、连接频繁抖动、复制延迟与参数配置 |

例如，SQL Server 演示环境中包含一个产生了 **15 亿次行比较**的执行计划，受影响查询在 5 小时内占用了 **27% 的数据库耗时**。Culprit 识别出其中强制使用嵌套循环连接的提示（Loop Join Hint），建议将其移除，并给出对比新旧执行计划及验证修复效果的具体步骤。

运行 `culprit checks` 可查看完整的检查项列表及所需的数据库能力。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="包含查询证据、相关执行计划算子、修复建议与验证步骤的诊断项详情" src="../images/findings-light.png">
</picture>

### 执行计划与运行时证据

支持以可视化图形、缩进树或算子表格三种视图浏览执行计划。智能洞察会解读高开销查找、基数估算偏差、低效连接、磁盘溢出、类型转换、内存授予与并行度问题，并可直接跳转到对应算子。

支持对比预估行数与实际行数、查看算子属性以及原始 XML 或 JSON，也可对受支持的只读语句显式采集实际执行指标。在 SQL Server 上，无需重新执行查询即可查看保留的最近一次实际执行计划；当数据库提供算子级实时统计时，还可实时观察正在运行查询的算子执行进度。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="按数据库耗时排序的查询列表，右侧展示所选查询的执行计划、智能洞察与算子详情" src="../images/queries-light.png">
</picture>

### 发布前后的变化对比

在数据库耗时趋势图上拖拽框选即可对比两个时间段：识别变慢的查询、新增查询、性能改善的查询以及已停止运行的语句，同时在同一时间轴上对照查看执行计划切换、表结构或参数变更、实例重启与发布标记。

结合本地已登录的 `aws` 或 `az` CLI 凭证，还可叠加 Amazon RDS CPU 积分、I/O 上限以及 Azure 实例规格上限等云监控指标作为辅助上下文。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="展示变慢、新增与改善查询的时间段对比视图，附带数据库耗时与执行次数变化" src="../images/what-changed-light.png">
</picture>

### 对比与分享执行计划

可直接粘贴、拖放或打开来自其他工具的执行计划与事件捕获文件，也可在浏览器中访问 [explain.culpritsql.com](https://explain.culpritsql.com) 直接分析：支持 SQL Server `.sqlplan` 与 `.xel`、PostgreSQL EXPLAIN 文本或 JSON、YugabyteDB DIST 输出、MySQL/MariaDB 执行计划以及 ClickHouse `EXPLAIN PLAN json=1`。通过同步联动的双栏视图对比两个版本的执行计划，高亮展示算子、访问路径、行数估算与代价的变化。

当执行计划保留了捕获的参数、统计信息、并行度与内存信息时，对比视图也会一并解释其中的差异。诊断项与执行计划支持导出分享为 Markdown、独立单文件 HTML 页面或原生执行计划文件，并默认开启字面量脱敏。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="并排对比两个执行计划，展示差异解读并高亮发生变化的算子" src="../images/compare-light.png">
</picture>

### 实时会话、阻塞与死锁

顺着阻塞链追溯到源头持锁会话，排查等待事件、长耗时语句以及应用未及时提交的空闲开启事务。已记录的死锁事件包含参与死锁的语句、死锁模式分析以及后续处理建议。

实时活动视图会自动刷新，也可随时暂停。在非生产环境连接上，可直接通过桌面应用或 CLI 取消正在执行的语句或终止会话。

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="实时会话视图，展示未提交事务及其后方排队等待的被阻塞会话" src="../images/live-light.png">
</picture>

### SQL 编辑器与数据库工具集

| 功能模块 | 包含内容 |
|---|---|
| **索引顾问** | 基于实际工作负载推荐候选索引，识别重复、重叠与未使用的索引，并评估保留这些索引带来的写入开销 |
| **SQL 编辑器** | 持久化标签页、感知表结构的自动补全、已保存查询、预估与实测执行计划，以及带快速修复的内联迁移告警 |
| **迁移脚本检查** | 基于已保存快照的离线检查；支持将受支持的 DDL 自动改写为并发或在线（Online）执行形式 |
| **耗时分布分析** | 将等待类别与具体查询关联，并提供实时采样以便与历史记录的工作负载对比 |
| **健康信号** | 内存、编译次数、连接数、行锁、单文件级存储延迟、复制状态以及云实例规格上限 |
| **结构浏览器** | 表大小、索引及其使用情况；跨环境对比列、索引、外键、触发器、约束与分区边界，并生成用于消除差异的同步 SQL |
| **连接管理** | 支持连接字符串，并可从 Azure Data Studio、VS Code、DBeaver 和 DataGrip 导入已有连接 |
| **云资源发现与 SSH** | 利用本地 CLI 登录态自动发现 Azure 与 AWS 数据库；支持通过 [SSH 跳板机](../ssh.md)连接 |
| **离线报告** | 已保存的扫描记录、历史趋势、跨期对比与共享快照，无需连接数据库即可离线查阅 |
| **归档** | 基于已发布的 Azure Data Explorer (ADX) 与 ClickHouse 遥测数据仓库，以只读方式调查故障事件、时间窗口与执行计划 |
| **键盘导航** | 命令面板（⌘K / Ctrl+K）、视图切换快捷键、查询标签页与快捷操作 |
| **多语言支持** | 英语（English）、简体中文、日语（日本語）、韩语（한국어）、俄语（Русский）、德语（Deutsch）、西班牙语（Español）、法语（Français）与葡萄牙语（Português） |

<a id="collector-and-culprit-fleet"></a>
## 采集器与 Culprit Fleet

将 `culprit collect` 作为资源受限的只读后台采集器持续运行，即可在不让交互式用户或编程智能体直连生产库的前提下持续监控数据库。采集器会将封装好的采集包写入持久化本地发件箱（Outbox），将原始证据备份至私有的 **Azure Blob Storage** 或 **Amazon S3**，在本地运行 Culprit 纯函数诊断引擎，并将故障事件（`CulpritIncidentsV1`）、诊断项（`CulpritFindingsV1`）、表结构/参数变更（`CulpritChangesV1`）以及 13 张原始证据表发布到 **Azure Data Explorer (ADX)** 或 **ClickHouse**。

### 1. 本地采集或生成数据仓库 DDL

```sh
# 将有界快照采集到本地发件箱（无需配置数据仓库）
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# 或直接通过容器镜像生成数据仓库表与辅助视图的 DDL
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. 在 Kubernetes 上部署安全加固的采集器分片

`ghcr.io/culpritsql/culprit-collector` 提供多架构容器镜像（`linux/amd64` 与 `linux/arm64`），并配套提供 OCI Helm Chart：默认副本数为 0，强制采用单写入者 `StatefulSet` 归属模型（`65532:65532`，只读根文件系统），并将源数据库、恢复存储与数据仓库凭证相互隔离：

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. 在 Culprit Fleet、SQL Studio 或 CLI 中排查分析

直接基于 ADX 或 ClickHouse 探索全集群范围的故障事件、时间窗口、查询时间线、执行计划与配置漂移——全程无需直连生产数据库：

- **Web 控制台：** **[fleet.culpritsql.com](https://fleet.culpritsql.com)**（或自行托管 **[culprit-fleet](https://github.com/culpritsql/culprit-fleet)**）
- **桌面应用：** 在 Culprit SQL Studio 侧边栏中打开**归档**（Archives）
- **CLI 与 MCP：** `culprit resources`、`culprit window`、`culprit incident`、`culprit collect queries`（生成 Grafana 仪表盘与告警规则）以及 `culprit mcp --warehouse-evidence`

[采集器与数据仓库架构 →](../collector.md) · [Kubernetes Helm Chart →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## 编程智能体与 CI

### MCP 与代码仓库配置

MCP 服务让 Claude Code、Cursor、Codex 等 MCP 客户端能够直接访问诊断项、执行计划、表结构、迁移检查、阻塞链、等待事件、索引建议与验证工具。智能体仅使用已保存的连接名称，数据库凭证始终保留在 Culprit 本地。

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

第二条命令会向代码仓库写入智能体使用说明与 MCP 配置文件；其他客户端也可以直接注册带 `mcp` 参数的 `culprit` 命令。当 SQL 注释中包含 `file:line` 源码位置信息（包括 Marginalia 与 SqlCommenter 注解）时，诊断简报可直接定位回应用代码的具体行号。

输出某条诊断项的简报，或通过 HypoPG 在不实际创建索引的情况下测试候选 PostgreSQL 索引：

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

智能体访问权限按连接独立控制，支持三档策略：关闭、仅允许读取统计信息与预估执行计划（默认），或允许执行经审批的只读查询。查询审批界面会展示完整的待执行语句，所有工具调用均记录在本地审计日志中。

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="智能体查询审批界面，展示待执行的完整 SQL、目标连接、行数与超时限制，以及拒绝或单次执行按钮" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[智能体配置、权限与审计详情 →](../agents.md)

### 迁移脚本检查与发布工作流

结合已保存快照中的表大小、列类型、索引及历史工作负载，对数据库迁移脚本进行检查。检查范围涵盖会引发锁表阻塞的 `CREATE INDEX` 与 `ALTER TABLE` 操作、缺失或类型不匹配的外键索引，以及会影响已知查询的 `DROP` 或 `RENAME` 变更。

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

使用 `--fail-on high` 可在发现高严重级别问题时令 CI 流水线失败退出；`--write` 可将受支持的重写规则直接应用回迁移文件。JSON 格式便于脚本处理，SARIF 格式可对接代码审查工具，稳定的诊断项 ID 则便于在多次扫描之间持续追踪同一问题。

发布完成后，可在时间线上打标、验证诊断项是否修复，或生成脱敏摘要：

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## 生产环境安全保障

- **扫描仅读取统计信息与系统目录视图**，并内置针对各数据库引擎的超时控制与执行计划采集保护机制。
- **数据库修复仅以建议形式提供**，由人工审阅并决定是否执行。
- **生产连接执行受确认机制保护**：对于 Culprit 无法归类为纯只读的语句需手动确认；破坏性操作需额外确认。
- **智能体查询权限需显式开启（Opt-in）**，每条只读语句均需单独审批，并保留完整的本地审计记录。
- **保存的密码存储于操作系统钥匙串（Keychain）**。Entra ID 可直接复用本地 Azure 登录态；导出的分享内容默认对字面量进行脱敏。

建议按工作流实际所需配置最小权限的数据库登录账号。采集、执行计划评估与语句执行的安全边界详见[安全指南](../guide.md#safety)与[数据库支持文档](../parity.md)。

## 开发构建

Culprit 基于 Rust、Tauri 与 React 构建。桌面应用、CLI 与 MCP 服务共享同一套分析引擎。无需 Fork 仓库即可通过 Rust API 扩展自定义检查项或数据库支持；面向用户的界面与报告文案统一由[语言目录](../localization.md)提供。

<details>
<summary>从源码构建</summary>

请先安装 [Rust stable](https://rustup.rs) 与 [Bun](https://bun.sh)。Linux 环境还需安装[平台依赖项](../guide.md#platforms)。

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# 桌面应用
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## 延伸阅读

- **[使用指南](../guide.md)：** 全部检查项与 CLI 命令说明、智能体配置、实测速度、安全细节及扩展开发方式
- **[采集器与数据仓库](../collector.md)：** 后台持续采集、ADX 与 ClickHouse 表结构、Azure Blob/S3 备份恢复以及 [Culprit Fleet](https://fleet.culpritsql.com)
- **[架构设计](../ARCHITECTURE.md)：** 分析流水线、所有检查项共同遵循的证据准则以及 Rust Crate 模块地图
- **[产品设计](../DESIGN.md)：** 产品行为方式及其设计原因
- **[编程智能体](../agents.md)：** MCP 工具集、权限分级、审批机制与审计日志
- **[引擎能力对比](../parity.md)：** 跨引擎功能矩阵、各数据库配置指南与已验证版本

Culprit 基于 [MIT 许可证](../../LICENSE)开源。

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>为每一个在数据库变慢时被告警呼叫的人打造。</sub>
</p>
