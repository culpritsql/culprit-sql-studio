<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>해결에 필요한 근거와 함께 데이터베이스 병목을 찾아내세요.</strong><br>
  macOS, Linux, Windows용 데이터베이스 성능 분석 데스크톱 앱 및 CLI입니다.
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
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>다운로드</strong></a> ·
  <a href="#get-started">빠른 시작</a> ·
  <a href="#what-you-get">주요 기능</a> ·
  <a href="#supported-databases">지원 데이터베이스</a> ·
  <a href="#collector-and-culprit-fleet">수집기 및 Fleet</a> ·
  <a href="#coding-agents-and-ci">에이전트 및 CI</a> ·
  <a href="../guide.md">가이드</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">라이브 Fleet 데모</a> ·
  <a href="https://explain.culpritsql.com">실행 계획 &amp; .xel 분석기</a>
</p>

<p align="center">
  <sub>
    <a href="../../README.md">English</a> ·
    <a href="README.zh-Hans.md">简体中文</a> ·
    <a href="README.ja.md">日本語</a> ·
    <strong>한국어</strong> ·
    <a href="README.ru.md">Русский</a> ·
    <a href="README.de.md">Deutsch</a> ·
    <a href="README.es.md">Español</a> ·
    <a href="README.fr.md">Français</a> ·
    <a href="README.pt-BR.md">Português</a>
  </sub>
</p>

Culprit은 쿼리 통계, 실행 계획, 대기 및 스키마 컨텍스트를 하나의 워크플로로 통합합니다. 영향도에 따라 진단 결과의 우선순위를 매기고, 각 항목을 근거, 권장 해결책 및 검증 단계와 연결합니다. 데스크톱 앱, 터미널 또는 코딩 에이전트에서 바로 사용할 수 있습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="타임라인, 우선순위별 진단 결과, 고비용 쿼리, 대기 및 현재 활성 세션을 보여주는 데이터베이스 대시보드" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## 시작하기

[데스크톱 앱 또는 독립 실행형 CLI를 다운로드하세요](https://github.com/culpritsql/culprit-sql-studio/releases/latest). 앱에서 연결을 추가하거나 SQLite 파일을 열면 대시보드를 바로 확인할 수 있습니다.

터미널에서 실행:

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

연결을 다시 열면 백그라운드에서 새 스캔이 실행되는 동안 저장된 리포트를 먼저 보여줍니다. 데스크톱 앱은 서명된 업데이트를 제공하며, 독립 실행형 CLI는 `culprit update`로 업데이트할 수 있습니다.

<details>
<summary>데모 데이터로 인터페이스 체험하기</summary>

[Bun](https://bun.sh)이 설치된 환경에서 다음 명령을 실행하세요:

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

브라우저 데모는 포함된 스냅샷을 사용하므로 별도의 데이터베이스 연결이 필요하지 않습니다.

</details>

<a id="supported-databases"></a>
## 지원 데이터베이스

| 프로바이더 | 소스 및 지원 기능 |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store, 플랜 캐시, DMV 및 보존된 교착 상태 이벤트 |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`, 카탈로그/통계 뷰, EXPLAIN 및 HypoPG 기반 가상 인덱스 |
| [**TimescaleDB**](../providers/timescale.md) | PostgreSQL 소스에 더해 하이퍼테이블 크기 및 관리형/압축 청크 정보 |
| [**YugabyteDB**](../providers/yugabyte.md) | 네이티브 YSQL/DocDB 카운터, DIST 실행 계획, 샤딩 및 범위 지정 활성 세션 소스 |
| [**MySQL**](../providers/mysql.md) | `performance_schema`, `sys`, EXPLAIN 및 슬로우 쿼리 로그 |
| [**MariaDB**](../providers/mariadb.md) | Performance Schema, 슬로우 로그, 사용자 통계, MariaDB 실행 계획 및 잠금 뷰 |
| [**ClickHouse**](../providers/clickhouse.md) | 기록된 초기 쿼리(`system.query_log`), MergeTree 파트/병합, 뮤테이션, 복제 및 `EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | 읽기 전용 파일 검사, `EXPLAIN QUERY PLAN` 및 프레임워크 쿼리 로그 |

제공되는 기능은 데이터베이스 엔진, 활성화된 소스 및 로그인 계정의 권한에 따라 달라집니다. 누락된 소스는 원인과 설정 안내가 함께 표시됩니다. 엔진별 기능 비교와 검증된 버전은 [엔진 기능 비교](../parity.md)를 참고하세요.

<a id="what-you-get"></a>
## 주요 기능

### 근거, 해결책 및 검증 단계가 포함된 진단 결과

각 진단 항목은 영향받는 쿼리, 측정 시간 범위 및 심각도 판단의 근거를 제시합니다. 항목을 열면 권장 변경 사항, 바로 복사할 수 있는 SQL(해당하는 경우), 검증 단계, 그리고 팀원이나 코딩 에이전트와 공유할 수 있는 요약 브리프를 확인할 수 있습니다.

| 영역 | Culprit 점검 항목 예시 |
|---|---|
| 쿼리 성능 | 고비용 조인, 실행 계획 퇴행, 파라미터 민감 실행 계획, 인덱스를 가로막는 타입 변환, N+1 패턴, 실패, 타임아웃 및 스필(spill) |
| 인덱스 및 스키마 | 누락·미사용·중복·중첩 인덱스, 인덱스가 없는 외래 키, 파티션 위험 요소 |
| 유지보수 및 용량 | 오래된 통계, 데드 튜플(dead tuple), 테이블 블로트(bloat), 대형 로우, 시퀀스 고갈 및 트랜잭션 ID 랩어라운드 위험 |
| 데이터베이스 상태 | 핫 로우(hot row), 테이블 잠금 에스컬레이션, 스토리지 지연 시간, 페이지 체크섬 손상, 연결 급증·반복(churn), 복제 지연 및 설정 |

예를 들어, SQL Server 데모에는 **15억 건의 로우 비교**를 수행하는 실행 계획이 포함되어 있습니다. 해당 쿼리는 5시간 동안 **전체 데이터베이스 시간의 27%**를 차지합니다. Culprit은 강제 적용된 루프 조인(loop-join) 힌트를 식별하고, 이를 제거하도록 제안하며, 새로운 실행 계획을 비교하고 결과를 검증하는 단계를 안내합니다.

전체 점검 목록과 필요한 기능 요건은 `culprit checks`를 실행해 확인할 수 있습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="쿼리 근거, 관련 실행 계획 단계, 권장 해결책 및 검증 단계가 표시된 진단 결과" src="../images/findings-light.png">
</picture>

### 실행 계획 및 런타임 근거

실행 계획을 다이어그램, 들여쓰기 트리 또는 연산자 표 형태로 탐색할 수 있습니다. 인사이트는 고비용 룩업(lookup), 로우 수 추정 오차, 비효율적인 조인, 스필, 타입 변환, 메모리 승인(memory grant) 및 병렬 처리를 설명하고 관련 연산자로 바로 연결합니다.

추정 로우 수와 실제 로우 수, 연산자 속성, 원본 XML 또는 JSON을 검사할 수 있으며, 지원되는 읽기 구문을 직접 실행해 측정할 수도 있습니다. SQL Server에서는 쿼리를 다시 실행하지 않고도 보존된 마지막 실제 실행 계획을 확인하거나, 관련 근거 데이터가 제공되는 경우 실행 중인 쿼리의 연산자 진행 상황을 실시간으로 살펴볼 수 있습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="데이터베이스 시간순으로 정렬된 쿼리 목록과 선택한 쿼리의 실행 계획, 인사이트 및 연산자 세부 정보" src="../images/queries-light.png">
</picture>

### 배포 후 변경 사항 분석

데이터베이스 시간 차트를 드래그하여 두 기간을 비교할 수 있습니다. 느려진 쿼리, 새로 등장한 쿼리, 개선된 쿼리, 실행이 중단된 구문을 한눈에 파악하고, 실행 계획 변경, 스키마 및 설정 변경, 재시작, 배포 마크를 함께 대조할 수 있습니다.

기존 `aws` 또는 `az` 로그인 세션을 활용하면 Amazon RDS CPU 크레딧, I/O 한도, Azure 티어 한도와 같은 클라우드 메트릭 컨텍스트가 함께 표시됩니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="데이터베이스 시간과 실행 횟수 변화를 함께 보여주는 느려진 쿼리, 신규 쿼리 및 개선된 쿼리의 기간 비교 화면" src="../images/what-changed-light.png">
</picture>

### 실행 계획 비교 및 공유

다른 도구에서 추출한 실행 계획이나 이벤트 캡처를 붙여넣거나 드롭하여 열고, 브라우저에서 [explain.culpritsql.com](https://explain.culpritsql.com)을 통해 바로 검사할 수도 있습니다. SQL Server `.sqlplan` 및 `.xel`, PostgreSQL EXPLAIN 텍스트 및 JSON, YugabyteDB DIST 출력, MySQL/MariaDB 실행 계획, ClickHouse `EXPLAIN PLAN json=1`을 지원합니다. 동기화된 다이어그램으로 두 버전을 나란히 비교하고 연산자, 접근 경로, 추정 로우 수 및 비용의 변경 사항을 강조 표시로 확인하세요.

실행 계획에 관련 정보가 남아 있는 경우 캡처된 파라미터, 통계, 병렬 처리 및 메모리의 차이도 함께 설명합니다. 진단 결과와 실행 계획은 리터럴 값 마스킹(redaction)이 기본 활성화된 상태로 Markdown, 독립 실행형 HTML 페이지 또는 네이티브 실행 계획 파일로 공유할 수 있습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="차이점 설명과 변경된 연산자 강조 표시가 포함된 두 실행 계획의 나란히 비교 화면" src="../images/compare-light.png">
</picture>

### 실시간 세션, 블로킹 및 교착 상태

블로킹 체인을 추적해 지연을 유발하는 세션을 찾아냅니다. 대기, 장기 실행 구문, 애플리케이션이 열어 둔 채 방치한 트랜잭션을 검사할 수 있습니다. 기록된 교착 상태에는 관련 구문, 발생 패턴 및 권장 조치 단계가 포함됩니다.

실시간 활성 상태는 자동으로 새로 고침되며 일시 중지할 수 있습니다. 비프로덕션 연결에서는 앱이나 CLI에서 구문을 취소하거나 세션을 종료할 수 있습니다.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="열린 트랜잭션과 그 뒤에서 대기 중인 블로킹된 세션들을 보여주는 실시간 세션 화면" src="../images/live-light.png">
</picture>

### SQL 편집 및 데이터베이스 도구

| 기능 | 포함 내용 |
|---|---|
| **인덱스 어드바이저** | 워크로드 기반 인덱스 후보 추천, 중복·중첩·미사용 인덱스 식별 및 인덱스 유지에 따른 쓰기 비용 분석 |
| **SQL 에디터** | 탭 상태 유지, 스키마 인식 자동 완성, 저장된 쿼리, 추정 및 실측 실행 계획, 빠른 수정이 포함된 인라인 마이그레이션 경고 |
| **마이그레이션 린터** | 저장된 스냅샷 기반 오프라인 점검, 동시(concurrent) 또는 온라인 작업을 위한 지원 DDL 재작성 |
| **시간 소비 분석** | 쿼리와 연결된 대기 카테고리 분석 및 기록된 워크로드와 비교할 수 있는 실시간 샘플 |
| **상태 신호** | 메모리, 컴파일, 연결, 로우 잠금, 파일별 스토리지 지연 시간, 복제 및 클라우드 한도 모니터링 |
| **스키마 탐색기** | 테이블 크기, 인덱스 및 사용량 확인; 환경 간 컬럼, 인덱스, 외래 키, 트리거, 제약 조건 및 파티션 경계를 비교하고 차이를 맞추는 SQL 생성 |
| **연결 관리** | 연결 문자열 지원 및 Azure Data Studio, VS Code, DBeaver, DataGrip 연결 가져오기 |
| **자동 탐색 및 SSH** | CLI 로그인을 활용한 Azure 및 AWS 데이터베이스 탐색, [SSH 점프 호스트](../ssh.md)를 통한 연결 |
| **오프라인 리포트** | 데이터베이스 연결 없이도 검사할 수 있는 저장된 스캔, 추세, 과거 이력 비교 및 공유 스냅샷 |
| **아카이브** | 게시된 Azure Data Explorer (ADX) 및 ClickHouse 텔레메트리 웨어하우스 기반의 읽기 전용 인시던트, 시간 범위 및 실행 계획 조사 |
| **키보드 탐색** | 명령 팔레트(⌘K / Ctrl+K), 뷰 단축키, 쿼리 탭 및 작업 실행 |
| **지원 언어** | 영어(English), 중국어 간체(简体中文), 일본어(日本語), 한국어, 러시아어(Русский), 독일어(Deutsch), 스페인어(Español), 프랑스어(Français), 포르투갈어(Português) |

<a id="collector-and-culprit-fleet"></a>
## 수집기 및 Culprit Fleet

대화형 사용자나 에이전트를 프로덕션에 직접 연결하지 않고도 데이터베이스를 지속적으로 모니터링하려면 리소스가 제한된 읽기 전용 백그라운드 수집기인 `culprit collect`를 실행하세요. 수집기는 봉인된 캡처를 내구성 있는 로컬 아웃박스(outbox)에 기록하고, 원본 근거 데이터를 프라이빗 **Azure Blob Storage** 또는 **Amazon S3**에 백업하며, Culprit의 순수 진단 엔진을 로컬에서 실행한 뒤 인시던트(`CulpritIncidentsV1`), 진단 결과(`CulpritFindingsV1`), 스키마/설정 변경(`CulpritChangesV1`) 및 13개의 원본 근거 테이블을 **Azure Data Explorer (ADX)** 또는 **ClickHouse**로 게시합니다.

### 1. 로컬 캡처 또는 웨어하우스 DDL 생성

```sh
# Capture a bounded snapshot into a local outbox (no warehouse required)
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# Or generate warehouse table and helper-view DDL directly from the container image
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. Kubernetes에 보안 강화된 수집기 샤드 배포

멀티 아키텍처 이미지(`linux/amd64` 및 `linux/arm64`)는 `ghcr.io/culpritsql/culprit-collector`로 제공되며, 기본 레플리카 수가 0으로 설정되고 단일 작성자(single-writer) `StatefulSet` 소유권(`65532:65532`, 읽기 전용 루트 파일시스템)을 강제하며 소스·복구·웨어하우스 자격 증명을 분리하는 OCI Helm 차트와 함께 배포됩니다:

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. Culprit Fleet, SQL Studio 또는 CLI에서 조사

프로덕션 데이터베이스에 실시간 연결을 전혀 맺지 않고도 ADX 또는 ClickHouse에서 전체 플릿의 인시던트, 시간 범위, 쿼리 타임라인, 실행 계획 및 설정 드리프트를 바로 탐색할 수 있습니다:

- **웹 콘솔:** **[fleet.culpritsql.com](https://fleet.culpritsql.com)** (또는 **[culprit-fleet](https://github.com/culpritsql/culprit-fleet)** 자체 호스팅)
- **데스크톱 앱:** Culprit SQL Studio 사이드바에서 **아카이브** 열기
- **CLI 및 MCP:** `culprit resources`, `culprit window`, `culprit incident`, `culprit collect queries`(Grafana 대시보드 및 알림 규칙), `culprit mcp --warehouse-evidence`

[수집기 및 웨어하우스 아키텍처 →](../collector.md) · [Kubernetes Helm 차트 →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## 코딩 에이전트 및 CI

### MCP 및 저장소 설정

MCP 서버를 통해 Claude Code, Cursor, Codex 및 기타 MCP 클라이언트가 진단 결과, 실행 계획, 스키마, 마이그레이션, 블로킹, 대기, 인덱스 조언 및 검증 기능에 접근할 수 있습니다. 에이전트는 저장된 연결 이름만 사용하며, 자격 증명은 Culprit 내부에 안전하게 유지됩니다.

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

두 번째 명령은 저장소 지침과 MCP 설정을 추가합니다. 다른 클라이언트에서는 `mcp` 인자를 지정해 `culprit`을 직접 등록할 수 있습니다. 쿼리 주석에 Marginalia나 SqlCommenter 어노테이션 등 `file:line` 참조가 포함되어 있으면 진단 브리프에서 해당 애플리케이션 코드 위치를 바로 가리킬 수 있습니다.

진단 항목의 브리프를 출력하거나, 실제 인덱스를 생성하지 않고 HypoPG를 통해 제안된 PostgreSQL 인덱스를 테스트해 보세요:

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

접근 권한은 연결별로 설정합니다: 사용 안 함, 통계 및 추정 실행 계획(기본값), 또는 승인된 읽기 전용 쿼리 중에서 선택할 수 있습니다. 쿼리 승인 창에는 실행될 정확한 구문이 표시되며, 모든 도구 호출은 로컬 감사 로그에 기록됩니다.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="정확한 SQL, 연결, 로우 및 시간 제한, 거부 또는 1회 실행 제어 버튼을 보여주는 에이전트 쿼리 승인 화면" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[에이전트 설정, 권한 및 감사 상세 안내 →](../agents.md)

### 마이그레이션 점검 및 배포 워크플로

저장된 스냅샷의 테이블 크기, 컬럼 타입, 인덱스 및 기록된 워크로드를 기준으로 마이그레이션 파일을 점검합니다. 블로킹을 유발하는 `CREATE INDEX` 및 `ALTER TABLE` 작업, 누락되거나 타입이 일치하지 않는 외래 키 인덱스, 기록된 쿼리에 영향을 주는 `DROP` 또는 `RENAME` 변경 사항을 찾아냅니다.

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

`--fail-on high`를 사용하면 높은 심각도의 진단 항목이 발견될 때 CI를 실패 처리할 수 있으며, `--write`는 지원되는 재작성 구문을 마이그레이션 파일에 바로 적용합니다. JSON은 스크립트 연동에, SARIF는 코드 리뷰 도구 연동에 활용할 수 있으며, 고정된 진단 ID를 통해 여러 스캔에 걸쳐 동일한 문제를 추적할 수 있습니다.

배포 후에는 타임라인에 배포 시점을 표시하고, 진단 항목의 해결 여부를 검증하거나 민감 정보가 마스킹된 요약을 생성할 수 있습니다:

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## 프로덕션 환경에서의 사용

- **스캔은 통계 및 카탈로그 뷰만 읽으며**, 프로바이더별 타임아웃과 실행 계획 안전장치가 적용됩니다.
- **데이터베이스 수정 사항은 제안 형태로만 제공되며**, 사용자가 직접 검토하고 적용합니다.
- **프로덕션 실행 시 확인 절차를 거칩니다.** Culprit이 읽기 작업으로 분류할 수 없는 구문은 실행 전 확인을 요구하며, 파괴적인 작업은 추가 확인을 거칩니다.
- **에이전트 쿼리 접근은 명시적 선택(opt-in) 방식**이며, 각 읽기 전용 구문에 대한 개별 승인과 로컬 감사 기록이 남습니다.
- **저장된 비밀번호는 OS 키체인을 사용합니다.** Entra ID는 기존 Azure 로그인 세션을 사용할 수 있으며, 공유용 내보내기 파일은 기본적으로 리터럴 값을 마스킹합니다.

워크플로에 필요한 권한만 가진 데이터베이스 로그인을 사용하세요. 수집, 실행 계획 수립 및 쿼리 실행의 경계에 대한 자세한 내용은 [안전 가이드](../guide.md#safety)와 [프로바이더 문서](../parity.md)에서 설명합니다.

## 개발

Culprit은 Rust, Tauri, React로 구축되었습니다. 앱, CLI, MCP 서버는 단일 분석 엔진을 공유합니다. 저장소를 포크하지 않고도 Rust API를 통해 커스텀 점검 규칙이나 데이터베이스 프로바이더를 추가할 수 있으며, 사용자에게 표시되는 텍스트는 [언어 카탈로그](../localization.md)에서 제공됩니다.

<details>
<summary>소스에서 빌드하기</summary>

[Rust stable](https://rustup.rs)과 [Bun](https://bun.sh)을 설치하세요. Linux 환경에서는 [플랫폼 의존성 패키지](../guide.md#platforms)도 필요합니다.

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# Desktop app
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## 더 알아보기

- **[가이드](../guide.md):** 모든 점검 항목과 명령어, 에이전트 설정, 측정 속도, 안전 세부 사항 및 Culprit 확장 방법
- **[수집기 및 웨어하우스](../collector.md):** 백그라운드 수집, ADX 및 ClickHouse 스키마, Azure Blob/S3 복구 및 [Culprit Fleet](https://fleet.culpritsql.com)
- **[아키텍처](../ARCHITECTURE.md):** 분석 파이프라인, 모든 점검 규칙이 따르는 근거 원칙 및 크레이트 구조
- **[디자인](../DESIGN.md):** 제품의 동작 방식과 설계 배경
- **[코딩 에이전트](../agents.md):** MCP 도구, 권한, 승인 절차 및 감사 로그
- **[엔진 기능 비교](../parity.md):** 엔진별 기능 비교, 프로바이더 가이드, 설정 방법 및 검증된 버전

Culprit은 [MIT 라이선스](../../LICENSE)로 배포되는 오픈 소스입니다.

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>데이터베이스가 느려질 때 호출받는 사람들을 위해 만들었습니다.</sub>
</p>
