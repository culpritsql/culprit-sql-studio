<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>Datenbank-Engpässe finden – mit den Belegen, um sie zu beheben.</strong><br>
  Desktop-App und CLI zur Performance-Analyse von Datenbanken unter macOS, Linux und Windows.
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
  <img alt="MCP-Server" src="https://img.shields.io/badge/MCP-ready-1b1a18?style=flat-square">
  <img alt="macOS, Linux und Windows" src="https://img.shields.io/badge/macOS%20%C2%B7%20Linux%20%C2%B7%20Windows-1b1a18?style=flat-square">
  <a href="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml/badge.svg"></a>
  <a href="../../LICENSE"><img alt="MIT-Lizenz" src="https://img.shields.io/badge/license-MIT-ffcf33?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>Herunterladen</strong></a> ·
  <a href="#get-started">Schnellstart</a> ·
  <a href="#what-you-get">Funktionen</a> ·
  <a href="#supported-databases">Provider</a> ·
  <a href="#collector-and-culprit-fleet">Collector &amp; Fleet</a> ·
  <a href="#coding-agents-and-ci">Agenten &amp; CI</a> ·
  <a href="../guide.md">Handbuch</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">Live-Fleet-Demo</a> ·
  <a href="https://explain.culpritsql.com">Live-Plan- &amp; .xel-Analyse</a>
</p>

<p align="center">
  <sub>
    <a href="../../README.md">English</a> ·
    <a href="README.zh-Hans.md">简体中文</a> ·
    <a href="README.ja.md">日本語</a> ·
    <a href="README.ko.md">한국어</a> ·
    <a href="README.ru.md">Русский</a> ·
    <strong>Deutsch</strong> ·
    <a href="README.es.md">Español</a> ·
    <a href="README.fr.md">Français</a> ·
    <a href="README.pt-BR.md">Português</a>
  </sub>
</p>

Culprit führt Abfragestatistiken, Ausführungspläne, Wartezeiten und Schemakontext in einem Workflow zusammen. Es ordnet Befunde nach Auswirkung und verknüpft jeden Befund mit Belegen, einer empfohlenen Behebung und Schritten zur Überprüfung. Nutzen Sie Culprit in der Desktop-App, im Terminal oder über einen Coding-Agenten.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="Datenbank-Dashboard mit Zeitverlauf, nach Auswirkung geordneten Befunden, teuren Abfragen, Wartezeiten und aktueller Aktivität" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## Schnellstart

[Laden Sie die Desktop-App oder die eigenständige CLI herunter](https://github.com/culpritsql/culprit-sql-studio/releases/latest). Fügen Sie in der App eine Verbindung hinzu oder öffnen Sie eine SQLite-Datei, um das zugehörige Dashboard anzuzeigen.

Im Terminal:

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

Beim erneuten Öffnen einer Verbindung wird sofort der gespeicherte Bericht angezeigt, während im Hintergrund ein neuer Scan läuft. Die Desktop-App bietet signierte Updates; die eigenständige CLI aktualisieren Sie mit `culprit update`.

<details>
<summary>Oberfläche mit Demodaten ausprobieren</summary>

Mit installiertem [Bun](https://bun.sh):

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

Die Browser-Demo nutzt mitgelieferte Snapshots und benötigt keine Datenbankverbindung.

</details>

<a id="supported-databases"></a>
## Unterstützte Datenbanken

| Provider | Quellen und Funktionen |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store, Plancache, DMVs und aufbewahrte Deadlock-Ereignisse |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`, Katalog- und Statistiksichten, EXPLAIN sowie hypothetische Indizes über HypoPG |
| [**TimescaleDB**](../providers/timescale.md) | Quellen von PostgreSQL sowie Hypertable-Größen und verwaltete/komprimierte Chunks |
| [**YugabyteDB**](../providers/yugabyte.md) | Native YSQL-/DocDB-Zähler, DIST-Pläne, Sharding und abgegrenzte Aktivitätsquellen |
| [**MySQL**](../providers/mysql.md) | `performance_schema`, `sys`, EXPLAIN und das Slow Query Log |
| [**MariaDB**](../providers/mariadb.md) | Performance Schema, Slow Query Log, Benutzerstatistiken, Pläne und Sperrsichten von MariaDB |
| [**ClickHouse**](../providers/clickhouse.md) | Protokollierte initiale Abfragen (`system.query_log`), MergeTree-Parts/-Merges, Mutationen, Replikation und `EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | Schreibgeschützte Dateiprüfung, `EXPLAIN QUERY PLAN` und Abfragelogs gängiger Frameworks |

Der Funktionsumfang hängt von der Datenbank-Engine, den aktivierten Quellen und den Berechtigungen Ihres Logins ab. Fehlende Quellen werden mit Begründung und Einrichtungshinweisen angezeigt. Einen Funktionsvergleich sowie getestete Versionen finden Sie unter [Engine-Parität](../parity.md).

<a id="what-you-get"></a>
## Funktionen im Überblick

### Befunde mit Belegen, Behebungen und Überprüfung

Jeder Befund benennt die betroffenen Abfragen, das Messzeitfenster und die Belege für seinen Schweregrad. Öffnen Sie einen Befund, um eine empfohlene Änderung, direkt kopierbares SQL (sofern zutreffend), Schritte zur Überprüfung sowie eine Kurzfassung zum Teilen mit Ihrem Team oder einem Coding-Agenten zu erhalten.

| Bereich | Beispiele für Prüfungen in Culprit |
|---|---|
| Abfrage-Performance | Teure Joins, Planregressionen, parameterempfindliche Pläne, indexblockierende Typkonvertierungen, N+1-Muster, Fehler, Timeouts und Überläufe |
| Indizes und Schema | Fehlende, ungenutzte, doppelte und überlappende Indizes; Fremdschlüssel ohne Index; Partitionsrisiken |
| Wartung und Kapazität | Veraltete Statistiken, tote Tupel, Tabellen-Bloat, übergroße Zeilen, Sequenzerschöpfung und Risiko eines Transaktions-ID-Wraparounds |
| Datenbankzustand | Hot Rows, Sperrausweitung zu Tabellensperren, Speicherlatenz, Seitenprüfsummenfehler, Verbindungsfluktuation, Replikationsverzögerung und Einstellungen |

Die Demo für SQL Server enthält beispielsweise einen Ausführungsplan mit **1,5 Milliarden Zeilenvergleichen**. Auf die betroffene Abfrage entfallen über fünf Stunden **27 % der Datenbankzeit**. Culprit erkennt den erzwungenen Loop-Join-Hinweis, empfiehlt dessen Entfernung und beschreibt Schritt für Schritt, wie Sie den neuen Plan vergleichen und prüfen, ob das Problem behoben ist.

Führen Sie `culprit checks` aus, um die vollständige Liste aller Prüfungen und der erforderlichen Fähigkeiten anzuzeigen.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="Ein Befund mit Abfragebelegen, relevantem Planschritt, empfohlener Behebung und Schritten zur Überprüfung" src="../images/findings-light.png">
</picture>

### Ausführungspläne und Laufzeitbelege

Untersuchen Sie Ausführungspläne als Diagramm, eingerückten Baum oder Operatorentabelle. Erkenntnisse erläutern teure Lookups, falsche Zeilenschätzungen, ineffiziente Joins, Überläufe, Typkonvertierungen, Speicherzuweisungen und Parallelität – mit direkten Links zu den jeweiligen Operatoren.

Prüfen Sie geschätzte und tatsächliche Zeilenanzahlen, Operatoreigenschaften sowie das rohe XML oder JSON. Unterstützte Leseanweisungen lassen sich gezielt messen. Auf SQL Server können Sie den aufbewahrten letzten tatsächlichen Plan untersuchen, ohne die Abfrage erneut auszuführen, oder den Operatorfortschritt einer laufenden Abfrage verfolgen, sofern diese Belege vorliegen.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="Nach Datenbankzeit geordnete Abfragen mit dem Ausführungsplan einer ausgewählten Abfrage, Erkenntnissen und Operatordetails" src="../images/queries-light.png">
</picture>

### Was sich nach einem Deployment geändert hat

Ziehen Sie im Diagramm der Datenbankzeit einen Bereich auf, um zwei Zeiträume zu vergleichen: Abfragen, die langsamer geworden sind, neue Abfragen, Verbesserungen sowie Anweisungen, die nicht mehr ausgeführt werden. Daneben sehen Sie Planwechsel, Schema- oder Einstellungsänderungen, Neustarts und Deployment-Markierungen.

Cloud-Metriken ergänzen Kontext wie CPU-Guthaben von Amazon RDS, I/O-Limits und Limits der Azure-Dienstebene – direkt über Ihre bestehende Anmeldung bei `aws` oder `az`.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="Ein Zeitraumvergleich mit langsameren, neuen und verbesserten Abfragen sowie Änderungen bei Datenbankzeit und Ausführungsanzahl" src="../images/what-changed-light.png">
</picture>

### Pläne vergleichen und teilen

Fügen Sie einen Ausführungsplan oder eine Ereignisaufzeichnung aus einem anderen Tool ein, ziehen Sie die Datei ins Fenster oder öffnen Sie sie – oder untersuchen Sie den Plan direkt im Browser unter [explain.culpritsql.com](https://explain.culpritsql.com): `.sqlplan` und `.xel` von SQL Server, EXPLAIN-Text oder -JSON von PostgreSQL, DIST-Ausgaben von YugabyteDB, Pläne von MySQL/MariaDB oder `EXPLAIN PLAN json=1` von ClickHouse. Vergleichen Sie zwei Versionen mit synchronisierten Diagrammen und hervorgehobenen Änderungen bei Operatoren, Zugriffspfaden, Zeilenschätzungen und Kosten.

Vergleiche erläutern außerdem Unterschiede bei erfassten Parametern, Statistiken, Parallelität und Arbeitsspeicher, sofern die Pläne diese Angaben enthalten. Teilen Sie Befunde und Pläne als Markdown, als eigenständige HTML-Seite oder als native Plandatei; Literalwerte werden dabei standardmäßig geschwärzt.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="Zwei Pläne nebeneinander mit einer Erläuterung der Unterschiede und hervorgehobenen geänderten Operatoren" src="../images/compare-light.png">
</picture>

### Live-Sitzungen, Blockierungen und Deadlocks

Verfolgen Sie Blockierungsketten zurück bis zu der blockierenden Sitzung an der Spitze. Untersuchen Sie Wartezeiten, lang laufende Anweisungen und von der Anwendung offen gelassene Transaktionen. Aufgezeichnete Deadlocks zeigen die beteiligten Anweisungen, das Muster und empfohlene nächste Schritte.

Die Live-Aktivität wird automatisch aktualisiert und lässt sich pausieren. Auf Verbindungen außerhalb der Produktion können Sie eine Anweisung abbrechen oder eine Sitzung über die App oder die CLI beenden.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="Live-Sitzungen mit einer offenen Transaktion und den dahinter wartenden blockierten Sitzungen" src="../images/live-light.png">
</picture>

### SQL-Bearbeitung und Datenbank-Werkzeuge

| Funktion | Umfang |
|---|---|
| **Index-Ratgeber** | Workload-basierte Indexkandidaten, doppelte, überlappende und ungenutzte Indizes sowie die Schreibkosten für deren Beibehaltung |
| **SQL-Editor** | Persistente Tabs, schemabasierte Autovervollständigung, gespeicherte Abfragen, geschätzte und gemessene Pläne sowie Inline-Migrationswarnungen mit Schnellkorrekturen |
| **Migrations-Linter** | Offline-Prüfungen gegen gespeicherte Snapshots; unterstützte DDL-Umschreibungen für nebenläufige oder Online-Operationen |
| **Wohin die Zeit geht** | Mit Abfragen verknüpfte Wartekategorien sowie eine Live-Stichprobe zum Vergleich mit der aufgezeichneten Workload |
| **Zustandssignale** | Arbeitsspeicher, Kompilierungen, Verbindungen, Zeilensperren, Speicherlatenz pro Datei, Replikation und Cloud-Limits |
| **Schema-Explorer** | Tabellengrößen, Indizes und Nutzung; Vergleich von Spalten, Indizes, Fremdschlüsseln, Triggern, Einschränkungen und Partitionsgrenzen über Umgebungen hinweg – inklusive SQL zum Angleich der Unterschiede |
| **Verbindungen** | Verbindungszeichenfolgen und Importe aus Azure Data Studio, VS Code, DBeaver und DataGrip |
| **Erkennung und SSH** | Azure- und AWS-Datenbanken über Ihre CLI-Anmeldung finden; Verbindung über einen [SSH-Jump-Host](../ssh.md) herstellen |
| **Offline-Berichte** | Gespeicherte Scans, Trends, historische Vergleiche und geteilte Snapshots, die Sie ohne Datenbankverbindung untersuchen können |
| **Archive** | Schreibgeschützte Untersuchung von Vorfällen, Zeitfenstern und Ausführungsplänen über veröffentlichte Telemetrie-Warehouses in Azure Data Explorer (ADX) und ClickHouse |
| **Tastatursteuerung** | Befehlspalette (⌘K / Ctrl+K), Tastenkürzel für Ansichten, Abfrage-Tabs und Aktionen |
| **Sprachen** | Englisch (English), Vereinfachtes Chinesisch (简体中文), Japanisch (日本語), Koreanisch (한국어), Russisch (Русский), Deutsch, Spanisch (Español), Französisch (Français) und Portugiesisch (Português) |

<a id="collector-and-culprit-fleet"></a>
## Collector und Culprit Fleet

Führen Sie `culprit collect` als begrenzten, schreibgeschützten Hintergrund-Collector aus, um Datenbanken kontinuierlich zu überwachen, ohne interaktive Benutzer oder Agenten mit der Produktion zu verbinden. Der Collector schreibt versiegelte Erfassungen in einen dauerhaften lokalen Telemetrie-Ausgangspuffer, sichert Rohbelege in privatem **Azure Blob Storage** oder **Amazon S3**, führt Culprits reine Diagnose-Engine lokal aus und veröffentlicht Vorfälle (`CulpritIncidentsV1`), Befunde (`CulpritFindingsV1`), Schema- und Einstellungsänderungen (`CulpritChangesV1`) sowie 13 Tabellen mit Rohbelegen in **Azure Data Explorer (ADX)** oder **ClickHouse**.

### 1. Lokal erfassen oder Warehouse-DDL erzeugen

```sh
# Begrenzten Snapshot in einen lokalen Telemetrie-Ausgangspuffer erfassen (kein Warehouse erforderlich)
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# Oder DDL für Warehouse-Tabellen und Hilfssichten direkt aus dem Container-Image erzeugen
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. Gehärteten Collector-Shard auf Kubernetes bereitstellen

Multi-Arch-Images (`linux/amd64` und `linux/arm64`) werden unter `ghcr.io/culpritsql/culprit-collector` bereitgestellt – ergänzt durch ein OCI-Helm-Chart, das standardmäßig null Replikate vorsieht, die Single-Writer-Zuständigkeit per `StatefulSet` erzwingt (`65532:65532`, schreibgeschütztes Root-Dateisystem) und Zugangsdaten für Quelle, Wiederherstellung und Warehouse strikt trennt:

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. In Culprit Fleet, Culprit SQL Studio oder per CLI untersuchen

Untersuchen Sie flottenweite Vorfälle, Zeitfenster, Abfrage-Zeitverläufe, Ausführungspläne und Konfigurations-Drift direkt aus ADX oder ClickHouse – ganz ohne aktive Verbindungen zu Ihren Produktionsdatenbanken:

- **Webkonsole:** **[fleet.culpritsql.com](https://fleet.culpritsql.com)** (oder selbst gehostet über `culprit-archive-web`)
- **Desktop-App:** Öffnen Sie **Archive** in der Seitenleiste von Culprit SQL Studio
- **CLI &amp; MCP:** `culprit resources`, `culprit window`, `culprit incident`, `culprit collect queries` (Grafana-Dashboards &amp; Alarmregeln) und `culprit mcp --warehouse-evidence`

[Collector- &amp; Warehouse-Architektur →](../collector.md) · [Kubernetes-Helm-Chart →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## Coding-Agenten und CI

### MCP- und Repository-Einrichtung

Über den MCP-Server erhalten Claude Code, Cursor, Codex und andere MCP-Clients Zugriff auf Befunde, Ausführungspläne, Schema, Migrationen, Blockierungen, Wartezeiten, Indexempfehlungen und Überprüfungen. Agenten verwenden gespeicherte Verbindungsnamen; die Zugangsdaten verbleiben in Culprit.

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

Der zweite Befehl legt Repository-Anweisungen und die MCP-Konfiguration an. Andere Clients können `culprit` direkt mit dem Argument `mcp` registrieren. Enthalten Abfragekommentare `file:line`-Verweise – einschließlich Marginalia- und SqlCommenter-Annotationen –, können die Kurzfassungen der Befunde direkt auf den Anwendungscode verweisen.

Geben Sie die Kurzfassung eines Befunds aus oder testen Sie einen vorgeschlagenen Index in PostgreSQL über HypoPG, ohne den echten Index anzulegen:

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

Der Zugriff wird pro Verbindung festgelegt: deaktiviert, Statistiken und geschätzte Pläne (Standard) oder freigegebene Leseabfragen. Bei der Abfragefreigabe wird die genaue SQL-Anweisung angezeigt; alle Tool-Aufrufe werden in einem lokalen Audit-Log protokolliert.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="Abfragefreigabe für einen Agenten mit der genauen SQL-Anweisung, Verbindung, Zeilen- und Zeitlimits sowie Schaltflächen zum Ablehnen oder einmaligen Ausführen" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[Agenten-Einrichtung, Berechtigungen und Audit-Details →](../agents.md)

### Migrationsprüfungen und Deployment-Workflows

Prüfen Sie Migrationsdateien anhand eines gespeicherten Snapshots gegen Tabellengrößen, Spaltentypen, Indizes und die aufgezeichnete Workload. Die Befunde decken blockierende `CREATE INDEX`- und `ALTER TABLE`-Operationen, fehlende oder unpassende Fremdschlüssel-Indizes sowie `DROP`- oder `RENAME`-Änderungen ab, die aufgezeichnete Abfragen betreffen.

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

Mit `--fail-on high` lassen Sie CI-Läufe bei Befunden mit hohem Schweregrad fehlschlagen. `--write` wendet unterstützte Umschreibungen direkt auf Migrationsdateien an. JSON eignet sich für Skripte, SARIF für Code-Review-Tools, und dank stabiler Befund-IDs lässt sich dasselbe Problem über mehrere Scans hinweg nachverfolgen.

Markieren Sie nach einem Deployment den Zeitverlauf, prüfen Sie, ob ein Befund behoben ist, oder erzeugen Sie eine geschwärzte Zusammenfassung:

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## Einsatz in der Produktion

- **Scans lesen Statistik- und Katalogsichten**, mit providerspezifischen Timeouts und Schutzgrenzen bei der Planerstellung.
- **Datenbank-Behebungen sind Vorschläge**, die Sie selbst prüfen und anwenden.
- **Ausführungen auf der Produktion erfordern eine Bestätigung** für Anweisungen, die Culprit nicht eindeutig als Lesevorgang einstufen kann; destruktive Operationen erfordern eine zusätzliche Bestätigung.
- **Abfragezugriff für Agenten ist Opt-in**, mit Einzelfreigabe für jede schreibgeschützte Anweisung und einem lokalen Audit-Log.
- **Gespeicherte Passwörter nutzen den Schlüsselbund des Betriebssystems.** Entra ID kann Ihre bestehende Azure-Anmeldung nutzen; teilbare Exporte schwärzen Literalwerte standardmäßig.

Verwenden Sie ein Datenbank-Login mit den Berechtigungen, die Ihr Workflow benötigt. Der [Sicherheitsleitfaden](../guide.md#safety) und die [Provider-Dokumentation](../parity.md) erläutern die Grenzen bei Erfassung, Planerstellung und Ausführung.

## Entwicklung

Culprit ist mit Rust, Tauri und React entwickelt. Desktop-App, CLI und MCP-Server teilen sich eine gemeinsame Analyse-Engine. Eigene Prüfungen oder Datenbank-Provider lassen sich über die Rust-API ohne Fork ergänzen; [Sprachkataloge](../localization.md) liefern sämtliche Texte der Benutzeroberfläche.

<details>
<summary>Aus dem Quellcode kompilieren</summary>

Installieren Sie [Rust stable](https://rustup.rs) und [Bun](https://bun.sh). Unter Linux werden zusätzlich die [Plattformabhängigkeiten](../guide.md#platforms) benötigt.

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# Desktop-App
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## Weiterführende Dokumentation

- **[Handbuch](../guide.md):** alle Prüfungen und Befehle, Agenten-Einrichtung, gemessene Geschwindigkeit, Sicherheitsdetails und Erweiterungsmöglichkeiten für Culprit
- **[Collector und Warehouses](../collector.md):** Hintergrunderfassung, ADX- und ClickHouse-Schemata, Wiederherstellung aus Azure Blob/S3 und [Culprit Fleet](https://fleet.culpritsql.com)
- **[Architektur](../ARCHITECTURE.md):** die Analyse-Pipeline, die Belegregeln jeder Prüfung und die Übersicht der Crates
- **[Design](../DESIGN.md):** wie sich das Produkt verhält und warum
- **[Coding-Agenten](../agents.md):** MCP-Tools, Berechtigungen, Freigaben und das Audit-Log
- **[Engine-Parität](../parity.md):** Funktionsvergleich, Provider-Anleitungen, Einrichtung und getestete Versionen

Culprit ist Open Source unter der [MIT-Lizenz](../../LICENSE).

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>Entwickelt für alle, die in der Rufbereitschaft alarmiert werden, wenn die Datenbank langsam ist.</sub>
</p>
