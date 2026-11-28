<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>Encuentra cuellos de botella en bases de datos, con la evidencia para corregirlos.</strong><br>
  Aplicación de escritorio y CLI para el análisis de rendimiento de bases de datos en macOS, Linux y Windows.
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
  <img alt="Servidor MCP" src="https://img.shields.io/badge/MCP-ready-1b1a18?style=flat-square">
  <img alt="macOS, Linux y Windows" src="https://img.shields.io/badge/macOS%20%C2%B7%20Linux%20%C2%B7%20Windows-1b1a18?style=flat-square">
  <a href="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml/badge.svg"></a>
  <a href="../../LICENSE"><img alt="Licencia MIT" src="https://img.shields.io/badge/license-MIT-ffcf33?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>Descargar</strong></a> ·
  <a href="#get-started">Inicio rápido</a> ·
  <a href="#what-you-get">Funciones</a> ·
  <a href="#supported-databases">Proveedores</a> ·
  <a href="#collector-and-culprit-fleet">Recopilador y Fleet</a> ·
  <a href="#coding-agents-and-ci">Agentes y CI</a> ·
  <a href="../guide.md">Guía</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">Demo Fleet en vivo</a> ·
  <a href="https://explain.culpritsql.com">Analizador de planes y .xel</a>
</p>

<p align="center">
  <sub>
    <a href="../../README.md">English</a> ·
    <a href="README.zh-Hans.md">简体中文</a> ·
    <a href="README.ja.md">日本語</a> ·
    <a href="README.ko.md">한국어</a> ·
    <a href="README.ru.md">Русский</a> ·
    <a href="README.de.md">Deutsch</a> ·
    <strong>Español</strong> ·
    <a href="README.fr.md">Français</a> ·
    <a href="README.pt-BR.md">Português</a>
  </sub>
</p>

Culprit reúne estadísticas de consultas, planes de ejecución, esperas y contexto del esquema en un solo flujo de trabajo. Clasifica los hallazgos por impacto y vincula cada uno con su evidencia, una corrección sugerida y pasos de verificación. Úsalo desde la aplicación de escritorio, la terminal o un agente de programación.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="Panel de la base de datos con línea de tiempo, hallazgos clasificados por impacto, consultas costosas, esperas y actividad actual" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## Primeros pasos

[Descarga la aplicación de escritorio o la CLI independiente](https://github.com/culpritsql/culprit-sql-studio/releases/latest). En la aplicación, agrega una conexión o abre un archivo SQLite para ver su panel.

Desde la terminal:

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

Al volver a abrir una conexión se muestra su informe guardado mientras se ejecuta un nuevo escaneo en segundo plano. La aplicación de escritorio ofrece actualizaciones firmadas; actualiza la CLI independiente con `culprit update`.

<details>
<summary>Prueba la interfaz con datos de demostración</summary>

Con [Bun](https://bun.sh) instalado:

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

La demostración en el navegador usa instantáneas incluidas y no necesita conexión a ninguna base de datos.

</details>

<a id="supported-databases"></a>
## Bases de datos compatibles

| Proveedor | Orígenes y capacidades |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store, caché de planes, DMV y eventos de interbloqueo conservados |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`, vistas de catálogo y estadísticas, EXPLAIN e índices hipotéticos mediante HypoPG |
| [**TimescaleDB**](../providers/timescale.md) | Orígenes de PostgreSQL más tamaños de hypertables y chunks administrados o comprimidos |
| [**YugabyteDB**](../providers/yugabyte.md) | Contadores nativos de YSQL/DocDB, planes DIST, particionamiento horizontal (sharding) y orígenes de actividad acotados |
| [**MySQL**](../providers/mysql.md) | `performance_schema`, `sys`, EXPLAIN y el registro de consultas lentas |
| [**MariaDB**](../providers/mariadb.md) | Performance Schema, registro de consultas lentas, estadísticas por usuario, planes de MariaDB y vistas de bloqueos |
| [**ClickHouse**](../providers/clickhouse.md) | Consultas iniciales registradas (`system.query_log`), partes y fusiones de MergeTree, mutaciones, replicación y `EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | Inspección de archivos en modo de solo lectura, `EXPLAIN QUERY PLAN` y registros de consultas del framework |

Las funciones dependen del motor, de los orígenes habilitados y de los permisos de tu inicio de sesión. Los orígenes no disponibles se muestran con el motivo y pasos de configuración. Consulta [Paridad entre motores](../parity.md) para ver la comparativa de funciones y las versiones verificadas.

<a id="what-you-get"></a>
## Qué ofrece

### Hallazgos con evidencia, correcciones y verificación

Cada hallazgo identifica las consultas afectadas, la ventana de medición y la evidencia detrás de su gravedad. Ábrelo para ver el cambio sugerido, el código SQL listo para copiar cuando corresponda, los pasos de verificación y un resumen para compartir con un compañero o un agente de programación.

| Área | Ejemplos de lo que comprueba Culprit |
|---|---|
| Rendimiento de consultas | Combinaciones costosas, regresiones de planes, planes sensibles a parámetros, conversiones de tipos que impiden el uso de índices, patrones N+1, fallos, tiempos de espera agotados y volcados a disco |
| Índices y esquema | Índices que faltan, sin usar, duplicados y superpuestos; claves foráneas sin índice; riesgos en particiones |
| Mantenimiento y capacidad | Estadísticas desactualizadas, tuplas muertas, inflado de tablas (bloat), filas grandes, agotamiento de secuencias y riesgo de desbordamiento de ID de transacción (wraparound) |
| Estado de la base de datos | Filas muy disputadas, escalado de bloqueos de tabla, latencia de almacenamiento, corrupción de sumas de comprobación de páginas, rotación excesiva de conexiones, retraso de replicación y configuración |

Por ejemplo, la demostración de SQL Server incluye un plan que realiza **1500 millones de comparaciones de filas**. La consulta afectada representa el **27 % del tiempo de base de datos** durante cinco horas. Culprit identifica la sugerencia de combinación de bucles forzada, propone quitarla y ofrece los pasos para comparar el nuevo plan y verificar el resultado.

Ejecuta `culprit checks` para ver la lista completa de comprobaciones y las capacidades necesarias.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="Un hallazgo con evidencia de la consulta, el paso relevante del plan, una corrección sugerida y pasos de verificación" src="../images/findings-light.png">
</picture>

### Planes de ejecución y evidencia en tiempo de ejecución

Explora los planes como diagrama, árbol con sangría o tabla de operaciones. Las observaciones explican búsquedas de clave costosas, errores en la estimación de filas, combinaciones ineficientes, volcados a disco, conversiones, concesiones de memoria y paralelismo, con enlaces a los operadores relevantes.

Inspecciona las filas estimadas y reales, las propiedades de los operadores y el XML o JSON sin procesar. Mide explícitamente las instrucciones de lectura compatibles. En SQL Server, inspecciona el último plan real conservado sin volver a ejecutar la consulta, o sigue el progreso por operador de una consulta en ejecución cuando esa evidencia esté disponible.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="Consultas clasificadas por tiempo de base de datos, con el plan de ejecución, observaciones y detalles de operadores de la consulta seleccionada" src="../images/queries-light.png">
</picture>

### Qué cambió tras un despliegue

Arrastra sobre el gráfico de tiempo de base de datos para comparar dos periodos: consultas que se volvieron más lentas, consultas nuevas, mejoras e instrucciones que dejaron de ejecutarse. Observa cambios de plan, cambios de esquema o de ajustes, reinicios y marcas de despliegue junto a esas variaciones.

Las métricas de la nube aportan contexto adicional, como créditos de CPU de Amazon RDS, límites de E/S y límites de nivel en Azure, usando tu sesión iniciada en `aws` o `az`.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="Comparación de periodos que muestra consultas más lentas, nuevas y mejoradas con cambios en el tiempo de base de datos y en el número de ejecuciones" src="../images/what-changed-light.png">
</picture>

### Comparar y compartir planes

Pega, arrastra o abre un plan o una captura de eventos de otra herramienta —o inspecciónalos en el navegador en [explain.culpritsql.com](https://explain.culpritsql.com)—: archivos `.sqlplan` y `.xel` de SQL Server, salida de texto o JSON de EXPLAIN en PostgreSQL, salida DIST de YugabyteDB, planes de MySQL/MariaDB o `EXPLAIN PLAN json=1` de ClickHouse. Compara dos versiones con diagramas sincronizados y cambios resaltados en operadores, rutas de acceso, estimaciones de filas y costo.

Las comparaciones también explican diferencias en parámetros capturados, estadísticas, paralelismo y memoria cuando los planes conservan esos datos. Comparte hallazgos y planes en formato Markdown, como página HTML autocontenida o como archivo de plan nativo, con ocultación de valores literales activada de forma predeterminada.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="Dos planes lado a lado con una explicación de sus diferencias y los operadores modificados resaltados" src="../images/compare-light.png">
</picture>

### Sesiones en vivo, bloqueos e interbloqueos

Sigue las cadenas de bloqueo hasta la sesión que las retiene. Inspecciona esperas, instrucciones de larga duración y transacciones que la aplicación dejó abiertas. Los interbloqueos registrados incluyen las instrucciones participantes, el patrón y los siguientes pasos sugeridos.

La actividad en vivo se actualiza automáticamente y se puede pausar. En conexiones que no son de producción, cancela una instrucción o finaliza una sesión desde la aplicación o la CLI.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="Sesiones en vivo con una transacción abierta y las sesiones bloqueadas esperando detrás de ella" src="../images/live-light.png">
</picture>

### Edición de SQL y herramientas de base de datos

| Función | Qué incluye |
|---|---|
| **Asesor de índices** | Candidatos de índices basados en la carga de trabajo, índices duplicados, superpuestos o sin usar, y el costo de escritura de mantenerlos |
| **Editor de SQL** | Pestañas persistentes, autocompletado consciente del esquema, consultas guardadas, planes estimados y medidos, y advertencias de migración en línea con correcciones rápidas |
| **Analizador de migraciones** | Comprobaciones sin conexión contra instantáneas guardadas; reescrituras de DDL compatibles para operaciones concurrentes o en línea |
| **Adónde va el tiempo** | Categorías de espera vinculadas a las consultas, más una muestra en vivo para comparar con la carga de trabajo registrada |
| **Señales de estado** | Memoria, compilaciones, conexiones, bloqueos de filas, latencia de almacenamiento por archivo, replicación y límites de la nube |
| **Explorador de esquemas** | Tamaños de tablas, índices y uso; comparación de columnas, índices, claves foráneas, triggers, restricciones y límites de partición entre entornos, con SQL para conciliar las diferencias |
| **Conexiones** | Cadenas de conexión e importaciones desde Azure Data Studio, VS Code, DBeaver y DataGrip |
| **Detección y SSH** | Encuentra bases de datos en Azure y AWS con el inicio de sesión de tu CLI; conéctate a través de un [host de salto SSH](../ssh.md) |
| **Informes sin conexión** | Escaneos guardados, tendencias, comparaciones históricas e instantáneas compartidas que puedes inspeccionar sin conexión a la base de datos |
| **Archivos** | Investigación en modo de solo lectura de incidentes, ventanas de tiempo y planes de ejecución sobre almacenes de telemetría publicados en Azure Data Explorer (ADX) y ClickHouse |
| **Navegación por teclado** | Paleta de comandos (⌘K / Ctrl+K), atajos de vistas, pestañas de consultas y acciones |
| **Idiomas** | Inglés (English), chino simplificado (简体中文), japonés (日本語), coreano (한국어), ruso (Русский), alemán (Deutsch), español (Español), francés (Français) y portugués (Português) |

<a id="collector-and-culprit-fleet"></a>
## Recopilador y Culprit Fleet

Ejecuta `culprit collect` como recopilador en segundo plano, acotado y de solo lectura, para supervisar bases de datos de forma continua sin conectar usuarios interactivos ni agentes a producción. El recopilador escribe capturas selladas en un búfer de salida local duradero, respalda la evidencia original en **Azure Blob Storage** o **Amazon S3** privados, ejecuta localmente el motor de diagnóstico puro de Culprit y publica incidentes (`CulpritIncidentsV1`), hallazgos (`CulpritFindingsV1`), cambios de esquema o ajustes (`CulpritChangesV1`) y 13 tablas de evidencia sin procesar en **Azure Data Explorer (ADX)** o **ClickHouse**.

### 1. Capturar localmente o generar el DDL del almacén

```sh
# Captura una instantánea acotada en un búfer de salida local (no requiere almacén)
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# O genera el DDL de tablas y vistas auxiliares del almacén directamente desde la imagen de contenedor
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. Desplegar un fragmento endurecido del recopilador en Kubernetes

Las imágenes multiarquitectura (`linux/amd64` y `linux/arm64`) se publican en `ghcr.io/culpritsql/culprit-collector`, junto con un chart OCI de Helm que usa cero réplicas de forma predeterminada, aplica propiedad de escritor único mediante `StatefulSet` (`65532:65532`, sistema de archivos raíz de solo lectura) y separa las credenciales de origen, recuperación y almacén:

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. Investigar en Culprit Fleet, SQL Studio o la CLI

Explora incidentes de toda la flota, ventanas de tiempo, líneas de tiempo de consultas, planes de ejecución y diferencias de configuración directamente desde ADX o ClickHouse, sin ninguna conexión activa a tus bases de datos de producción:

- **Consola web:** **[fleet.culpritsql.com](https://fleet.culpritsql.com)** (o aloja tu propia instancia de `culprit-archive-web`)
- **Aplicación de escritorio:** abre **Archivos** en la barra lateral de Culprit SQL Studio
- **CLI y MCP:** `culprit resources`, `culprit window`, `culprit incident`, `culprit collect queries` (paneles y reglas de alerta de Grafana) y `culprit mcp --warehouse-evidence`

[Arquitectura del recopilador y del almacén →](../collector.md) · [Chart de Helm para Kubernetes →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## Agentes de programación y CI

### MCP y configuración del repositorio

El servidor MCP da acceso a Claude Code, Cursor, Codex y otros clientes MCP a hallazgos, planes de consultas, esquema, migraciones, bloqueos, esperas, recomendaciones de índices y verificación. Los agentes usan nombres de conexiones guardadas; las credenciales permanecen en Culprit.

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

El segundo comando agrega instrucciones de repositorio y la configuración de MCP. Otros clientes pueden registrar `culprit` directamente con el argumento `mcp`. Cuando los comentarios de las consultas contienen referencias `file:line`, incluidas anotaciones de Marginalia y SqlCommenter, los resúmenes de los hallazgos pueden enlazar directamente al código de la aplicación.

Imprime el resumen de un hallazgo o prueba un índice propuesto en PostgreSQL mediante HypoPG sin crear el índice real:

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

El acceso se configura por conexión: desactivado, estadísticas y planes estimados (el valor predeterminado), o consultas de solo lectura aprobadas. La aprobación de consultas muestra la instrucción exacta; las llamadas a herramientas quedan registradas en un registro de auditoría local.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="Aprobación de consulta de un agente que muestra el SQL exacto, la conexión, los límites de filas y tiempo, y controles para denegar o ejecutar una vez" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[Configuración de agentes, permisos y detalles de auditoría →](../agents.md)

### Comprobaciones de migraciones y flujos de despliegue

Comprueba los archivos de migración frente a los tamaños de tablas, tipos de columnas, índices y carga de trabajo registrada de una instantánea guardada. Los hallazgos cubren operaciones `CREATE INDEX` y `ALTER TABLE` que provocan bloqueos, índices de claves foráneas que faltan o no coinciden, y cambios `DROP` o `RENAME` que afectan a consultas registradas.

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

Usa `--fail-on high` para hacer fallar el pipeline de CI ante hallazgos de gravedad alta. `--write` aplica reescrituras compatibles a los archivos de migración. JSON facilita la integración con scripts, SARIF se integra con herramientas de revisión de código, y los identificadores estables de hallazgos permiten seguir el mismo problema a lo largo de varios escaneos.

Tras el despliegue, marca la línea de tiempo, verifica un hallazgo o genera un resumen con valores literales ocultados:

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## Uso en producción

- **Los escaneos leen vistas de estadísticas y de catálogo**, con tiempos de espera agotados y protecciones de planificación específicos de cada proveedor.
- **Las correcciones de base de datos son sugerencias** para que las revises y apliques.
- **La ejecución en producción requiere confirmación** para las instrucciones que Culprit no puede clasificar como lecturas; las operaciones destructivas requieren una confirmación adicional.
- **El acceso de agentes a consultas es opcional**, con aprobación para cada instrucción de solo lectura y un registro de auditoría local.
- **Las contraseñas guardadas usan el llavero del sistema.** Entra ID puede usar tu inicio de sesión de Azure; las exportaciones compartibles ocultan los valores literales de forma predeterminada.

Usa un inicio de sesión de base de datos con los permisos que necesite tu flujo de trabajo. La [guía de seguridad](../guide.md#safety) y la [documentación de proveedores](../parity.md) explican los límites de recopilación, planificación y ejecución.

## Desarrollo

Culprit está construido con Rust, Tauri y React. La aplicación, la CLI y el servidor MCP comparten un único motor de análisis. Agrega comprobaciones personalizadas o proveedores de bases de datos mediante la API de Rust sin necesidad de bifurcar el código; los [catálogos de idiomas](../localization.md) suministran el texto visible para el usuario.

<details>
<summary>Compilar desde el código fuente</summary>

Instala [Rust estable](https://rustup.rs) y [Bun](https://bun.sh). En Linux también se necesitan las [dependencias de la plataforma](../guide.md#platforms).

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# Aplicación de escritorio
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## Más información

- **[Guía](../guide.md):** cada comprobación y comando, configuración de agentes, velocidad medida, detalles de seguridad y cómo ampliar Culprit
- **[Recopilador y almacenes](../collector.md):** recopilación en segundo plano, esquemas de ADX y ClickHouse, recuperación en Azure Blob/S3 y [Culprit Fleet](https://fleet.culpritsql.com)
- **[Arquitectura](../ARCHITECTURE.md):** la canalización de análisis, las reglas de evidencia que sigue cada comprobación y el mapa de crates
- **[Diseño](../DESIGN.md):** cómo se comporta el producto y por qué
- **[Agentes de programación](../agents.md):** herramientas MCP, permisos, aprobaciones y el registro de auditoría
- **[Paridad entre motores](../parity.md):** comparativa de funciones, guías de proveedores, configuración y versiones verificadas

Culprit es software de código abierto bajo la [licencia MIT](../../LICENSE).

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>Hecho para quienes reciben la alerta cuando la base de datos va lenta.</sub>
</p>
