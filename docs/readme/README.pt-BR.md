<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>Encontre gargalos no banco de dados, com as evidências para corrigi-los.</strong><br>
  Aplicativo desktop e CLI para análise de desempenho de bancos de dados no macOS, Linux e Windows.
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
  <img alt="macOS, Linux e Windows" src="https://img.shields.io/badge/macOS%20%C2%B7%20Linux%20%C2%B7%20Windows-1b1a18?style=flat-square">
  <a href="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml/badge.svg"></a>
  <a href="../../LICENSE"><img alt="Licença MIT" src="https://img.shields.io/badge/license-MIT-ffcf33?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>Baixar</strong></a> ·
  <a href="#get-started">Início rápido</a> ·
  <a href="#what-you-get">Recursos</a> ·
  <a href="#supported-databases">Provedores</a> ·
  <a href="#collector-and-culprit-fleet">Coletor e Fleet</a> ·
  <a href="#coding-agents-and-ci">Agentes e CI</a> ·
  <a href="../guide.md">Guia</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">Demo Fleet ao vivo</a> ·
  <a href="https://explain.culpritsql.com">Analisador de planos e .xel</a>
</p>

<p align="center">
  <sub>
    <a href="../../README.md">English</a> ·
    <a href="README.zh-Hans.md">简体中文</a> ·
    <a href="README.ja.md">日本語</a> ·
    <a href="README.ko.md">한국어</a> ·
    <a href="README.ru.md">Русский</a> ·
    <a href="README.de.md">Deutsch</a> ·
    <a href="README.es.md">Español</a> ·
    <a href="README.fr.md">Français</a> ·
    <strong>Português</strong>
  </sub>
</p>

O Culprit reúne estatísticas de consultas, planos de execução, esperas e contexto de esquema em um único fluxo de trabalho. Ele classifica os achados por impacto e conecta cada um às evidências, a uma correção sugerida e às etapas de verificação. Use pelo aplicativo desktop, pelo terminal ou por um agente de código.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="Painel do banco de dados com linha do tempo, achados classificados, consultas custosas, esperas e atividade atual" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## Primeiros passos

[Baixe o aplicativo desktop ou a CLI independente](https://github.com/culpritsql/culprit-sql-studio/releases/latest). No aplicativo, adicione uma conexão ou abra um arquivo SQLite para ver o painel.

Pelo terminal:

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

Ao reabrir uma conexão, o relatório salvo é exibido enquanto uma nova varredura roda em segundo plano. O aplicativo desktop oferece atualizações assinadas; atualize a CLI independente com `culprit update`.

<details>
<summary>Testar a interface com dados de demonstração</summary>

Com o [Bun](https://bun.sh) instalado:

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

A demonstração no navegador usa snapshots incluídos e não requer conexão com banco de dados.

</details>

<a id="supported-databases"></a>
## Bancos de dados compatíveis

| Provedor | Fontes e capacidades |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store, cache de planos, DMVs e eventos retidos de deadlock |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`, views de catálogo e estatísticas, EXPLAIN e índices hipotéticos via HypoPG |
| [**TimescaleDB**](../providers/timescale.md) | Fontes do PostgreSQL mais tamanhos de hypertables e chunks gerenciados/comprimidos |
| [**YugabyteDB**](../providers/yugabyte.md) | Contadores nativos YSQL/DocDB, planos DIST, sharding e fontes de atividade com escopo |
| [**MySQL**](../providers/mysql.md) | `performance_schema`, `sys`, EXPLAIN e slow query log |
| [**MariaDB**](../providers/mariadb.md) | Performance Schema, slow log, estatísticas de usuário, planos do MariaDB e views de bloqueio |
| [**ClickHouse**](../providers/clickhouse.md) | Consultas iniciais registradas (`system.query_log`), parts/merges de MergeTree, mutações, replicação e `EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | Inspeção somente leitura de arquivo, `EXPLAIN QUERY PLAN` e logs de consultas de frameworks |

Os recursos disponíveis dependem do mecanismo, das fontes habilitadas e das permissões do seu login. Fontes ausentes aparecem com o motivo e orientações de configuração. Consulte [Paridade entre mecanismos](../parity.md) para ver a comparação de recursos e as versões verificadas.

<a id="what-you-get"></a>
## O que o Culprit oferece

### Achados com evidências, correções e verificação

Cada achado identifica as consultas afetadas, a janela de medição e as evidências por trás da severidade. Abra o achado para ver a alteração sugerida, o SQL pronto para copiar quando aplicável, as etapas de verificação e um resumo técnico para compartilhar com a equipe ou com um agente de código.

| Área | Exemplos do que o Culprit verifica |
|---|---|
| Desempenho de consultas | Joins custosos, regressões de plano, planos sensíveis a parâmetros, conversões de tipo que impedem o uso de índices, padrões N+1, falhas, timeouts e spills |
| Índices e esquema | Índices ausentes, não utilizados, duplicados e sobrepostos; chaves estrangeiras sem índice; riscos de particionamento |
| Manutenção e capacidade | Estatísticas desatualizadas, tuplas mortas, bloat de tabela, linhas grandes, esgotamento de sequências e risco de wraparound de ID de transação |
| Saúde do banco de dados | Linhas quentes, escalonamento de bloqueio de tabela, latência de armazenamento, corrupção de checksum de página, rotatividade de conexões, atraso de replicação e configurações |

Por exemplo, a demonstração do SQL Server inclui um plano que realiza **1,5 bilhão de comparações de linhas**. A consulta afetada responde por **27% do tempo de banco de dados** em cinco horas. O Culprit identifica o hint forçado de loop join, sugere removê-lo e indica as etapas para comparar o novo plano e verificar o resultado.

Execute `culprit checks` para ver a lista completa de verificações e as capacidades necessárias.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="Um achado com evidências da consulta, etapa relevante do plano, correção sugerida e etapas de verificação" src="../images/findings-light.png">
</picture>

### Planos de execução e evidências em tempo de execução

Explore planos como diagrama, árvore indentada ou tabela de operações. Os insights explicam lookups custosos, erros na estimativa de linhas, joins ineficientes, spills, conversões, concessões de memória e paralelismo, com links diretos para os operadores relevantes.

Inspecione linhas estimadas e reais, propriedades dos operadores e o XML ou JSON bruto. Meça explicitamente instruções de leitura compatíveis. No SQL Server, inspecione o último plano real retido sem reexecutar a consulta, ou acompanhe o progresso dos operadores de uma consulta em andamento quando essa evidência estiver disponível.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="Consultas classificadas por tempo de banco de dados, com o plano de execução da consulta selecionada, insights e detalhes dos operadores" src="../images/queries-light.png">
</picture>

### O que mudou após um deploy

Arraste sobre o gráfico de tempo de banco de dados para comparar dois períodos: consultas que ficaram mais lentas, consultas novas, melhorias e instruções que pararam de executar. Veja trocas de plano, alterações de esquema ou configuração, reinicializações e marcações de deploy lado a lado com essas mudanças.

Métricas de nuvem adicionam contexto como créditos de CPU do Amazon RDS, limites de I/O e limites de camada do Azure, aproveitando sua sessão existente da CLI `aws` ou `az`.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="Comparação entre períodos mostrando consultas mais lentas, novas e aprimoradas, com variações no tempo de banco de dados e na contagem de execuções" src="../images/what-changed-light.png">
</picture>

### Comparação e compartilhamento de planos

Cole, arraste ou abra um plano ou captura de eventos de outra ferramenta — ou inspecione diretamente no navegador em [explain.culpritsql.com](https://explain.culpritsql.com): arquivos `.sqlplan` e `.xel` do SQL Server, texto ou JSON de EXPLAIN do PostgreSQL, saída DIST do YugabyteDB, planos do MySQL/MariaDB ou `EXPLAIN PLAN json=1` do ClickHouse. Compare duas versões com diagramas sincronizados e destaque para mudanças em operadores, caminhos de acesso, estimativas de linhas e custo.

As comparações também explicam diferenças em parâmetros capturados, estatísticas, paralelismo e memória quando retidos nos planos. Compartilhe achados e planos em Markdown, como página HTML independente ou arquivo de plano nativo, com ocultação de valores literais ativada por padrão.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="Dois planos lado a lado com explicação das diferenças e operadores alterados em destaque" src="../images/compare-light.png">
</picture>

### Sessões ativas, bloqueios e deadlocks

Siga cadeias de bloqueio até a sessão que está retendo as demais. Inspecione esperas, instruções de longa duração e transações deixadas abertas pela aplicação. Os deadlocks registrados incluem as instruções participantes, o padrão identificado e os próximos passos sugeridos.

A atividade em tempo real é atualizada automaticamente e pode ser pausada. Em conexões que não são de produção, cancele uma instrução ou encerre uma sessão pelo aplicativo ou pela CLI.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="Sessões ativas com uma transação aberta e as sessões bloqueadas aguardando atrás dela" src="../images/live-light.png">
</picture>

### Edição SQL e ferramentas de banco de dados

| Recurso | O que inclui |
|---|---|
| **Consultor de índices** | Candidatos a índice baseados na carga de trabalho, índices duplicados/sobrepostos/não utilizados e o custo de escrita para mantê-los |
| **Editor SQL** | Abas persistentes, autocompletar ciente do esquema, consultas salvas, planos estimados e medidos e avisos de migração inline com correções rápidas |
| **Linter de migrações** | Verificações offline contra snapshots salvos; reescritas de DDL compatíveis para operações concorrentes ou online |
| **Para onde vai o tempo** | Categorias de espera associadas às consultas, além de uma amostra ao vivo para comparar com a carga registrada |
| **Sinais de saúde** | Memória, compilações, conexões, bloqueios de linha, latência de armazenamento por arquivo, replicação e limites da nuvem |
| **Explorador de esquema** | Tamanhos de tabelas, índices e uso; comparação de colunas, índices, chaves estrangeiras, triggers, constraints e limites de partição entre ambientes, com SQL para reconciliar diferenças |
| **Conexões** | Strings de conexão e importação do Azure Data Studio, VS Code, DBeaver e DataGrip |
| **Descoberta e SSH** | Localização de bancos no Azure e na AWS com seu login da CLI; conexão via [jump host SSH](../ssh.md) |
| **Relatórios offline** | Varreduras salvas, tendências, comparações históricas e snapshots compartilhados que você pode inspecionar sem conexão com o banco |
| **Arquivos** | Investigação somente leitura de incidentes, janelas de tempo e planos de execução sobre warehouses de telemetria publicados no Azure Data Explorer (ADX) e no ClickHouse |
| **Navegação por teclado** | Paleta de comandos (⌘K / Ctrl+K), atalhos de visualização, abas de consulta e ações |
| **Idiomas** | Inglês (English), Chinês Simplificado (简体中文), Japonês (日本語), Coreano (한국어), Russo (Русский), Alemão (Deutsch), Espanhol (Español), Francês (Français) e Português |

<a id="collector-and-culprit-fleet"></a>
## Coletor e Culprit Fleet

Execute `culprit collect` como um coletor em segundo plano limitado e somente leitura para monitorar bancos de dados continuamente sem conectar usuários interativos ou agentes à produção. O coletor grava capturas seladas em uma outbox local durável, faz backup das evidências brutas em armazenamento privado no **Azure Blob Storage** ou **Amazon S3**, executa localmente o motor de diagnóstico puro do Culprit e publica incidentes (`CulpritIncidentsV1`), achados (`CulpritFindingsV1`), alterações de esquema/configuração (`CulpritChangesV1`) e 13 tabelas de evidências brutas no **Azure Data Explorer (ADX)** ou **ClickHouse**.

### 1. Capturar localmente ou gerar o DDL do warehouse

```sh
# Captura um snapshot limitado em uma outbox local (sem necessidade de warehouse)
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# Ou gera o DDL de tabelas e views auxiliares do warehouse diretamente da imagem de contêiner
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. Implantar um shard endurecido do coletor no Kubernetes

Imagens multiarquitetura (`linux/amd64` e `linux/arm64`) são publicadas em `ghcr.io/culpritsql/culprit-collector`, acompanhadas de um Helm chart OCI que usa zero réplicas por padrão, impõe propriedade de escritor único via `StatefulSet` (`65532:65532`, sistema de arquivos raiz somente leitura) e separa as credenciais de origem, recuperação e warehouse:

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. Investigar no Culprit Fleet, SQL Studio ou CLI

Explore incidentes em toda a frota, janelas de tempo, linhas do tempo de consultas, planos de execução e desvios de configuração diretamente no ADX ou ClickHouse — sem nenhuma conexão ativa com seus bancos de dados de produção:

- **Console web:** **[fleet.culpritsql.com](https://fleet.culpritsql.com)** (ou hospede você mesmo o `culprit-archive-web`)
- **Aplicativo desktop:** abra **Arquivos** na barra lateral do Culprit SQL Studio
- **CLI e MCP:** `culprit resources`, `culprit window`, `culprit incident`, `culprit collect queries` (painéis e regras de alerta do Grafana) e `culprit mcp --warehouse-evidence`

[Arquitetura do coletor e do warehouse →](../collector.md) · [Helm chart para Kubernetes →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## Agentes de código e CI

### MCP e configuração do repositório

O servidor MCP oferece ao Claude Code, Cursor, Codex e outros clientes MCP acesso a achados, planos de consulta, esquema, migrações, bloqueios, esperas, recomendações de índices e verificação. Os agentes usam apenas os nomes das conexões salvas; as credenciais permanecem no Culprit.

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

O segundo comando adiciona instruções ao repositório e a configuração do MCP. Outros clientes podem registrar `culprit` diretamente com o argumento `mcp`. Quando comentários de consulta contêm referências `file:line`, incluindo anotações Marginalia e SqlCommenter, os resumos dos achados podem apontar de volta para o código da aplicação.

Exiba o resumo de um achado ou teste um índice proposto no PostgreSQL via HypoPG sem criar o índice real:

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

O acesso é definido por conexão: desativado, estatísticas e planos estimados (o padrão) ou consultas somente leitura aprovadas. A aprovação de consulta mostra a instrução exata; as chamadas de ferramentas são registradas em um log de auditoria local.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="Aprovação de consulta de um agente mostrando o SQL exato, a conexão, os limites de linhas e tempo e controles para negar ou executar uma vez" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[Configuração de agentes, permissões e detalhes de auditoria →](../agents.md)

### Verificações de migração e fluxos de deploy

Verifique arquivos de migração contra tamanhos de tabelas, tipos de colunas, índices e a carga de trabalho registrada em um snapshot salvo. Os achados cobrem operações bloqueantes de `CREATE INDEX` e `ALTER TABLE`, índices de chave estrangeira ausentes ou incompatíveis e alterações `DROP` ou `RENAME` que afetam consultas registradas.

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

Use `--fail-on high` para reprovar o CI em achados de alta severidade. `--write` aplica reescritas compatíveis aos arquivos de migração. O formato JSON atende scripts, o SARIF integra-se a ferramentas de revisão de código, e IDs estáveis de achados permitem acompanhar o mesmo problema entre varreduras.

Após o deploy, marque a linha do tempo, verifique um achado ou gere um resumo com dados sensíveis ocultados:

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## Uso em produção

- **As varreduras leem apenas views de estatísticas e catálogo**, com timeouts específicos por provedor e proteções de planejamento.
- **As correções de banco de dados são sugestões** para você revisar e aplicar.
- **A execução em produção exige confirmação** para instruções que o Culprit não consegue classificar como leitura; operações destrutivas exigem uma confirmação adicional.
- **O acesso de consultas por agentes é opcional (opt-in)**, com aprovação individual para cada instrução somente leitura e trilha de auditoria local.
- **As senhas salvas usam o chaveiro do sistema operacional.** O Entra ID pode usar sua sessão do Azure; exportações compartilháveis ocultam valores literais por padrão.

Use um login de banco de dados apenas com as permissões necessárias ao seu fluxo de trabalho. O [guia de segurança](../guide.md#safety) e a [documentação de provedores](../parity.md) detalham os limites de coleta, planejamento e execução.

## Desenvolvimento

O Culprit é construído com Rust, Tauri e React. O aplicativo, a CLI e o servidor MCP compartilham um único motor de análise. Adicione verificações personalizadas ou provedores de banco de dados pela API em Rust sem precisar fazer fork; os [catálogos de idiomas](../localization.md) fornecem todo o texto exibido ao usuário.

<details>
<summary>Compilar a partir do código-fonte</summary>

Instale o [Rust stable](https://rustup.rs) e o [Bun](https://bun.sh). No Linux, instale também as [dependências da plataforma](../guide.md#platforms).

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# Aplicativo desktop
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## Saiba mais

- **[Guia](../guide.md):** todas as verificações e comandos, configuração de agentes, velocidade medida, detalhes de segurança e como estender o Culprit
- **[Coletor e warehouses](../collector.md):** coleta em segundo plano, esquemas de ADX e ClickHouse, recuperação no Azure Blob/S3 e [Culprit Fleet](https://fleet.culpritsql.com)
- **[Arquitetura](../ARCHITECTURE.md):** o pipeline de análise, as regras de evidência que toda verificação segue e o mapa de crates
- **[Design](../DESIGN.md):** como o produto se comporta e por quê
- **[Agentes de código](../agents.md):** ferramentas MCP, permissões, aprovações e log de auditoria
- **[Paridade entre mecanismos](../parity.md):** comparação de recursos, guias de provedores, configuração e versões verificadas

O Culprit é código aberto sob a [licença MIT](../../LICENSE).

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>Feito para quem recebe o alerta quando o banco de dados fica lento.</sub>
</p>
