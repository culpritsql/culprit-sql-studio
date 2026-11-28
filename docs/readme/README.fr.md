<p align="center">
  <img src="../images/logo.svg" width="72" height="72" alt="Culprit SQL Studio">
</p>

<h1 align="center">Culprit SQL Studio</h1>

<p align="center">
  <strong>Identifiez les goulets d’étranglement de vos bases de données, avec les preuves pour les corriger.</strong><br>
  Une application de bureau et une CLI d’analyse des performances de bases de données pour macOS, Linux et Windows.
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
  <img alt="macOS, Linux et Windows" src="https://img.shields.io/badge/macOS%20%C2%B7%20Linux%20%C2%B7%20Windows-1b1a18?style=flat-square">
  <a href="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/culpritsql/culprit-sql-studio/actions/workflows/ci.yml/badge.svg"></a>
  <a href="../../LICENSE"><img alt="Licence MIT" src="https://img.shields.io/badge/license-MIT-ffcf33?style=flat-square"></a>
</p>

<p align="center">
  <a href="https://github.com/culpritsql/culprit-sql-studio/releases/latest"><strong>Télécharger</strong></a> ·
  <a href="#get-started">Démarrage rapide</a> ·
  <a href="#what-you-get">Fonctionnalités</a> ·
  <a href="#supported-databases">Fournisseurs</a> ·
  <a href="#collector-and-culprit-fleet">Collecteur et Fleet</a> ·
  <a href="#coding-agents-and-ci">Agents et CI</a> ·
  <a href="../guide.md">Guide</a><br>
  <a href="https://culpritsql.com">culpritsql.com</a> ·
  <a href="https://fleet.culpritsql.com">Démo Fleet en direct</a> ·
  <a href="https://explain.culpritsql.com">Analyseur de plans &amp; .xel</a>
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
    <strong>Français</strong> ·
    <a href="README.pt-BR.md">Português</a>
  </sub>
</p>

Culprit réunit les statistiques de requêtes, les plans d’exécution, les attentes et le contexte du schéma dans un même flux de travail. Il classe les constats par impact et relie chacun d’eux aux éléments recueillis, à un correctif suggéré et à des étapes de vérification. Utilisez-le depuis l’application de bureau, le terminal ou un agent de code.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="Tableau de bord de base de données avec chronologie, constats classés, requêtes coûteuses, attentes et activité en cours" src="../images/dashboard-light.png">
</picture>

<a id="get-started"></a>
## Démarrage rapide

[Téléchargez l’application de bureau ou la CLI autonome](https://github.com/culpritsql/culprit-sql-studio/releases/latest). Dans l’application, ajoutez une connexion ou ouvrez un fichier SQLite pour afficher son tableau de bord.

Depuis le terminal :

```sh
culprit conn add shop --env staging --url 'postgres://app@db.example.com:5432/shop'
culprit scan shop
culprit dashboard shop
```

La réouverture d’une connexion affiche son rapport enregistré pendant qu’une nouvelle analyse s’exécute en arrière-plan. L’application de bureau propose des mises à jour signées ; mettez à jour la CLI autonome avec `culprit update`.

<details>
<summary>Essayer l’interface avec des données de démonstration</summary>

Avec [Bun](https://bun.sh) installé :

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio/app
bun install --frozen-lockfile
bun run dev
```

La démo dans le navigateur utilise des instantanés inclus et ne nécessite aucune connexion à une base de données.

</details>

<a id="supported-databases"></a>
## Bases de données prises en charge

| Fournisseur | Sources et capacités |
|---|---|
| [**SQL Server / Azure SQL**](../providers/mssql.md) | Query Store, cache des plans, DMV et événements d’interblocage conservés |
| [**PostgreSQL / Amazon RDS**](../providers/postgres.md) | `pg_stat_statements`, vues de catalogue et de statistiques, EXPLAIN et index hypothétiques via HypoPG |
| [**TimescaleDB**](../providers/timescale.md) | Sources PostgreSQL, tailles des hypertables et chunks gérés ou compressés |
| [**YugabyteDB**](../providers/yugabyte.md) | Compteurs natifs YSQL/DocDB, plans DIST, sharding et sources d’activité ciblées |
| [**MySQL**](../providers/mysql.md) | `performance_schema`, `sys`, EXPLAIN et le journal des requêtes lentes |
| [**MariaDB**](../providers/mariadb.md) | Performance Schema, journal des requêtes lentes, statistiques utilisateur, plans MariaDB et vues de verrous |
| [**ClickHouse**](../providers/clickhouse.md) | Requêtes initiales journalisées (`system.query_log`), parts et fusions MergeTree, mutations, réplication et `EXPLAIN PLAN` |
| [**SQLite**](../providers/sqlite.md) | Inspection de fichier en lecture seule, `EXPLAIN QUERY PLAN` et journaux de requêtes des frameworks |

Les fonctionnalités dépendent du moteur, des sources activées et des autorisations de votre identifiant. Les sources manquantes s’affichent avec leur motif et des instructions de configuration. Consultez [Parité des moteurs](../parity.md) pour le comparatif des fonctionnalités et les versions vérifiées.

<a id="what-you-get"></a>
## Fonctionnalités

### Constats avec preuves, correctifs et vérification

Chaque constat identifie les requêtes affectées, la période de mesure et les éléments qui justifient sa gravité. Ouvrez-le pour consulter le changement suggéré, le code SQL à copier le cas échéant, les étapes de vérification et une synthèse à partager avec un collègue ou un agent de code.

| Domaine | Exemples de contrôles effectués par Culprit |
|---|---|
| Performances des requêtes | Jointures coûteuses, régressions de plan, plans sensibles aux paramètres, conversions de types bloquant les index, motifs N+1, échecs, délais d’attente dépassés et débordements sur disque |
| Index et schéma | Index manquants, inutilisés, en double ou redondants ; clés étrangères non indexées ; pièges de partitionnement |
| Maintenance et capacité | Statistiques obsolètes, lignes mortes, bloat des tables, lignes volumineuses, épuisement des séquences et risque de bouclage des identifiants de transaction |
| Santé de la base de données | Lignes chaudes, escalade de verrous de table, latence de stockage, corruption des sommes de contrôle de pages, instabilité des connexions, retard de réplication et paramètres |

Par exemple, la démo SQL Server inclut un plan d’exécution effectuant **1,5 milliard de comparaisons de lignes**. La requête affectée représente **27 % du temps de base de données** sur cinq heures. Culprit identifie son indicateur de jointure de boucles forcée, suggère de le retirer et détaille les étapes pour comparer le nouveau plan et vérifier le résultat.

Exécutez `culprit checks` pour afficher la liste complète des contrôles et les capacités requises.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/findings-dark.png">
  <img alt="Un constat avec les éléments sur la requête, l’étape concernée du plan d’exécution, un correctif suggéré et des étapes de vérification" src="../images/findings-light.png">
</picture>

### Plans d’exécution et mesures à l’exécution

Explorez les plans d’exécution sous forme de diagramme, d’arborescence indentée ou de table d’opérations. Les diagnostics du plan expliquent les recherches de clé coûteuses, les erreurs d’estimation de lignes, les jointures inefficaces, les débordements sur disque, les conversions, les allocations de mémoire et le parallélisme, avec des liens vers les opérateurs concernés.

Examinez le nombre de lignes estimé et réel, les propriétés des opérateurs ainsi que le XML ou JSON brut. Mesurez explicitement les instructions de lecture prises en charge. Sur SQL Server, consultez le dernier plan réel conservé sans réexécuter la requête, ou suivez la progression par opérateur d’une requête en cours lorsque ces éléments sont disponibles.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/queries-dark.png">
  <img alt="Requêtes classées par temps de base de données, avec le plan d’exécution d’une requête sélectionnée, les diagnostics et les détails des opérateurs" src="../images/queries-light.png">
</picture>

### Ce qui a changé après un déploiement

Faites glisser le curseur sur le graphique du temps de base de données pour comparer deux périodes : requêtes ralenties, nouvelles requêtes, améliorations et instructions qui ne s’exécutent plus. Visualisez les changements de plan, les modifications de schéma ou de paramètres, les redémarrages et les repères de déploiement en regard de ces évolutions.

Les métriques cloud ajoutent du contexte, comme les crédits CPU Amazon RDS, les limites d’E/S et les limites de niveau de service Azure, à partir de votre connexion `aws` ou `az` existante.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/what-changed-dark.png">
  <img alt="Une comparaison de périodes montrant les requêtes ralenties, nouvelles et améliorées avec l’évolution du temps de base de données et du nombre d’exécutions" src="../images/what-changed-light.png">
</picture>

### Comparer et partager des plans d’exécution

Collez, déposez ou ouvrez un plan d’exécution ou une capture d’événements provenant d’un autre outil — ou examinez-les dans le navigateur sur [explain.culpritsql.com](https://explain.culpritsql.com) : fichiers SQL Server `.sqlplan` et `.xel`, sortie EXPLAIN PostgreSQL en texte ou JSON, sortie DIST YugabyteDB, plans MySQL/MariaDB ou `EXPLAIN PLAN json=1` ClickHouse. Comparez deux versions grâce à des diagrammes synchronisés mettant en évidence les changements d’opérateurs, de chemins d’accès, d’estimations de lignes et de coût.

Les comparaisons expliquent également les écarts de paramètres capturés, de statistiques, de parallélisme et de mémoire lorsque les plans les conservent. Partagez constats et plans d’exécution au format Markdown, sous forme de page HTML autonome ou de fichier de plan natif, avec le masquage des valeurs littérales activé par défaut.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/compare-dark.png">
  <img alt="Deux plans d’exécution côte à côte avec l’explication de leurs différences et les opérateurs modifiés mis en évidence" src="../images/compare-light.png">
</picture>

### Sessions en direct, blocages et interblocages

Remontez les chaînes de blocage jusqu’à la session qui détient les verrous. Examinez les attentes, les instructions longues et les transactions laissées ouvertes par l’application. Les interblocages enregistrés incluent les instructions impliquées, le schéma d’interblocage et les actions recommandées.

L’activité en direct s’actualise automatiquement et peut être mise en pause. Sur les connexions hors production, annulez une instruction ou mettez fin à une session depuis l’application ou la CLI.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/live-dark.png">
  <img alt="Sessions en direct avec une transaction ouverte et les sessions bloquées en attente derrière elle" src="../images/live-light.png">
</picture>

### Édition SQL et outillage de base de données

| Fonctionnalité | Contenu |
|---|---|
| **Conseiller d’index** | Candidats d’index fondés sur la charge de travail, index en double, redondants ou inutilisés, et coût en écriture de leur maintien |
| **Éditeur SQL** | Onglets persistants, autocomplétion sensible au schéma, requêtes enregistrées, plans estimés et mesurés, et avertissements de migration intégrés avec correctifs rapides |
| **Analyseur de migrations** | Contrôles hors ligne sur des instantanés enregistrés ; réécritures DDL prises en charge pour des opérations concurrentes ou en ligne |
| **Où passe le temps** | Catégories d’attente reliées aux requêtes, avec un échantillon en direct à comparer à la charge de travail enregistrée |
| **Signaux de santé** | Mémoire, compilations, connexions, verrous de lignes, latence de stockage par fichier, réplication et limites cloud |
| **Explorateur de schéma** | Tailles des tables, index et utilisation ; comparaison des colonnes, index, clés étrangères, déclencheurs, contraintes et bornes de partitions entre environnements, avec le SQL pour réconcilier les écarts |
| **Connexions** | Chaînes de connexion et importations depuis Azure Data Studio, VS Code, DBeaver et DataGrip |
| **Découverte et SSH** | Détection des bases Azure et AWS via votre connexion CLI ; connexion via un [hôte de rebond SSH](../ssh.md) |
| **Rapports hors ligne** | Analyses enregistrées, tendances, comparaisons historiques et instantanés partagés consultables sans connexion à la base de données |
| **Archives** | Investigation en lecture seule des incidents, périodes et plans d’exécution sur des entrepôts de télémétrie Azure Data Explorer (ADX) et ClickHouse publiés |
| **Navigation au clavier** | Palette de commandes (⌘K / Ctrl+K), raccourcis de vues, onglets de requêtes et actions |
| **Langues** | Anglais (English), chinois simplifié (简体中文), japonais (日本語), coréen (한국어), russe (Русский), allemand (Deutsch), espagnol (Español), français (Français) et portugais (Português) |

<a id="collector-and-culprit-fleet"></a>
## Collecteur et Culprit Fleet

Exécutez `culprit collect` comme collecteur en arrière-plan borné et en lecture seule afin de surveiller vos bases de données en continu sans connecter d’utilisateurs interactifs ni d’agents à la production. Le collecteur écrit des captures scellées dans un tampon de sortie local durable, sauvegarde les éléments bruts dans un stockage privé **Azure Blob Storage** ou **Amazon S3**, exécute localement le moteur de diagnostic pur de Culprit, et publie les incidents (`CulpritIncidentsV1`), les constats (`CulpritFindingsV1`), les changements de schéma et de paramètres (`CulpritChangesV1`) ainsi que 13 tables d’éléments bruts dans **Azure Data Explorer (ADX)** ou **ClickHouse**.

### 1. Capturer localement ou générer le DDL de l’entrepôt

```sh
# Capturer un instantané borné dans un tampon de sortie local (sans entrepôt requis)
culprit collect outbox init ./outbox
culprit collect once shop --collection activity --outbox ./outbox

# Ou générer le DDL des tables et vues auxiliaires de l'entrepôt depuis l'image conteneur
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.4 \
  collect schema --store clickhouse --format sql > clickhouse-schema.sql
```

### 2. Déployer un shard de collecteur durci sur Kubernetes

Des images multi-architectures (`linux/amd64` et `linux/arm64`) sont publiées sur `ghcr.io/culpritsql/culprit-collector`, accompagnées d’un chart Helm OCI configuré par défaut à zéro réplica, qui impose un propriétaire unique en écriture via `StatefulSet` (`65532:65532`, système de fichiers racine en lecture seule) et sépare les identifiants des sources, de la sauvegarde et de l’entrepôt :

```sh
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.4 \
  --namespace db-monitoring \
  --create-namespace \
  -f values.yaml
```

### 3. Investiguer dans Culprit Fleet, SQL Studio ou la CLI

Explorez les incidents à l’échelle de la flotte, les périodes, les chronologies de requêtes, les plans d’exécution et la dérive de configuration directement depuis ADX ou ClickHouse — sans aucune connexion active à vos bases de données de production :

- **Console web :** **[fleet.culpritsql.com](https://fleet.culpritsql.com)** (ou auto-hébergez `culprit-archive-web`)
- **Application de bureau :** ouvrez **Archives** dans la barre latérale de Culprit SQL Studio
- **CLI et MCP :** `culprit resources`, `culprit window`, `culprit incident`, `culprit collect queries` (tableaux de bord et règles d’alerte Grafana) et `culprit mcp --warehouse-evidence`

[Architecture du collecteur et des entrepôts →](../collector.md) · [Chart Helm Kubernetes →](../../deploy/helm/culprit-collector/README.md)

<a id="coding-agents-and-ci"></a>
## Agents de code et CI

### MCP et configuration du dépôt

Le serveur MCP donne à Claude Code, Cursor, Codex et aux autres clients MCP accès aux constats, plans d’exécution, schémas, migrations, blocages, attentes, conseils d’index et vérifications. Les agents utilisent les noms des connexions enregistrées ; les identifiants restent dans Culprit.

```sh
claude mcp add culprit -- culprit mcp
culprit agent init --connection shop
```

La seconde commande ajoute les instructions du dépôt et la configuration MCP. Les autres clients peuvent enregistrer directement `culprit` avec l’argument `mcp`. Lorsque les commentaires de requêtes contiennent des références `file:line`, notamment les annotations Marginalia et SqlCommenter, les synthèses des constats renvoient directement au code applicatif.

Affichez la synthèse d’un constat ou testez un index PostgreSQL proposé via HypoPG sans créer l’index réel :

```sh
culprit show shop 'filtered-scan:public.orders'
culprit explain shop --sql "SELECT * FROM orders WHERE status = 'open'" \
  --with-index "CREATE INDEX ON orders (status ASC)"
```

L’accès se configure par connexion : désactivé, statistiques et plans estimés (par défaut), ou requêtes en lecture seule approuvées. L’approbation d’une requête affiche l’instruction exacte ; les appels d’outils sont consignés dans un journal d’audit local.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/agent-approval-dark.png">
  <img alt="Approbation d’une requête d’agent affichant le code SQL exact, la connexion, les limites de lignes et de durée, ainsi que les boutons pour refuser ou exécuter une fois" src="../images/agent-approval-light.png" width="720">
</picture>
</p>

[Configuration des agents, autorisations et journal d’audit →](../agents.md)

### Contrôles de migrations et flux de déploiement

Vérifiez les fichiers de migration par rapport aux tailles de tables, types de colonnes, index et charges de travail enregistrées d’un instantané. Les constats couvrent les opérations `CREATE INDEX` et `ALTER TABLE` bloquantes, les index de clés étrangères manquants ou incompatibles, ainsi que les modifications `DROP` ou `RENAME` qui affectent des requêtes enregistrées.

```sh
culprit lint-sql prod-snapshot.json --file migrations/ --fail-on high
culprit lint-sql prod-snapshot.json --file migrations/ --format sarif > culprit.sarif
```

Utilisez `--fail-on high` pour faire échouer la CI en cas de constats de gravité élevée. `--write` applique les réécritures prises en charge aux fichiers de migration. Le format JSON s’intègre aux scripts, le format SARIF aux outils de revue de code, et les identifiants stables des constats permettent de suivre un même problème d’une analyse à l’autre.

Après un déploiement, placez un repère sur la chronologie, vérifiez un constat ou générez un résumé expurgé :

```sh
culprit mark shop deploy v2.4.0
culprit verify shop 'filtered-scan:public.orders'
culprit digest shop > digest.json
```

## Utilisation en production

- **Les analyses lisent les vues de statistiques et de catalogue**, avec des délais d’attente et des garde-fous de planification propres à chaque fournisseur.
- **Les correctifs de base de données sont des suggestions** à relire et appliquer vous-même.
- **L’exécution en production demande une confirmation** pour toute instruction que Culprit ne peut pas classer comme une lecture ; les opérations destructrices exigent une confirmation supplémentaire.
- **L’accès aux requêtes pour les agents est explicite**, avec une approbation pour chaque instruction en lecture seule et une piste d’audit locale.
- **Les mots de passe enregistrés utilisent le trousseau du système.** Entra ID peut réutiliser votre connexion Azure ; les exportations partageables masquent les valeurs littérales par défaut.

Utilisez un identifiant de base de données disposant des autorisations nécessaires à votre flux de travail. Le [guide de sécurité](../guide.md#safety) et la [documentation des fournisseurs](../parity.md) détaillent les limites de collecte, de planification et d’exécution.

## Développement

Culprit est développé avec Rust, Tauri et React. L’application, la CLI et le serveur MCP partagent un seul moteur d’analyse. Ajoutez vos propres contrôles ou fournisseurs de bases de données via l’API Rust sans faire de fork ; les [catalogues de langues](../localization.md) fournissent l’ensemble des textes affichés à l’utilisateur.

<details>
<summary>Compiler depuis les sources</summary>

Installez [Rust stable](https://rustup.rs) et [Bun](https://bun.sh). Sous Linux, installez également les [dépendances système](../guide.md#platforms).

```sh
git clone https://github.com/culpritsql/culprit-sql-studio.git
cd culprit-sql-studio

# CLI
cargo build --locked --release -p culprit-cli

# Application de bureau
cd app
bun install --frozen-lockfile
bunx tauri build
```

</details>

## En savoir plus

- **[Guide](../guide.md) :** tous les contrôles et commandes, la configuration des agents, les performances mesurées, les détails de sécurité et comment étendre Culprit
- **[Collecteur et entrepôts](../collector.md) :** collecte en arrière-plan, schémas ADX et ClickHouse, sauvegarde Azure Blob/S3 et [Culprit Fleet](https://fleet.culpritsql.com)
- **[Architecture](../ARCHITECTURE.md) :** le pipeline d’analyse, les règles de preuve suivies par chaque contrôle et la cartographie des crates
- **[Conception](../DESIGN.md) :** le comportement du produit et ses raisons
- **[Agents de code](../agents.md) :** outils MCP, autorisations, approbations et journal d’audit
- **[Parité des moteurs](../parity.md) :** comparatif des fonctionnalités, guides des fournisseurs, configuration et versions vérifiées

Culprit est un logiciel open source distribué sous [licence MIT](../../LICENSE).

<br>

<p align="center">
  <img src="../images/logo.svg" width="28" height="28" alt=""><br>
  <sub>Conçu pour celles et ceux qui reçoivent l’alerte quand la base de données ralentit.</sub>
</p>
