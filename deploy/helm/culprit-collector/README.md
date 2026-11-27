# Culprit Collector Helm Chart

Deploys one hardened, non-root Culprit collector shard (`StatefulSet`) for bounded, read-only database telemetry acquisition, pure local synthesis, private Azure Blob or S3 recovery, and verified delivery to Azure Data Explorer (ADX) or ClickHouse.

## Quick Start

### 1. Generate warehouse schema DDL from the collector image

No local Rust or CLI installation is needed—the published multi-arch container image (`linux/amd64` + `linux/arm64`) is the CLI:

```sh
docker run --rm ghcr.io/culpritsql/culprit-collector:v0.6.3 \
  collect schema --store adx --format json > adx-schema.json
```

Apply the generated table and helper DDL to your existing ADX or ClickHouse database.

### 2. Create `values.yaml` (Azure AKS Workload Identity + ADX example)

```yaml
replicaCount: 0 # Set to 1 with activation.sourceQuiescent=true once initialized

activation:
  sourceQuiescent: false
  initNamespaceJob:
    enabled: true # Creates the recovery namespace once; set back to false afterward

image:
  repository: ghcr.io/culpritsql/culprit-collector
  tag: v0.6.3

serviceAccount:
  create: false
  name: culprit-sa

azureWorkloadIdentity:
  enabled: true
  tenantId: "00000000-0000-0000-0000-000000000000"
  clientId: "11111111-1111-1111-1111-111111111111"

connections:
  orders:
    engine: mssql
    environment: production
    host: orders-sql.database.windows.net
    port: 1433
    database: orders_db
    auth: entra
    options:
      encrypt: "True"

collector:
  pipeline: database-telemetry
  shard: orders
  inventory:
    targets:
      - id: orders-db
        connection: orders
        subject:
          kind: database
        shard: orders
        admission_group: orders-elastic-pool
  sourceCredentials:
    orders: source
  recovery:
    provider: azure-blob
    options:
      account: mystorageaccount
      container: culprit-native
      prefix: database-telemetry/orders
    credential: recovery
  target:
    kind: adx
    query:
      endpoint: https://mycluster.westeurope.kusto.windows.net
      database: culprit
    credential: analytics
    delivery:
      credential: ingestion

persistence:
  storageClassName: managed-csi
  size: 10Gi
```

### 3. Install from GHCR OCI Registry

```sh
# Step 1: Deploy ConfigMap + one-shot namespace init Job (replicaCount=0)
helm upgrade --install culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.3 \
  --namespace db-monitoring \
  -f values.yaml

# Step 2: Activate the collector shard once init completes and source quiescence is confirmed
helm upgrade culprit-collector \
  oci://ghcr.io/culpritsql/charts/culprit-collector \
  --version 0.6.3 \
  --namespace db-monitoring \
  -f values.yaml \
  --set activation.initNamespaceJob.enabled=false \
  --set replicaCount=1 \
  --set activation.sourceQuiescent=true
```

## Safety Guarantees

- **Single owner per shard:** `replicaCount` is schema-bounded to `0` or `1`, `StatefulSet` uses `OnDelete` updates, and PVC uses `ReadWriteOncePod` with `Retain` policy.
- **Explicit activation gate:** Setting `replicaCount=1` fails template rendering unless either `activation.sourceQuiescent=true` or `activation.deliveryOnly=true` is explicitly provided.
- **No secrets in ConfigMaps:** Rendering fails if `connections` contains inline `password`, `token`, or `secret` keys. Entra tokens use direct projected federation (`api://AzureADTokenExchange`), and `password-environment` profiles read from Kubernetes `Secret` references in `secretEnvFrom`.
- **Hardened runtime:** Runs as `65532:65532` with `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `RuntimeDefault` seccomp, and `ALL` Linux capabilities dropped.
