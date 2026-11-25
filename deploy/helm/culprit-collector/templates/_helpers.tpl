{{- define "culprit-collector.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "culprit-collector.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "culprit-collector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "culprit-collector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "culprit-collector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: culprit-collector
shard: {{ .Values.collector.shard | quote }}
{{- end }}

{{- define "culprit-collector.labels" -}}
helm.sh/chart: {{ include "culprit-collector.chart" . }}
{{ include "culprit-collector.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "culprit-collector.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "culprit-collector.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "culprit-collector.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- $tag := default (printf "v%s" .Chart.AppVersion) .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}
{{- end }}

{{- define "culprit-collector.validateActivation" -}}
{{- $replicas := int .Values.replicaCount }}
{{- if or (lt $replicas 0) (gt $replicas 1) }}
{{- fail "replicaCount must be 0 or 1 (one StatefulSet owner per static collector shard)" }}
{{- end }}
{{- if and .Values.activation.sourceQuiescent .Values.activation.deliveryOnly }}
{{- fail "activation.sourceQuiescent and activation.deliveryOnly are mutually exclusive" }}
{{- end }}
{{- if and (eq $replicas 1) (not (or .Values.activation.sourceQuiescent .Values.activation.deliveryOnly)) }}
{{- fail "Setting replicaCount=1 requires explicit activation.sourceQuiescent=true or activation.deliveryOnly=true" }}
{{- end }}
{{- if .Values.azureWorkloadIdentity.enabled }}
{{- if not (and .Values.azureWorkloadIdentity.tenantId .Values.azureWorkloadIdentity.clientId) }}
{{- fail "azureWorkloadIdentity.enabled=true requires both azureWorkloadIdentity.tenantId and azureWorkloadIdentity.clientId" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "culprit-collector.connectionsToml" -}}
# Managed by Helm. Connection metadata only; never store secrets here.
{{- range $connName, $conn := .Values.connections }}
{{- if or (hasKey $conn "password") (hasKey $conn "token") (hasKey $conn "secret") }}
{{- fail (printf "connections.%s must not contain inline secrets; use Entra/IAM or secretEnvFrom" $connName) }}
{{- end }}
[connections.{{ $connName }}]
{{- range $key, $val := $conn }}
{{- if not (kindIs "map" $val) }}
{{- if kindIs "bool" $val }}
{{ $key }} = {{ $val }}
{{- else if or (kindIs "int" $val) (kindIs "int64" $val) (kindIs "float64" $val) }}
{{ $key }} = {{ $val }}
{{- else }}
{{ $key }} = {{ printf "%q" (toString $val) }}
{{- end }}
{{- end }}
{{- end }}
{{- range $key, $val := $conn }}
{{- if and (kindIs "map" $val) (gt (len $val) 0) }}

[connections.{{ $connName }}.{{ $key }}]
{{- range $subKey, $subVal := $val }}
{{- if kindIs "bool" $subVal }}
{{ $subKey }} = {{ $subVal }}
{{- else if or (kindIs "int" $subVal) (kindIs "int64" $subVal) (kindIs "float64" $subVal) }}
{{ $subKey }} = {{ $subVal }}
{{- else }}
{{ $subKey }} = {{ printf "%q" (toString $subVal) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- end }}
{{- end }}

{{- define "culprit-collector.collectorJson" -}}
{{- $creds := .Values.collector.credentials }}
{{- if and (empty $creds) .Values.azureWorkloadIdentity.enabled }}
{{- $wi := dict "kind" "azure" "identity" (dict "kind" "workload-identity" "tenant_id" .Values.azureWorkloadIdentity.tenantId "client_id" .Values.azureWorkloadIdentity.clientId "token_file" "/var/run/secrets/culprit/token") }}
{{- $creds = dict "source" $wi "recovery" $wi "analytics" $wi "ingestion" $wi }}
{{- end }}
{{- $recovery := deepCopy .Values.collector.recovery }}
{{- if eq (toString $recovery.provider) "s3" }}
{{- $_ := unset $recovery.options "account" }}
{{- $_ := unset $recovery.options "container" }}
{{- end }}
{{- $target := deepCopy .Values.collector.target }}
{{- if ne (toString $target.kind) "adx" }}
{{- $_ := unset $target "delivery" }}
{{- end }}
{{- $cfg := dict
  "pipeline" .Values.collector.pipeline
  "shard" .Values.collector.shard
  "workspace" "/var/lib/culprit/workspace"
  "state" "/var/lib/culprit/state"
  "health_listen" .Values.collector.healthListen
  "inventory" .Values.collector.inventory
  "monitoring" .Values.collector.monitoring
  "credentials" $creds
  "source_credentials" .Values.collector.sourceCredentials
  "recovery" $recovery
  "target" $target
  "limits" .Values.collector.limits
}}
{{- mustToPrettyJson $cfg }}
{{- end }}
