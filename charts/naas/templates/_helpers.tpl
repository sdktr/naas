{{/*
Common labels applied to all resources.
*/}}
{{- define "naas.labels" -}}
app.kubernetes.io/name: naas
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Container image with tag defaulting to appVersion.
*/}}
{{- define "naas.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Secret name — use existing or generated.
*/}}
{{- define "naas.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{ .Values.secrets.existingSecret }}
{{- else -}}
naas-secret
{{- end -}}
{{- end }}

{{/*
NATS servers list.
*/}}
{{- define "naas.natsServers" -}}
{{- if .Values.nats.enabled -}}
nats://nats:{{ .Values.nats.port }}
{{- else -}}
{{ .Values.nats.external.servers }}
{{- end -}}
{{- end }}
