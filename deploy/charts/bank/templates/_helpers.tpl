{{/* Pod settings every service shares: no API token, non-root, the restricted Pod Security Standard. */}}
{{- define "bank.pod" -}}
automountServiceAccountToken: false
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  seccompProfile: {type: RuntimeDefault}
{{- end }}

{{/* Container hardening and memory. Pass (dict "limit" "256Mi") instead of . for more memory. */}}
{{- define "bank.container" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: {drop: [ALL]}
resources:
  requests: {memory: {{ .request | default "64Mi" }}}
  limits: {memory: {{ .limit | default "128Mi" }}}
{{- end }}

{{- define "bank.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{/* The scorer's state: a Valkey pod in the bank, or the bank's ElastiCache on AWS. */}}
{{- define "bank.valkeyUrl" -}}
{{ .Values.aws.valkeyUrl | default "valkey://valkey:6379" }}
{{- end }}
