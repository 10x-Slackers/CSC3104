{{/* Pod settings every bank service shares: no API token, non-root, restricted PSS. */}}
{{- define "bank.pod" -}}
automountServiceAccountToken: false
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  seccompProfile: {type: RuntimeDefault}
{{- end }}

{{- define "bank.container" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: {drop: [ALL]}
resources:
  requests: {memory: 64Mi}
  limits: {memory: 128Mi}
{{- end }}

{{/* This bank's key tree from keys/<bank>/ (install.sh generates it), mounted at /keys/<bank>/ without subPath.
Takes (list $ "<glob under keys/<bank>/>"), so a service mounts only the keys it reads. */}}
{{- define "bank.keyVolume" -}}
{{- $ := index . 0 }}
name: keys
secret:
  secretName: bank-keys
  items:
    {{- range $path, $_ := $.Files.Glob (printf "keys/%s/%s" $.Values.bank (index . 1)) }}
    - {key: {{ $path | trimPrefix "keys/" | replace "/" "__" }}, path: {{ $path | trimPrefix "keys/" }}}
    {{- end }}
{{- end }}
