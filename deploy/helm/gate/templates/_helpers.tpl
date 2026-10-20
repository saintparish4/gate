{{/* Names and labels shared by every template. */}}

{{- define "gate.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "gate.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "gate.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "gate.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "gate.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "gate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: gate
{{- end -}}

{{- define "gate.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: server
{{- end -}}

{{- define "gate.aggregatorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "gate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: usage-aggregator
{{- end -}}

{{/* An image reference, refusing an empty repository or tag. */}}
{{- define "gate.image" -}}
{{- $repo := required (printf "%s.image.repository is required" .what) .image.repository -}}
{{- $tag := required (printf "%s.image.tag is required (an immutable tag, never latest)" .what) .image.tag -}}
{{- if eq $tag "latest" -}}
{{- fail (printf "%s.image.tag is latest: pin the commit tag publish-image.sh pushed" .what) -}}
{{- end -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
