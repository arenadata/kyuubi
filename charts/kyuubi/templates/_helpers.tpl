{{/*
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/}}

{{/*
A comma separated string of enabled frontend protocols, e.g. "THRIFT_BINARY,THRIFT_HTTP".
REST and Spark Connect are not offered: either brings back the metadata store, and with it state
a Deployment cannot keep.
For details, see 'kyuubi.frontend.protocols': https://kyuubi.readthedocs.io/en/master/configuration/settings.html#frontend
*/}}
{{- define "kyuubi.frontend.protocols" -}}
  {{- $protocols := list }}
  {{- range $name, $frontend := .Values.server }}
    {{- if not (has $name (list "thriftBinary" "thriftHttp" "mysql")) }}
      {{- fail (printf "server.%s: a stateless Kyuubi serves thriftBinary, thriftHttp and mysql only; REST and Spark Connect need the metadata store" $name) }}
    {{- end }}
    {{- if $frontend.enabled }}
      {{- $protocols = $name | snakecase | upper | append $protocols }}
    {{- end }}
  {{- end }}
  {{- if not $protocols }}
    {{ fail "At least one frontend protocol must be enabled!" }}
  {{- end }}
  {{- $protocols |  join "," }}
{{- end }}

{{/*
The name of the first enabled frontend's port: what the probes knock on.
*/}}
{{- define "kyuubi.probePort" -}}
  {{- $port := "" }}
  {{- range $name := list "thriftBinary" "thriftHttp" "mysql" }}
    {{- $frontend := get $.Values.server $name }}
    {{- if and (not $port) $frontend $frontend.enabled }}
      {{- $port = $name | kebabcase }}
    {{- end }}
  {{- end }}
  {{- $port }}
{{- end }}

{{/*
The namespaces whose Services declare engines: the release's own unless listed.
*/}}
{{- define "kyuubi.discovery.namespaces" -}}
  {{- .Values.discovery.namespaces | default (list .Release.Namespace) | uniq | join "," }}
{{- end }}

{{/*
Whether Prometheus metrics are served.
*/}}
{{- define "kyuubi.prometheus.enabled" -}}
  {{- if and .Values.metrics.enabled (.Values.metrics.reporters | nospace | splitList "," | has "PROMETHEUS") }}true{{- end }}
{{- end }}

{{/*
kyuubi-defaults.conf. The chart owns what makes the server stateless; kyuubiConf.defaults is
appended after it and may override anything else. The plugin reads its settings from this file,
not from the command line, so they cannot go into args.
*/}}
{{- define "kyuubi.defaults" -}}
{{- $owned := list "kyuubi.frontend.protocols" "kyuubi.ha.addresses" "kyuubi.ha.client.class" "kyuubi.session.conf.advisor" "kyuubi.plugin.kubernetes.namespaces" }}
{{- range $key := $owned }}
  {{- if regexMatch (printf "(?m)^\\s*%s\\s*[=:\\s]" (replace "." "\\." $key)) ($.Values.kyuubiConf.defaults | default "") }}
    {{- fail (printf "kyuubiConf.defaults sets %s, which the chart owns: it is what keeps the server stateless" $key) }}
  {{- end }}
{{- end -}}
# Rendered by the chart: no ZooKeeper, no metadata store, no engines of its own.
kyuubi.frontend.protocols={{ include "kyuubi.frontend.protocols" . }}
kyuubi.frontend.connection.url.use.hostname=false
kyuubi.frontend.thrift.binary.bind.port={{ .Values.server.thriftBinary.port }}
kyuubi.frontend.thrift.http.bind.port={{ .Values.server.thriftHttp.port }}
kyuubi.frontend.mysql.bind.port={{ .Values.server.mysql.port }}

# The plugin in place of ZooKeeper. The address is any value: empty starts an embedded one.
kyuubi.ha.addresses=kubernetes
kyuubi.ha.client.class=org.apache.kyuubi.plugin.kubernetes.KubernetesDiscoveryClient

# Engine profiles, and where their engines run, from the Services.
kyuubi.session.conf.advisor=org.apache.kyuubi.plugin.kubernetes.ClusterProfileAdvisor
kyuubi.plugin.kubernetes.namespaces={{ include "kyuubi.discovery.namespaces" . }}
kyuubi.plugin.kubernetes.labelSelector={{ .Values.discovery.labelSelector }}
kyuubi.plugin.kubernetes.annotationPrefix={{ .Values.discovery.annotationPrefix }}
# Profile names are resolved before the advisor runs; the Services' are unknown to the server.
kyuubi.engine.profiles.unknown.strategy=LOG
# Engines run by operators are shared by every user.
kyuubi.engine.share.level={{ .Values.engine.shareLevel }}

kyuubi.metrics.enabled={{ .Values.metrics.enabled }}
kyuubi.metrics.reporters={{ .Values.metrics.reporters }}
kyuubi.metrics.prometheus.port={{ .Values.metrics.prometheusPort }}
{{- with .Values.kyuubiConf.defaults }}

# kyuubiConf.defaults
{{ tpl . $ }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kyuubi.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "kyuubi.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{ include "kyuubi.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
