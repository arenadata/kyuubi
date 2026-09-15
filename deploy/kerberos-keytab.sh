#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Fetches the gateway's keytab from ldap-kdc and puts it in a Secret.
#
# Done from here rather than by the KDC, because the alternative is giving the
# KDC pod the right to write Secrets in this namespace - a much larger thing to
# grant than fetching one file at deploy time.
set -euo pipefail

NS="${NS:-gateway}"
PRINCIPAL="${PRINCIPAL:-hive/kyuubi-routing-gateway.${NS}.svc.cluster.local}"
SECRET="${SECRET:-kyuubi-routing-gateway-keytab}"
PORT="${PORT:-15555}"

TOKEN="$(kubectl -n "$NS" get secret ldap-kdc-api -o jsonpath='{.data.token}' | base64 -d)"
KEYTAB="$(mktemp -t keytab.XXXXXX)"

kubectl -n "$NS" port-forward svc/ldap-kdc "$PORT:5555" >/dev/null 2>&1 &
FORWARD=$!
trap 'kill $FORWARD 2>/dev/null || true; rm -f "$KEYTAB"' EXIT

for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:$PORT/readyz" && break
  sleep 0.5
done

# The name carries a slash; the API accepts it either raw or encoded.
curl -sSf -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$PORT/api/v1/principals/${PRINCIPAL}/keytab" -o "$KEYTAB"

if [ ! -s "$KEYTAB" ]; then
  echo "the API returned an empty keytab for $PRINCIPAL" >&2
  exit 1
fi

kubectl -n "$NS" create secret generic "$SECRET" \
  --from-file=gateway.keytab="$KEYTAB" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "keytab for $PRINCIPAL is in secret/$SECRET ($(wc -c < "$KEYTAB" | tr -d ' ') bytes)"
