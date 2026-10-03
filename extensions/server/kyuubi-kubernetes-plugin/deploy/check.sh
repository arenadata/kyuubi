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

# What a stateless Kyuubi has to do, checked on the stand setup.sh brought up:
# no ZooKeeper, no metadata store, and no engine of its own - engines are the operators'.
# Every session goes through beeline, as a client's would.
set -uo pipefail

NS=kyuubi-stand
FAILED=0

k() { kubectl -n "$NS" "$@"; }
ok() { printf '   \033[32mOK\033[0m    %s\n' "$*"; }
fail() { printf '   \033[31mFAIL\033[0m  %s\n' "$*"; FAILED=$((FAILED + 1)); }
scenario() { printf '\n== %s\n' "$*"; }
expect() { # description, haystack, needle
  if grep -qF -- "$3" <<<"$2"; then ok "$1"; else fail "$1 - wanted '$3' in: $(tail -c 600 <<<"$2")"; fi
}

# beeline HOST USER SQL [session conf, after the #]
sql() {
  local host="$1" user="$2" statement="$3" conf="${4:-}"
  k exec client -- ./bin/beeline -u "jdbc:hive2://$host:10009/${conf:+;#$conf}" -n "$user" \
    --silent=true --showHeader=false --outputformat=csv2 -e "$statement" 2>&1 \
    | grep -vE "^(SLF4J|WARNING|Connecting|Connected|Driver|Transaction|Beeline version|Closing)"
}

pods=($(k get pod -l app.kubernetes.io/name=kyuubi -o jsonpath='{.items[*].metadata.name}'))
A=${pods[0]}
B=${pods[1]}
A_IP=$(k get pod "$A" -o jsonpath='{.status.podIP}')

engines() { # engine JVMs running in a pod
  k exec "$1" -- sh -c 'for c in /proc/[0-9]*/cmdline; do tr "\0" " " < "$c" 2>/dev/null; echo; done' \
    | grep -oE "org\.apache\.kyuubi\.engine\.[a-z]+\.[A-Za-z]+Engine" | sort | uniq -c
}
TRINO_ENGINE=$(k get pod -l app.kubernetes.io/name=trino-engine -o jsonpath='{.items[0].metadata.name}')
sessions_on_engine() { # client addresses of the sessions the Trino engine has opened
  k logs "$TRINO_ENGINE" | grep -oE "Opening session for [a-z]+@[0-9.]+" | sed 's/.*@//' | sort -u
}
B_IP=$(k get pod "$B" -o jsonpath='{.status.podIP}')

scenario "0. Two servers, and not a ZooKeeper among them"
for p in "$A" "$B"; do
  log=$(k logs "$p")
  if grep -qiE "EmbeddedZookeeper|ZooKeeperServer|zkServer" <<<"$log"; then
    fail "$p started a ZooKeeper"
  else
    ok "$p: no ZooKeeper, embedded or otherwise"
  fi
  expect "$p serves Thrift on 10009" "$log" "Starting and exposing JDBC connection at: jdbc:hive2://"
  if grep -q "RestFrontendService\|Metadata" <<<"$(grep -E 'Service\[|is started' <<<"$log")"; then
    fail "$p started REST or the metadata store"
  else
    ok "$p: no REST, no metadata store"
  fi
done

scenario "1. A session that asks for nothing gets the default profile: Trino, from its Service"
out=$(sql "$A_IP" bob "select count(*) from tpch.tiny.nation")
expect "Trino answered through the Trino engine" "$out" "25"
expect "the engine connected where the Service points" "$(k logs "$A" | grep -F "profile 'trino'" | tail -1)" \
  "Applying engine profile 'trino' from Service kyuubi-stand/trino"

scenario "2. A session that asks for a profile by name gets it: PostgreSQL through the JDBC engine"
out=$(sql "$A_IP" alice "select current_database(), 1 + 1" "kyuubi.engine.profile=warehouse")
expect "PostgreSQL answered through the JDBC engine" "$out" "postgres,2"

scenario "3. A profile nobody declared is refused, naming the ones that exist"
out=$(sql "$A_IP" alice "select 1" "kyuubi.engine.profile=nope")
expect "refused" "$out" "Engine profile 'nope' is not defined"
expect "with what exists" "$out" "[impala, trino, warehouse]"

scenario "4. Engines are the operators': Kyuubi starts none, and every server shares them"
out=$(sql "$B_IP" carol "select count(*) from tpch.tiny.region")
expect "a session through the other server ran too" "$out" "5"
for p in "$A" "$B"; do
  [ -z "$(engines "$p")" ] && ok "$p runs no engine process" || fail "$p runs: $(engines "$p")"
done
seen=$(sessions_on_engine)
expect "the Trino engine served the first server" "$seen" "$A_IP"
expect "and the second" "$seen" "$B_IP"

scenario "5. A profile whose engine nobody runs is refused, and nothing is started for it"
out=$(sql "$A_IP" alice "select 1" "kyuubi.engine.profile=impala")
expect "refused, saying why" "$out" "has no port named 'kyuubi'"
[ -z "$(engines "$A")" ] && ok "no engine was started for it" || fail "started: $(engines "$A")"

scenario "6. A cluster re-declared on its Service takes effect without a restart"
k annotate service warehouse-engine kyuubi/users=alice --overwrite >/dev/null
sleep 3
out=$(sql "$A_IP" bob "select 1" "kyuubi.engine.profile=warehouse")
expect "bob is no longer let onto warehouse" "$out" "Engine profile 'warehouse' is not allowed for user bob"
out=$(sql "$A_IP" alice "select 3" "kyuubi.engine.profile=warehouse")
expect "alice still is" "$out" "3"
k annotate service warehouse-engine kyuubi/users- >/dev/null

scenario "7. An engine that is down fails its sessions; Kyuubi does not start one in its place"
k scale deploy/trino-engine --replicas=0 >/dev/null
k wait --for=delete pod -l app.kubernetes.io/name=trino-engine --timeout=120s >/dev/null 2>&1
out=$(sql "$A_IP" bob "select 1")
expect "the session fails" "$out" "Error"
[ -z "$(engines "$A")" ] && ok "and no engine was started for it" || fail "started: $(engines "$A")"
k scale deploy/trino-engine --replicas=1 >/dev/null
k rollout status deploy/trino-engine --timeout=300s >/dev/null
out=$(sql "$A_IP" bob "select count(*) from tpch.tiny.nation")
expect "once the operator brings it back, sessions run again" "$out" "25"

scenario "8. A server that dies costs only itself"
k delete pod "$A" --wait=true >/dev/null
out=$(sql kyuubi bob "select count(*) from tpch.tiny.region")
expect "a new session through the Service lands on a live server" "$out" "5"
k rollout status deploy/kyuubi --timeout=300s >/dev/null
new=$(k get pod -l app.kubernetes.io/name=kyuubi -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v "^$B$")
[ -n "$new" ] && ok "its replacement $new is up, with nothing to recover" || fail "no replacement"

printf '\n%s\n' "$([ "$FAILED" -eq 0 ] && echo "All scenarios behaved." || echo "$FAILED check(s) failed.")"
exit "$FAILED"
