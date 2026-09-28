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

# What a stateless Kyuubi has to do, checked on the stand setup.sh brought up.
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

registry() { k exec "$1" -- sh -c 'cd /tmp/kyuubi-discovery 2>/dev/null && find . -type d -name "serverUri=*" | grep -v "^./kyuubi/" | sort'; }
engines() { # engine JVMs running in a Kyuubi pod
  k exec "$1" -- sh -c 'for c in /proc/[0-9]*/cmdline; do tr "\0" " " < "$c" 2>/dev/null; echo; done' \
    | grep -oE "org\.apache\.kyuubi\.engine\.[a-z]+\.[A-Za-z]+Engine" | sort | uniq -c
}

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
expect "with what exists" "$out" "[trino, warehouse]"

scenario "4. The engines are registered on the pod that started them, and only there"
reg=$(registry "$A")
expect "the Trino engine of bob" "$reg" "_USER_TRINO/bob/trino/serverUri="
expect "the JDBC engine of alice" "$reg" "_USER_JDBC/alice/warehouse/serverUri="
running=$(engines "$A")
expect "both are processes of the pod" "$running" "TrinoSqlEngine"
expect "the JDBC engine too" "$running" "JdbcSQLEngine"
[ -z "$(registry "$B")" ] && ok "$B has nothing registered" || fail "$B has: $(registry "$B")"

scenario "5. A second session of the same user finds the engine instead of starting one"
out=$(sql "$A_IP" bob "select name from tpch.tiny.nation where nationkey = 0")
expect "answered" "$out" "ALGERIA"
n=$(registry "$A" | grep -c "_USER_TRINO/bob/")
[ "$n" = "1" ] && ok "still one Trino engine for bob" || fail "$n Trino engines for bob"

scenario "6. A cluster re-declared on its Service takes effect without a restart"
k annotate service warehouse kyuubi/users=alice --overwrite >/dev/null
sleep 3
out=$(sql "$A_IP" bob "select 1" "kyuubi.engine.profile=warehouse")
expect "bob is no longer let onto warehouse" "$out" "Engine profile 'warehouse' is not allowed for user bob"
out=$(sql "$A_IP" alice "select 3" "kyuubi.engine.profile=warehouse")
expect "alice still is" "$out" "3"
k annotate service warehouse kyuubi/users- >/dev/null

scenario "7. An idle engine stops, and takes its registration with it"
started=$(date +%s)
for _ in $(seq 1 60); do [ -z "$(registry "$A")" ] && break; sleep 3; done
if [ -z "$(registry "$A")" ]; then
  ok "the registry of $A is empty after $(( $(date +%s) - started ))s"
else
  fail "still registered: $(registry "$A")"
fi
# The registration goes first; the engine then takes a few seconds to stop its services.
for _ in $(seq 1 30); do [ -z "$(engines "$A")" ] && break; sleep 2; done
[ -z "$(engines "$A")" ] && ok "and no engine process is left" || fail "left running: $(engines "$A")"

scenario "8. A server that dies costs only itself"
k delete pod "$A" --wait=true >/dev/null
out=$(sql kyuubi bob "select count(*) from tpch.tiny.region")
expect "a new session through the Service lands on a live server" "$out" "5"
k rollout status deploy/kyuubi --timeout=300s >/dev/null
new=$(k get pod -l app.kubernetes.io/name=kyuubi -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v "^$B$")
[ -n "$new" ] && ok "its replacement $new is up, with nothing to recover" || fail "no replacement"

printf '\n%s\n' "$([ "$FAILED" -eq 0 ] && echo "All scenarios behaved." || echo "$FAILED check(s) failed.")"
exit "$FAILED"
