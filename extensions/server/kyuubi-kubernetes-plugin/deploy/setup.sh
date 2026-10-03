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

# Brings the stand up in kind: a Kyuubi image from what the build left in target/,
# pushed to the kind registry at localhost:5001, then Trino, PostgreSQL, an engine for each
# as an operator would run it, and two
# Kyuubi servers in the namespace kyuubi-stand. Build first, from the repository root:
#
#   mvn -pl kyuubi-assembly,externals/kyuubi-trino-engine,externals/kyuubi-jdbc-engine,\
#     extensions/server/kyuubi-kubernetes-plugin,kyuubi-hive-beeline -am install -DskipTests
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
NS=kyuubi-stand
REGISTRY=localhost:5001
SCALA=2.13
VERSION=$(sed -n 's:.*<version>\(1\.[^<]*\)</version>.*:\1:p' "$REPO/pom.xml" | head -1)
CONTEXT="$HERE/../target/deploy"
STAGE="$CONTEXT/stage"

step() { printf '\n== %s\n' "$*"; }

step "KYUUBI_HOME for $VERSION, laid out as build/dist would"
rm -rf "$CONTEXT"
mkdir -p "$STAGE"/{jars,plugins,drivers,beeline-jars,conf,externals/engines/trino,externals/engines/jdbc}
cp "$REPO"/kyuubi-assembly/target/scala-$SCALA/jars/*.jar "$STAGE/jars/"
plugin="$REPO/extensions/server/kyuubi-kubernetes-plugin/target/kyuubi-kubernetes-plugin_$SCALA-$VERSION.jar"
# The server loads the plugin from its own classpath; engines from plugins/.
cp "$plugin" "$STAGE/jars/"
cp "$plugin" "$STAGE/plugins/"
for engine in trino jdbc; do
  cp "$REPO/externals/kyuubi-$engine-engine/target/kyuubi-$engine-engine_$SCALA-$VERSION.jar" \
    "$REPO"/externals/kyuubi-$engine-engine/target/scala-$SCALA/jars/*.jar \
    "$STAGE/externals/engines/$engine/"
done
cp "$REPO"/kyuubi-hive-beeline/target/*.jar "$STAGE/beeline-jars/"
cp -r "$REPO/bin" "$STAGE/"
# A jar the server already has is linked, not copied, as build/dist does: the engines
# share most of theirs with the server, and copies would double the image.
for dir in beeline-jars externals/engines/trino externals/engines/jdbc; do
  depth=$(awk -F/ '{print NF}' <<<"$dir")
  up=$(printf '../%.0s' $(seq 1 "$depth"))
  for jar in "$STAGE/$dir"/*.jar; do
    name=$(basename "$jar")
    if [ -f "$STAGE/jars/$name" ]; then
      ln -snf "${up}jars/$name" "$jar"
    fi
  done
done
cp "$REPO/conf/log4j2.xml.template" "$STAGE/conf/log4j2.xml"
# The JDBC engine carries no drivers: whichever the clusters need goes on its classpath.
cp "$(ls "$HOME"/.m2/repository/org/postgresql/postgresql/*/postgresql-*.jar | grep -v sources | sort -V | tail -1)" \
  "$STAGE/drivers/"
cp "$HERE/Dockerfile" "$CONTEXT/"
du -sh "$STAGE"

step "Images into $REGISTRY"
docker build -q -t "$REGISTRY/kyuubi-stateless:dev" "$CONTEXT" >/dev/null
docker push -q "$REGISTRY/kyuubi-stateless:dev"
for image in trinodb/trino:483 postgres:16-alpine; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image"
  docker tag "$image" "$REGISTRY/$image"
  docker push -q "$REGISTRY/$image"
done

step "Clusters and Kyuubi in $NS"
kubectl apply -f "$HERE/clusters.yaml" -f "$HERE/engines.yaml" -f "$HERE/kyuubi.yaml" >/dev/null
# A new image under the same tag reaches only new pods.
kubectl -n "$NS" rollout restart deploy/kyuubi deploy/trino-engine deploy/warehouse-engine >/dev/null
kubectl -n "$NS" delete pod client --ignore-not-found >/dev/null
kubectl apply -f "$HERE/kyuubi.yaml" >/dev/null
for workload in deploy/trino deploy/warehouse deploy/trino-engine deploy/warehouse-engine deploy/kyuubi; do
  kubectl -n "$NS" rollout status "$workload" --timeout=600s
done
kubectl -n "$NS" wait --for=condition=Ready pod/client --timeout=300s

step "Ready. Next: $HERE/check.sh"
