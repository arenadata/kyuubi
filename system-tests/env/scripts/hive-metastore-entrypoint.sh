#!/usr/bin/env bash

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

# apache/hive:4.x does not ship org.postgresql.Driver.
# Entrypoint copies /tmp/ext-jars/*.jar into $HIVE_HOME/lib before schematool (Hive 4.x).
set -euo pipefail

POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.3}"
MAVEN_CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"
STAGING_DIR="/tmp/ext-jars"
JAR_PATH="${STAGING_DIR}/postgresql-${POSTGRES_JDBC_VERSION}.jar"
HIVE_LIB="${HIVE_HOME:-/opt/hive}/lib"

mkdir -p "${STAGING_DIR}"

existing_pg="$(ls "${HIVE_LIB}"/postgresql-*.jar 2>/dev/null | head -1 || true)"

if [[ -n "${existing_pg}" ]]; then
  # Image already ships the driver; nothing to download.
  :
elif [[ -f "${JAR_PATH}" ]]; then
  :
elif command -v curl >/dev/null 2>&1; then
  curl -fsSL \
    "${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o "${JAR_PATH}"
elif command -v wget >/dev/null 2>&1; then
  wget -q -O "${JAR_PATH}" \
    "${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"
else
  echo "ERROR: no PostgreSQL JDBC jar in ${HIVE_LIB} and neither curl nor wget is available" >&2
  exit 1
fi

exec /entrypoint.sh "$@"
