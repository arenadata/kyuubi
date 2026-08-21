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
# Usage:
#   mvn clean package -Dspark.artifact.version=3.5   (or 4.2)
#   KYUUBI_URL=sc://host:10199/;use_ssl=true KYUUBI_AUTH=KERBEROS ./run.sh
#   KYUUBI_URL=sc://host:10199/;use_ssl=true KYUUBI_AUTH=LDAP KYUUBI_USERNAME=john KYUUBI_PASSWORD=secret ./run.sh
#   SPARK_ARTIFACT_VERSION=4.2 ./run.sh   # to run the jar built against Spark 4.2

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_ARTIFACT_VERSION="${SPARK_ARTIFACT_VERSION:-3.5}"
JAR="$SCRIPT_DIR/target/kyuubi-spark-connect-client-jvm-examples-spark-${SPARK_ARTIFACT_VERSION}-1.0-SNAPSHOT.jar"

if [[ ! -f "$JAR" ]]; then
  echo "Jar not found: $JAR" >&2
  echo "Build it first: mvn clean package -f \"$SCRIPT_DIR/pom.xml\" -Dspark.artifact.version=${SPARK_ARTIFACT_VERSION}" >&2
  exit 1
fi

exec java --add-opens=java.base/java.nio=ALL-UNNAMED -jar "$JAR" "$@"
