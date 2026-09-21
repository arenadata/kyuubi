<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Running Tests

**Kyuubi** can be tested based on [Apache Maven](https://maven.apache.org) and the ScalaTest Maven Plugin,
please refer to the [ScalaTest documentation](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin),

## Running Tests Fully

The following is an example of a command to run all the tests:

```bash
./build/mvn clean install
```

## Running Tests for a Module

```bash
./build/mvn clean install -pl kyuubi-common
```

## Running Tests for a Single Test

When developing locally, it’s convenient to run one single test, or a couple of tests, rather than all.

With Maven, you can use the -DwildcardSuites flag to run individual Scala tests:

```bash
./build/mvn clean install -Dtest=none -DwildcardSuites=org.apache.kyuubi.service.FrontendServiceSuite
```

If you want to make a single test that need to integrate with kyuubi-spark-sql-engine module, please build the package
for kyuubi-spark-sql-engine module at first.

You can leverage the ready-made tool for creating a binary distribution.

```bash
./build/dist
```

## Testing Engine Profiles and Access Rules

The ADH-8475 and ADH-9238 regression tests use the existing ScalaTest infrastructure.
Kyuubi, embedded ZooKeeper, and local Spark/Hive engine processes run on JVMs. The Trino
and PostgreSQL suites additionally use the existing single-service Testcontainers fixtures
(`trinodb/trino:411` and `postgres:16.1`); they need a working Docker-compatible runtime.
They do not require the `system-tests` images or an external Hadoop cluster.

Use JDK 21 and build the selected modules and their dependencies from the repository root:

```bash
./build/mvn -Pscala-2.13,spark-3.5,java-21,flink-provided,jdbc-shaded \
  -pl kyuubi-server,integration-tests/kyuubi-hive-it,integration-tests/kyuubi-trino-it,integration-tests/kyuubi-jdbc-it \
  -am install -DskipTests
```

The build prepares the existing Spark/Hive distributions and engine jars. Run only the relevant
ScalaTest suites with `-Dsuites` (a comma-separated list of fully qualified class names):

```bash
./build/mvn -Pscala-2.13,spark-3.5,java-21,flink-provided,jdbc-shaded \
  -pl kyuubi-server test -Dtest=none \
  -Dsuites=org.apache.kyuubi.engine.EngineProfileJdbcSuite,org.apache.kyuubi.engine.EngineProfileJdbcLogSuite,org.apache.kyuubi.server.api.v1.EngineProfileRestSuite

./build/mvn -Pscala-2.13,spark-3.5,java-21,flink-provided,jdbc-shaded \
  -pl integration-tests/kyuubi-hive-it test -Dtest=none \
  -Dsuites=org.apache.kyuubi.it.hive.operation.HiveEngineProfileSuite

./build/mvn -Pscala-2.13,spark-3.5,java-21,flink-provided,jdbc-shaded \
  -pl integration-tests/kyuubi-trino-it test -Dtest=none \
  -Dsuites=org.apache.kyuubi.it.trino.operation.TrinoEngineProfileSuite

./build/mvn -Pscala-2.13,spark-3.5,java-21,flink-provided,jdbc-shaded \
  -pl integration-tests/kyuubi-jdbc-it test -Dtest=none \
  -Dsuites=org.apache.kyuubi.it.jdbc.postgresql.PostgreSQLEngineProfileSuite
```
## Testing Multiple Spark Engine Profiles

`MultiSparkEngineProfileSuite` runs a Kyuubi server with embedded ZooKeeper and two local
Spark engine processes. It keeps Spark 3.5 and Spark 4.2 connections open for the same user,
checks real SQL results and engine identities, and verifies reuse within each profile and
isolation between profiles. It requires JDK 21 and Scala 2.13 distributions of both Spark versions.

Prepare the server and both engine versions from the repository root:

```bash
./build/mvn \
  -Pscala-2.13,spark-3.5,java-21,flink-provided,hive-provided \
  -pl kyuubi-server -am clean install -DskipTests

./build/mvn \
  -Pscala-2.13,spark-4.2,java-21,flink-provided,hive-provided \
  -pl externals/kyuubi-download,externals/kyuubi-spark-sql-engine \
  install -DskipTests
```

The first command builds and installs the server's dependencies. The second adds Spark 4.2
using the existing download module and engine build. Do not add `clean` or `-am` to the second
command: both unpacked distributions and both versioned engine jars must remain available.

Run the suite explicitly, passing the absolute paths of the unpacked Spark distributions in
`externals/kyuubi-download/target`. For the versions currently selected by the Maven profiles:

```bash
./build/mvn \
  -Pscala-2.13,spark-3.5,java-21,flink-provided,hive-provided \
  -pl kyuubi-server test -Dtest=none \
  -Dsuites=org.apache.kyuubi.engine.spark.MultiSparkEngineProfileSuite \
  -Dkyuubi.test.spark3.home="$PWD/externals/kyuubi-download/target/spark-3.5.4.4-4.3.0-2-bin-hadoop3" \
  -Dkyuubi.test.spark4.home="$PWD/externals/kyuubi-download/target/spark-4.2.0.1-4.3.0-2-bin-hadoop3"
```
