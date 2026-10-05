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

# Kyuubi Kubernetes plugin

A Kyuubi server in Kubernetes without ZooKeeper and without a metadata store, that starts no engine:
engines are started by operators, each behind a Service, and the Service says which engine profile
it serves and where. Every server pod stands on its own.

Two classes, one jar, no change to the server:

| Class | Loaded through | What it does |
|---|---|---|
| `ClusterProfileAdvisor` | `kyuubi.session.conf.advisor` | Engine profiles declared on Services - the same vocabulary as `kyuubi.engine.profile.<name>.*`, read from annotations and followed as they change. |
| `KubernetesDiscoveryClient` | `kyuubi.ha.client.class` | Where a session's engine runs: the Service of its profile, from the same informer. A profile with no running engine fails the session instead of making the server start one. |

## What "stateless" costs and does not cost

A server pod that dies loses its own sessions and nothing else. New connections land on a live pod
through the Service; a session on a dead pod has to be reopened, as it would with ZooKeeper too -
Kyuubi never moved a session between servers. Engines outlive any server pod and are shared by all
of them: they belong to their operators.

Nothing durable is kept anywhere: no ZooKeeper, no database. That holds while the REST frontend
and Spark Connect are off, because those are what the metadata store serves.

## Configuration

`kyuubi-defaults.conf`:

```properties
# Only Thrift: REST and Spark Connect would bring the metadata store back.
kyuubi.frontend.protocols=THRIFT_BINARY

# Any value at all: an empty one makes the server start an embedded ZooKeeper.
kyuubi.ha.addresses=kubernetes
kyuubi.ha.client.class=org.apache.kyuubi.plugin.kubernetes.KubernetesDiscoveryClient

kyuubi.session.conf.advisor=org.apache.kyuubi.plugin.kubernetes.ClusterProfileAdvisor
# Where the clusters' Services are, comma separated. Unset means all namespaces,
# which needs a ClusterRole to read Services.
kyuubi.plugin.kubernetes.namespaces=trino,impala
kyuubi.plugin.kubernetes.labelSelector=kyuubi/enabled=true
kyuubi.plugin.kubernetes.annotationPrefix=kyuubi/

# The server checks a requested profile name against its own declarations before this
# plugin runs and knows nothing of the Services: it must log, not fail. The plugin fails
# the session itself when a name is neither a Service's profile nor a declared one.
kyuubi.engine.profiles.unknown.strategy=LOG

# An engine run by an operator is shared by every user it serves.
kyuubi.engine.share.level=SERVER
```

The jar goes into the server's classpath (`$KYUUBI_HOME/jars`); the Kubernetes client it uses is
there already. Engines do not load it: they register nowhere.

RBAC: `get`, `list` and `watch` on `services` in every namespace listed, as a Role each or one
ClusterRole. Nothing is written.

## A cluster as a Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: analytics
  namespace: trino
  labels:
    kyuubi/enabled: "true"
  annotations:
    kyuubi/type: TRINO
    kyuubi/session.engine.trino.connection.catalog: hive
    kyuubi/users: alice,bob
spec:
  ports:
    - name: http
      port: 8080
```

is, to a session, what

```properties
kyuubi.engine.profile.analytics.type=TRINO
kyuubi.engine.profile.analytics.session.engine.trino.connection.catalog=hive
kyuubi.engine.profile.analytics.session.engine.trino.connection.url=http://analytics.trino.svc:8080
```

would be in `kyuubi-defaults.conf` - and unlike that file, it is read again whenever it changes.
A session asks for it as it asks for any profile:

```
jdbc:kyuubi://kyuubi:10009/;#kyuubi.engine.profile=analytics
```

Annotation keys, all under the configured prefix:

| Key | Meaning |
|---|---|
| `type` | the engine type: `TRINO`, `JDBC`, ... - required |
| `env.<VAR>` | an environment variable for the engine |
| `session.<key>` | a Kyuubi session config, `kyuubi.session.<key>` |
| `conf.<key>` | any config key, as is |
| `profile` | the profile name; the Service name unless set |
| `users` | who may use it, comma separated; anyone who asks for it by name, unless set |
| `default` | `"true"` on the one profile users who ask for nothing get |
| `scheme`, `port` | how to reach the cluster through this Service: `http` and its first port unless set; `port` is a number or a port name |
| `port-name` | the name of the port the profile's engine listens behind, `kyuubi` unless set; see [Engines](#engines) |

Every value may say `{host}`, `{port}` and `{url}` for the Service's own address, and `{user}`
for the session user, filled in per session. A `TRINO` profile that names no connection url gets
the Service's. An Impala cluster through the JDBC engine:

```yaml
    kyuubi/type: JDBC
    kyuubi/port: hs2
    kyuubi/conf.kyuubi.engine.jdbc.type: impala
    kyuubi/conf.kyuubi.engine.jdbc.connection.url: "jdbc:impala://{host}:{port}/;hive.server2.proxy.user={user}"
```

How a session's profile is chosen: the one it names with `kyuubi.engine.profile`, refused if the
Service lists `users` and the session's is not among them; otherwise the Service naming the user;
otherwise the default; otherwise nothing, and the server's own profile resolution stands. Profiles
declared in `kyuubi-defaults.conf` keep working alongside, and a name that exists in both is
applied from both, the Service's on top.

Each profile gets its own `kyuubi.engine.share.level.subdomain`, its name made path-safe, unless
the Service sets one: engines are keyed by share level, type, user and subdomain, and without it two
profiles at the same share level would share one engine pointed at whichever cluster it was
launched for.

A Service whose annotations do not make a profile - an unknown key, a port it does not have, no
`type` - is skipped with a warning and costs only itself.

## Engines

A Service runs the engine of its profile when one of its ports is named `kyuubi`, or as its
`port-name` annotation says:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: spark4
  namespace: engines
  labels:
    kyuubi/enabled: "true"
  annotations:
    kyuubi/type: SPARK_SQL
spec:
  selector:
    app: spark-thrift-server
  ports:
    - name: kyuubi
      port: 10009
```

A session of the profile `spark4` is connected to `spark4.engines.svc:10009`; Kubernetes picks a
Ready pod behind it. A Service whose port already has a name keeps it and says which one it is:

```yaml
  annotations:
    kyuubi/type: SPARK_SQL
    kyuubi/port-name: thrift
spec:
  ports:
    - name: thrift
      port: 10000
```

Without `port-name`, a Service with no port named `kyuubi` is a profile without an engine. With
it, a port the Service does not have is a typo, and the Service is skipped like one with an unknown
key. The engine runs registered nowhere - no `kyuubi.ha.addresses` in its own
configuration - and kept up between sessions.

The server asks for an engine before it would start one, by its engine space:
`/<namespace>_<version>_<share level>_<type>/<user>/<subdomain>`. Every profile gets its own
subdomain, so the space names the profile, and its type has to match. A space no Service runs an
engine for is answered with an error - "no Service declares a profile for it", or "has no port
named 'kyuubi'" - which fails the session: Kyuubi never starts an engine with this client. At
`CONNECTION` share level the space names a connection rather than a profile and is always refused.

When the server cannot connect to an engine it asks to forget it; the Service stays, as it is its
operator's. Nothing is written anywhere, and the server's own registration goes nowhere - clients
reach the servers through their Service. Paths, locks and counters other parts of Kyuubi ask for -
engine pools, `kyuubi-ctl` - are kept in the server's memory.

## Deployment

`deploy/` brings up, in the namespace `kyuubi-stand`, from images pushed to the kind registry at
`localhost:5001`: a Trino and a PostgreSQL, an engine for each run as an operator would run it - a
Deployment of its own behind a Service that declares the profile - two Kyuubi servers with this
plugin, and beeline:

```sh
mvn -pl kyuubi-assembly,externals/kyuubi-trino-engine,externals/kyuubi-jdbc-engine,\
  extensions/server/kyuubi-kubernetes-plugin,kyuubi-hive-beeline -am clean install -DskipTests
extensions/server/kyuubi-kubernetes-plugin/deploy/setup.sh
extensions/server/kyuubi-kubernetes-plugin/deploy/check.sh
extensions/server/kyuubi-kubernetes-plugin/deploy/stop.sh
```

`check.sh` runs through beeline:

- no server starts a ZooKeeper, REST or a metadata store;
- a session with no profile gets the default one, Trino, through its engine's Service;
- a named profile gets PostgreSQL through the JDBC engine's;
- an unknown name is refused, listing the names that exist;
- no server runs an engine process, and both servers' sessions land on the same engine;
- a profile whose Service names no engine is refused, and nothing is started for it;
- a changed annotation takes effect without a restart;
- an engine scaled to zero fails its sessions, and none is started in its place;
- a deleted server costs only its own sessions.

Build with `clean`: `kyuubi-assembly/target` keeps jars from earlier builds, and two Jackson
versions side by side break the server.

## Not covered

- Whether the pods behind an engine's Service are Ready is Kubernetes's to say: a Service with
  none fails the session on connection, after `kyuubi.session.engine.open.max.attempts`.
- `kyuubi-defaults.conf` itself is still read once, at start. What changes without a restart is
  what the Services declare.
