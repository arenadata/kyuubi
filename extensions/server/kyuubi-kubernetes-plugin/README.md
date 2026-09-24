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

A Kyuubi server in Kubernetes without ZooKeeper and without a metadata store: every pod stands on
its own, and the clusters it can send a session to are declared on their Services.

Two classes, one jar, no change to the server:

| Class | Loaded through | What it does |
|---|---|---|
| `PodDiscoveryClient` | `kyuubi.ha.client.class` | The engine registry ZooKeeper used to hold, on the pod's own file system. The server and the engines it launches are processes of one pod, and the file system they share is registry enough. |
| `ClusterProfileAdvisor` | `kyuubi.session.conf.advisor` | Engine profiles declared on Services - the same vocabulary as `kyuubi.engine.profile.<name>.*`, read from annotations and followed as they change. |

## What "stateless" costs and does not cost

A pod that dies loses its own sessions and nothing else. New connections land on a live pod
through the Service; a session on a dead pod has to be reopened, as it would with ZooKeeper too -
Kyuubi never moved a session between servers. Engines are per pod: two pods serving the same user
at `USER` share level each start their own engine.

Nothing durable is kept anywhere: no ZooKeeper, no database. That holds while the REST frontend
and Spark Connect are off, because those are what the metadata store serves.

## Configuration

`kyuubi-defaults.conf`:

```properties
# Only Thrift: REST and Spark Connect would bring the metadata store back.
kyuubi.frontend.protocols=THRIFT_BINARY

# Any value at all: an empty one makes the server start an embedded ZooKeeper.
kyuubi.ha.addresses=pod
kyuubi.ha.client.class=org.apache.kyuubi.plugin.kubernetes.PodDiscoveryClient
# The registry; the server and its engines read the same value. Defaults to
# ${java.io.tmpdir}/kyuubi-discovery.
kyuubi.ha.pod.dir=/tmp/kyuubi-discovery

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

# Engines are separate JVMs and need the plugin too. One line per engine type in use.
kyuubi.engine.trino.extra.classpath=/opt/kyuubi/plugins/*
kyuubi.engine.jdbc.extra.classpath=/opt/kyuubi/plugins/*
```

The jar goes into the server's classpath (`$KYUUBI_HOME/jars`) and into the directory the
`extra.classpath` lines name. The engines only load `PodDiscoveryClient`; the Kubernetes client
the advisor uses is on the server's classpath already.

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

## The registry

Under `kyuubi.ha.pod.dir`, a directory per discovery path, the same paths the ZooKeeper client
uses. A node is a directory: its data in `.data`, and for an ephemeral node the process that owns
it in `.owner` as pid and start time. A node whose owner has exited is dropped the next time its
namespace is read, which is what stands in for ZooKeeper's session: a crashed engine is forgotten
when the server next looks. An engine whose node is deleted - by the server giving up on it, or by
`kyuubi-ctl delete engine` - stops itself, as it would when ZooKeeper told it so. Locks are file
locks, one directory per lock path.

The registry does not outlive the pod, and does not need to: nothing in it is worth keeping.

## Not covered

- Engines that run outside the pod - Spark in cluster mode - register from another pod and cannot
  be seen through this registry. They need a registry the pods share, which is a different
  `DiscoveryClient` behind the same `kyuubi.ha.client.class`.
- `kyuubi-defaults.conf` itself is still read once, at start. What changes without a restart is
  what the Services declare.
