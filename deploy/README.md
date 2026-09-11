# Deploying the routing gateway

A single entry point in front of Trino clusters running in Kubernetes. Clients
connect over HS2 — binary or HTTP — and each session is routed to a cluster
chosen by the authenticated identity. No engine is launched: sessions are opened
against clusters that already exist, so results cross one intermediary rather
than two.

## Prerequisites

| | |
|---|---|
| Docker with BuildKit | the image is built in it, and so is the project |
| A container registry | `localhost:5001` here; reachable under the same name from the nodes |
| A Kubernetes cluster | kind is enough; `kubectl` pointed at it |
| A warm `~/.m2` | the build reads it rather than fetching a gigabyte again |

No JDK is needed on the host. If `~/.m2` is cold, run the module build once
first — see the module README's Building section — or the image build will fail
offline with an unresolvable artifact.

## Quick start

```bash
kubectl apply -f deploy/namespace.yaml -f deploy/rbac.yaml
./deploy/build.sh                                  # builds, pushes to localhost:5001
kubectl apply -f deploy/deployment.yaml
kubectl apply -f deploy/clusters-example.yaml      # optional: two example clusters
kubectl -n gateway rollout status deploy/kyuubi-routing-gateway
```

## What is here

| File | |
|---|---|
| `namespace.yaml` | the namespace everything lives in |
| `rbac.yaml` | ServiceAccount, Role, RoleBinding |
| `deployment.yaml` | ConfigMap, Deployment, Service |
| `clusters-example.yaml` | example cluster declarations |
| `Dockerfile` | build and runtime image |
| `Dockerfile.dockerignore` | build context, read in preference to the repo's own |
| `build.sh` | builds and pushes |

## The image

Two stages. The first builds with Maven against the laptop's `~/.m2`, passed in
as a build context and bind-mounted read-write: Kyuubi's dependency tree is
about a gigabyte, so a build that fetched it would take far longer than the
compile, every time. `rw` makes the mount an overlay, so a build cannot corrupt
the cache it was handed, and `-o` turns a missing artifact into an immediate,
legible failure rather than a slow reach for a remote that may not be reachable.

The second stage is distroless and runs as uid 65532. The gateway is a classpath
launch rather than a shaded jar, so a vulnerable dependency shows up in an image
scan under its own name instead of being buried inside one artifact.

`Dockerfile.dockerignore` keeps every `pom.xml` — the root pom is a reactor over
all of them and Maven refuses to start if one is missing — and drops the other
modules' sources, which come from the mounted repository already built.

Overrides:

```bash
REGISTRY=registry.example.com TAG=v1 M2_DIR=/some/.m2 ./deploy/build.sh
```

The tag defaults to the short commit hash, and `latest` is pushed alongside it.

## Configuration

Everything is in the ConfigMap **`kyuubi-routing-gateway`**, defined at the top
of `deployment.yaml`. It holds one key, `kyuubi-defaults.conf`, mounted read-only
at `/etc/kyuubi` through the `config` volume and found by the gateway through
`KYUUBI_CONF_DIR`. The gateway reads it **once, at startup**.

Two objects in this namespace share that name, which is worth knowing before
reading a log line about either:

| | Holds | Created by |
|---|---|---|
| ConfigMap `kyuubi-routing-gateway` | `kyuubi-defaults.conf`, the gateway's own configuration | you, from `deployment.yaml` |
| Secret `kyuubi-gw-<cluster>-<hash>` | the shared reservation ledger, one per cluster | the gateway, only with `admission.shared` |

Nothing but the gateway touches the Secrets, and they are Secrets rather than
ConfigMaps because the ledger says which clusters are busy and how much is
running on each - a map of the platform's load.

### Frontends

| Key | Value | |
|---|---|---|
| `kyuubi.frontend.protocols` | `THRIFT_BINARY,THRIFT_HTTP` | a protocol the gateway does not implement is a startup error, not a warning |
| `kyuubi.frontend.thrift.binary.bind.port` | `10009` | |
| `kyuubi.frontend.thrift.http.bind.port` | `10010` | |
| `kyuubi.frontend.thrift.http.path` | `cliservice` (default, not set here) | the client must use the same path |

### Discovery

| Key | Default | |
|---|---|---|
| `kyuubi.gateway.engine` | `trino` | one gateway serves one engine |
| `kyuubi.gateway.cluster.resolver` | `static` | `kubernetes` here |
| `kyuubi.gateway.kubernetes.namespace` | all | restrict the search |
| `kyuubi.gateway.kubernetes.labelSelector` | `kyuubi.gateway/enabled=true` | which Services are clusters |
| `kyuubi.gateway.kubernetes.pollInterval` | `10000` | ms between polls |

### Metrics

On their own port, so the gateway can be scraped without exposing anything
else. There is no admin REST endpoint.

| Key | Value | |
|---|---|---|
| `kyuubi.metrics.enabled` | `true` | |
| `kyuubi.metrics.reporters` | `PROMETHEUS` | |
| `kyuubi.metrics.prometheus.port` | `10019` | also carries the probes |
| `kyuubi.metrics.prometheus.path` | `/metrics` | |

### Admission, scaling and the rest

All off by default. Each is covered under *Optional features* below.

## Declaring a cluster

A cluster is a Service carrying the selector label and the annotations. Nothing
else registers one — there is no operator of the gateway's own and no file
listing backends — so adding a cluster means adding a Service, and removing one
means deleting it.

Discovery reads annotations and never connects, so a Service pointing at a host
that does not exist is still a cluster as far as the gateway is concerned. Only
a query needs something behind it.

| | |
|---|---|
| **label** `kyuubi.gateway/enabled: "true"` | without it the Service is invisible |
| `kyuubi.gateway/engine` | **required** — its presence is what registers the cluster |
| `kyuubi.gateway/users` | comma-separated; who may be routed here |
| `kyuubi.gateway/default` | where users with no cluster of their own land |
| `kyuubi.gateway/scheme` | `http` by default |
| `kyuubi.gateway/port` | a number or a port name; else the Service's first port |
| `kyuubi.gateway/session.<key>` | session configuration for anyone routed here |
| `kyuubi.gateway/max-memory-per-node-bytes` | capacity — all three or none |
| `kyuubi.gateway/workers` | |
| `kyuubi.gateway/max-workers` | |
| `kyuubi.gateway/scale-target` | the custom resource the gateway may resize |
| `kyuubi.gateway/scale-group` | `trino.arenadata.io` by default |
| `kyuubi.gateway/scale-version` | `v1alpha1` by default |
| `kyuubi.gateway/scale-plural` | `clusters` by default |

A user in no cluster's list, with no default declared, is **refused** — never
redirected. A routing mistake must not become an access-control bypass.

Capacity is used only when all three of its annotations are present and
consistent: a partial declaration would have the gateway size queries against a
ceiling it invented.

Mind the spelling inside `session.`: Trino's connection settings are
`kyuubi.session.engine.trino.connection.*`. The other spelling is accepted
silently and fails late with "Trino default catalog can not be null!".

## Permissions

The Role grants only what the enabled features need:

| Rule | Needed for | |
|---|---|---|
| `services: get, list` | discovery | always |
| `secrets: get, list, create, update, delete` | the shared reservation ledger | only with `admission.shared` |
| `clusters/scale: get, update, patch` | resizing a cluster | only with `scaling.enabled` |
| `clusters: get` | reaching the scale subresource | only with `scaling.enabled` |

A Role, not a ClusterRole: the gateway discovers clusters in its own namespace,
and watching every namespace would mean reading every Secret in the cluster. The
scale *subresource* rather than the resource, so a gateway that can resize a
cluster still cannot change its image, its catalogs or its credentials.

Drop the rules you do not need. One replica with admission off needs only the
first.

Check what the account can actually do:

```bash
kubectl auth can-i --list \
  --as=system:serviceaccount:gateway:kyuubi-routing-gateway -n gateway
```

## Connecting

```bash
kubectl -n gateway port-forward svc/kyuubi-routing-gateway 10009:10009 10010:10010
```

| Transport | URL |
|---|---|
| binary HS2 | `jdbc:hive2://<host>:10009/` |
| HTTP HS2 | `jdbc:hive2://<host>:10010/;transportMode=http;httpPath=cliservice` |

The user in the connection is the routing key. Against the example clusters,
`alice` and `bob` reach `trino-analytics`, `carol` reaches `trino-etl`, and
`eve` — in nobody's list — lands on `trino-etl` because it is the default.

Any HS2 client works: beeline, the Kyuubi or Hive JDBC driver, and BI tools that
speak either transport.

## Verifying

```bash
kubectl -n gateway get pods
kubectl -n gateway logs deploy/kyuubi-routing-gateway | grep "Routing gateway started"
```

Then watch discovery:

```bash
kubectl -n gateway port-forward svc/kyuubi-routing-gateway 10019:10019
curl -s localhost:10019/metrics | grep kyuubi_gateway
```

| Metric | |
|---|---|
| `kyuubi_gateway_discovery_clusters` | how many clusters are known |
| `kyuubi_gateway_discovery_revision` | moves on **any** change to the set |
| `kyuubi_gateway_discovery_refresh_failures` | polls that failed |
| `kyuubi_gateway_cluster_<ns>_<name>_*` | one group per cluster |

The revision is the one to alert on. A count cannot show an edit that leaves the
count alone — renaming a user, correcting a port, changing a capacity — and
those change where queries go just as much as adding a cluster does. Watching
the revision needs no knowledge of what the set is supposed to contain.

`refresh_failures` matters because a failed poll keeps serving the last good
set: routing stays up, quietly stale, and nothing else says so.

To see a change land, edit an annotation and watch both move:

```bash
kubectl -n gateway annotate svc trino-analytics kyuubi.gateway/users=alice,bob,eve --overwrite
sleep 12
curl -s localhost:10019/metrics | grep -E "discovery_revision|trino_analytics_users"
```

## Optional features

Each is off by default, and each should be turned on deliberately: they change
what clients see, or they write to Kubernetes.

### Admission

```properties
kyuubi.gateway.admission.enabled        true
kyuubi.gateway.admission.policy         PackByMemory      # or Exclusive
kyuubi.gateway.sizing.memoryFactor      1.5
kyuubi.gateway.sizing.defaultWorkers    2
kyuubi.gateway.admission.holdTimeout    0                 # ms to wait on a busy cluster
```

Needs the three capacity annotations on each cluster; a cluster without them is
admitted unaccounted, with a warning.

`holdTimeout` is 0 because a held query holds the frontend thread carrying it: a
gateway that waits by default would let one busy cluster exhaust the thrift
handler pool and stop answering for every other cluster too. Size it against
that pool.

### Scaling

```properties
kyuubi.gateway.scaling.enabled          true
```

Needs `clusters/scale` in the Role, and the `scale-target` annotation on each
cluster that may be resized. **It also needs the CRD to declare
`subresource:scale`** — without it the subresource does not exist and every
scale-up fails, which shows as queries refused with `NeedsScaleUp`.

Only upwards. Lowering a replica count deletes pods, and a Trino worker that
disappears takes its query fragments with it.

### Reconciliation

```properties
kyuubi.gateway.reconcile.enabled        true
kyuubi.gateway.reconcile.interval       30000
kyuubi.gateway.reconcile.grace          60000
kyuubi.gateway.reconcile.user           gateway
```

Reclaims reservations whose queries are over, by asking each coordinator's
`/v1/query` what it is actually running. Needs no Kubernetes rights, but a
cluster with authentication enabled has to permit `reconcile.user` to read that
endpoint. The same pass measures what queries used against what was reserved and
logs the `memoryFactor` that would have been right.

### More than one replica

```properties
kyuubi.gateway.admission.shared          true
kyuubi.gateway.admission.sharedNamespace gateway
```

The ledger becomes a Secret per cluster, named `kyuubi-gw-<cluster>-<hash>`,
created and deleted by the gateway as clusters come and go. Admission is a
compare-and-swap on its `resourceVersion`, so replicas contend rather than
coordinate: no leader, and nothing to release when one dies.

Required before raising `replicas`. Without it each replica admits against its
own ledger and together they overcommit every cluster — the exact overcommit the
gate exists to prevent. Needs the `secrets` rule in the Role.

### JDBC clusters (Impala)

```properties
kyuubi.gateway.engine                     impala
kyuubi.gateway.jdbc.impersonationTemplate ;hive.server2.proxy.user={user}
```

One gateway serves one engine, so Impala needs a second Deployment with its own
ConfigMap. Impersonation is **on** by default: without it every query reaches
Impala as the gateway's own account. The cluster must list the gateway's
principal in `--authorized_proxy_user_config`, or every session fails — loudly,
which is the point.

## Updating

```bash
# configuration: the gateway reads it once, at startup
kubectl -n gateway rollout restart deploy/kyuubi-routing-gateway

# image
./deploy/build.sh && kubectl -n gateway rollout restart deploy/kyuubi-routing-gateway
```

`imagePullPolicy: Always` with the `latest` tag means a restart picks up a new
push. Pin a real tag for anything that has to be reproducible.

## Troubleshooting

**The pod starts but routes nowhere, and the log shows only built-in defaults.**
The configuration was not read. The chain is ConfigMap `kyuubi-routing-gateway`
-> key `kyuubi-defaults.conf` -> volume `config` -> `/etc/kyuubi` ->
`KYUUBI_CONF_DIR`; a break anywhere in it leaves the gateway on its defaults,
started and healthy and routing nowhere. Check the whole chain at once:

```bash
kubectl -n gateway get cm kyuubi-routing-gateway -o jsonpath='{.data}' | head -c 200
kubectl -n gateway get pod -l app.kubernetes.io/name=kyuubi-routing-gateway \
  -o jsonpath='{.items[0].spec.containers[0].env}{"\n"}{.items[0].spec.volumes}'
```

The log is the quickest tell: with this ConfigMap applied the HTTP frontend
starts, so `Routing gateway started with frontends: KyuubiTBinaryFrontend,
KyuubiTHttpFrontendService` means the file was read. One frontend means it was
not.

Nothing reloads configuration on its own, and an edit to the ConfigMap is
invisible to the Deployment - it needs a rollout restart.

**`Published 0 clusters` and `discovery_clusters` is 0.** The Services are
missing the label, are in another namespace than
`kyuubi.gateway.kubernetes.namespace`, or have no `kyuubi.gateway/engine`
annotation — presence of that annotation is what registers a cluster.

**A session opens but the first query fails with "Trino default catalog can not
be null!".** The catalog was set as `kyuubi.engine.trino.connection.catalog`. It
is `kyuubi.session.engine.trino.connection.catalog`.

**Sessions are refused with "No cluster is allowed for user X".** X is in no
cluster's `users` list and no cluster is `default`. That is the intended answer,
not a bug: refusing is what keeps a routing mistake from becoming an
access-control bypass.

**Queries are refused with `NeedsScaleUp` while scaling is on.** Either the
cluster has no `scale-target` annotation, or the CRD declares no `scale`
subresource, or the Role is missing `clusters/scale`.

**A query hangs and then fails with "No nodes available to run query".** The
query was admitted for more workers than the cluster has, and
`required_workers_count` held it. That is the mechanism working; the cluster
could not grow. Check `max-workers` and whether scaling is on.

**`ImagePullBackOff`.** The nodes must reach the registry under the same name
the image uses. With kind and a local registry that means `localhost:5001`
resolving inside the node, not only on the laptop.

## Uninstall

```bash
kubectl delete -f deploy/clusters-example.yaml --ignore-not-found
kubectl delete -f deploy/deployment.yaml --ignore-not-found
kubectl delete -f deploy/rbac.yaml --ignore-not-found
kubectl delete -f deploy/namespace.yaml --ignore-not-found
```

Shared ledgers are Secrets in the gateway's namespace and go with it. They hold
only in-flight reservations, so losing them costs nothing once the gateway is
gone.
