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
first - see the Building section of
[`kyuubi-routing-gateway/README.md`](../kyuubi-routing-gateway/README.md) - or
the image build will fail offline with an unresolvable artifact.

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
| `namespace.yaml` | the namespace `gateway`, which everything below lives in |
| `rbac.yaml` | ServiceAccount, Role and RoleBinding, all named `kyuubi-routing-gateway` |
| `deployment.yaml` | ConfigMap, Deployment and the gateway's **own** Service, same name again |
| `clusters-example.yaml` | Services that **declare clusters** - a different use of Service entirely |
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

**Six objects are called `kyuubi-routing-gateway`** - the ServiceAccount, Role,
RoleBinding, ConfigMap, Deployment and Service - so a sentence about "the
gateway's ConfigMap" is only unambiguous because of the kind, not the name.
`kubectl -n gateway get sa,role,rolebinding,cm,deploy,svc` lists them together.

The reservation ledger is **not** among them. It is a Secret per cluster named
`kyuubi-gw-<cluster>-<hash>`, created by the gateway itself and only when
`admission.shared` is on:

| | Kind | Holds | Created by |
|---|---|---|---|
| `kyuubi-routing-gateway` | ConfigMap | `kyuubi-defaults.conf`, the gateway's configuration | you, from `deployment.yaml` |
| `kyuubi-gw-<cluster>-<hash>` | Secret | the shared reservation ledger | the gateway |

A Secret rather than a ConfigMap because the ledger says which clusters are busy
and how much is running on each - a map of the platform's load.

### Frontends

The value column is what this ConfigMap sets.

| Key | Value | |
|---|---|---|
| `kyuubi.frontend.protocols` | `THRIFT_BINARY,THRIFT_HTTP` (default `THRIFT_BINARY,REST`) | see below |
| `kyuubi.frontend.thrift.binary.bind.port` | `10009` | |
| `kyuubi.frontend.thrift.http.bind.port` | `10010` | |
| `kyuubi.frontend.thrift.http.path` | `cliservice` (default, not set here) | the client must use the same path |

`kyuubi.frontend.protocols` has to be set. Kyuubi's default is
`THRIFT_BINARY,REST`, and the
gateway implements no REST frontend - it would log
`Frontend protocol REST is not wired in the gateway yet, ignoring` and come up
with the binary frontend alone. Naming a list of protocols the gateway
implements none of is a startup error rather than a warning, so a typo here
fails loudly instead of leaving a server listening on nothing.

### Discovery

The value column is what this ConfigMap sets; where it differs from the
built-in default, both are given.

| Key | Value | |
|---|---|---|
| `kyuubi.gateway.engine` | `trino` (also the default) | one gateway serves one engine |
| `kyuubi.gateway.cluster.resolver` | `kubernetes` (default `static`) | |
| `kyuubi.gateway.kubernetes.namespace` | `gateway` (default: every namespace) | |
| `kyuubi.gateway.kubernetes.labelSelector` | `kyuubi.gateway/enabled=true` (also the default) | which Services are clusters |
| `kyuubi.gateway.kubernetes.pollInterval` | `10000` (also the default) | ms between polls |

### Metrics

On their own port, so the gateway can be scraped without exposing anything
else. There is no admin REST endpoint.

| Key | Value | |
|---|---|---|
| `kyuubi.metrics.enabled` | `true` | |
| `kyuubi.metrics.reporters` | `PROMETHEUS` | |
| `kyuubi.metrics.prometheus.port` | `10019` | also carries the probes |
| `kyuubi.metrics.prometheus.path` | `/metrics` | |

### What this ConfigMap leaves off

Admission, scaling, reconciliation and the shared ledger are all off, and the
file says so explicitly rather than relying on defaults. Each is covered under
*Optional features* below.

JDBC impersonation is the one thing that is **on** by default, and it only
applies to a gateway serving an engine over JDBC - Impala here, not Trino.

## Declaring a cluster

A cluster is a Service carrying the label `kyuubi.gateway/enabled: "true"` and
the annotations below. Nothing else registers one - there is no operator of the
gateway's own and no file listing backends - so adding a cluster means adding a
Service, and removing one means deleting it.

The gateway's own Service, `kyuubi-routing-gateway`, is a Service in the same
namespace and is **not** a cluster: it carries no such label, so discovery never
sees it. That is the only thing keeping the gateway from discovering itself.

Discovery reads annotations and never connects, so a Service pointing at a host
that does not exist is still a cluster as far as the gateway is concerned. Only
a query needs something behind it.

| | |
|---|---|
| **label** `kyuubi.gateway/enabled: "true"` | without it the Service is invisible to discovery |
| `kyuubi.gateway/engine` | **required** — its presence is what registers the cluster |
| `kyuubi.gateway/users` | comma-separated list; who may be routed here |
| `kyuubi.gateway/default` | `"true"` marks where users with no cluster of their own land |
| `kyuubi.gateway/scheme` | `http` or `https`; `http` by default |
| `kyuubi.gateway/port` | a number or a port name; else the Service's first port |
| `kyuubi.gateway/session.<key>` | session configuration for anyone routed here |
| `kyuubi.gateway/max-memory-per-node-bytes` | capacity: bytes, the engine's own per-node query limit |
| `kyuubi.gateway/workers` | capacity: workers the cluster has now |
| `kyuubi.gateway/max-workers` | capacity: the ceiling it may be scaled to |
| `kyuubi.gateway/scale-target` | the custom resource the gateway may resize |
| `kyuubi.gateway/scale-group` | `trino.arenadata.io` by default |
| `kyuubi.gateway/scale-version` | `v1alpha1` by default |
| `kyuubi.gateway/scale-plural` | `clusters` by default |

A user in no cluster's list, with no default declared, is **refused** — never
redirected. A routing mistake must not become an access-control bypass.

Capacity is used only when all three of the capacity annotations above are
present and `max-workers` is at least `workers`. A partial declaration would
have the gateway size queries against a ceiling it invented, so it is ignored
entirely and the cluster is admitted to unaccounted, with a warning in the log.

Mind the spelling inside `session.`: Trino's connection settings are
`kyuubi.session.engine.trino.connection.*`. The other spelling is accepted
silently and fails late with "Trino default catalog can not be null!".

## Permissions

The Role `kyuubi-routing-gateway` in namespace `gateway` grants only what the
enabled features need:

| Resource | API group | Verbs | Needed for | |
|---|---|---|---|---|
| `services` | core (`""`) | `get`, `list` | discovery | always |
| `secrets` | core (`""`) | `get`, `list`, `create`, `update`, `delete` | the shared reservation ledger | only with `admission.shared` |
| `clusters/scale` | `trino.arenadata.io` | `get`, `update`, `patch` | resizing a cluster | only with `scaling.enabled` |
| `clusters` | `trino.arenadata.io` | `get` | reaching the scale subresource | only with `scaling.enabled` |

A Role, not a ClusterRole: the gateway discovers clusters in its own namespace,
and watching every namespace would mean reading every Secret in the cluster. The
scale *subresource* rather than the resource, so a gateway that can resize a
cluster still cannot change its image, its catalogs or its credentials.

Drop the rules you do not need. One replica with admission off needs only the
first.

Check what the ServiceAccount `kyuubi-routing-gateway` can actually do:

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

`<host>` is `localhost` behind the port-forward above, and
`kyuubi-routing-gateway.gateway.svc.cluster.local` - or just
`kyuubi-routing-gateway.gateway` - from inside the cluster.

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

A cluster's name is `<namespace>/<service>`, lowercased with everything that is
not a letter or digit turned into `_`, so the Service `trino-analytics` in
`gateway` appears as `kyuubi_gateway_cluster_gateway_trino_analytics_*`. The
suffixes are `up`, `users`, `default`, `scalable`, and - when the cluster
declares capacity - `workers`, `max_workers`, `max_memory_per_node_bytes`.

The revision is the one to alert on. A count cannot show an edit that leaves the
count alone — renaming a user, correcting a port, changing a capacity — and
those change where queries go just as much as adding a cluster does. Watching
the revision needs no knowledge of what the set is supposed to contain.

`refresh_failures` matters because a failed poll keeps serving the last good
set: routing stays up, quietly stale, and nothing else says so.

To see a change land, edit an annotation and watch both move:

```bash
kubectl -n gateway annotate svc trino-analytics kyuubi.gateway/users=alice,bob,eve --overwrite
sleep 12   # one poll interval plus a little - see kyuubi.gateway.kubernetes.pollInterval
curl -s localhost:10019/metrics | grep -E "discovery_revision|trino_analytics_users"
```

`discovery_revision` goes up by one and `..._users` from 2 to 3, while
`discovery_clusters` stays put - which is the case a count alone cannot show.

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

Needs `clusters/scale` in the Role `kyuubi-routing-gateway`, and the
`kyuubi.gateway/scale-target` annotation on each cluster Service that may be
resized. **It also needs the CRD `clusters.trino.arenadata.io` to declare
`subresource:scale`** - without it the subresource does not exist and every
scale-up fails, which shows as queries refused with `NeedsScaleUp`.

Only upwards. Lowering a replica count deletes pods, and a Trino worker that
disappears takes its query fragments with it.

### Reconciliation

```properties
kyuubi.gateway.reconcile.enabled        true
kyuubi.gateway.reconcile.interval       30000
kyuubi.gateway.reconcile.grace          60000
kyuubi.gateway.reconcile.user           kyuubi-reconciler
```

Reclaims reservations whose queries are over, by asking each coordinator's
`/v1/query` what it is actually running.

`reconcile.user` is a **Trino** user sent as `X-Trino-User`, not a Kubernetes
identity and nothing to do with the namespace. It defaults to the OS user of the
gateway process, which inside this image is uid 65532 with no name worth
sending, so set it. A cluster with authentication enabled has to permit it to
read that endpoint; no Kubernetes rights are involved either way.

The same pass measures what queries actually used against what was reserved and
writes an INFO line per cluster - `Calibration for <cluster> over N queries` -
with the `memoryFactor` that would have been right. It reports only; setting the
factor stays a decision.

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
gate exists to prevent. Needs the `secrets` rule in the Role
`kyuubi-routing-gateway`.

### JDBC clusters (Impala)

```properties
kyuubi.gateway.engine                     impala
kyuubi.gateway.jdbc.impersonationTemplate ;hive.server2.proxy.user={user}
```

One gateway serves one engine, so Impala needs a second Deployment - and every
object in `rbac.yaml` and `deployment.yaml` is named `kyuubi-routing-gateway`,
so copying them verbatim overwrites the Trino gateway rather than adding to it.
Rename all of them, the Service included, and give the new ConfigMap its own
frontend ports if both are to be reachable on one host.

Impersonation is **on** by default: without it every query reaches Impala as the
account the gateway connects with, and Impala's per-user authorisation sees one
user for everybody. The Impala cluster must list that account in
`--authorized_proxy_user_config`, or every session fails - loudly, which is the
point.

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

**`Published 0 clusters` and `discovery_clusters` is 0.** The cluster-declaring
Services - `trino-analytics` and the like, not the gateway's own
`kyuubi-routing-gateway` - are missing the label
`kyuubi.gateway/enabled: "true"`, are in a namespace other than
`kyuubi.gateway.kubernetes.namespace`, or have no `kyuubi.gateway/engine`
annotation: presence of that annotation is what registers a cluster.

**A session opens but the first query fails with "Trino default catalog can not
be null!".** The catalog was set as `kyuubi.engine.trino.connection.catalog`. It
is `kyuubi.session.engine.trino.connection.catalog`.

**Sessions are refused with "No cluster is allowed for user X".** X is in no
cluster's `users` list and no cluster is `default`. That is the intended answer,
not a bug: refusing is what keeps a routing mistake from becoming an
access-control bypass.

**Queries are refused with `NeedsScaleUp` while scaling is on.** Either the
cluster's Service has no `kyuubi.gateway/scale-target` annotation, or the CRD
`clusters.trino.arenadata.io` declares no `scale` subresource, or the Role
`kyuubi-routing-gateway` is missing `clusters/scale`. Check the last two:

```bash
kubectl get crd clusters.trino.arenadata.io -o jsonpath='{.spec.versions[*].subresources}'
kubectl -n gateway get role kyuubi-routing-gateway -o jsonpath='{.rules}'
```

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
