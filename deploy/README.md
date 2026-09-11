# Deploying the routing gateway

```bash
kubectl apply -f deploy/namespace.yaml -f deploy/rbac.yaml
./deploy/build.sh                       # builds, pushes to localhost:5001
kubectl apply -f deploy/deployment.yaml
kubectl apply -f deploy/clusters-example.yaml
```

## What is here

| File | |
|---|---|
| `namespace.yaml` | the namespace everything lives in |
| `rbac.yaml` | ServiceAccount, Role, RoleBinding |
| `deployment.yaml` | ConfigMap, Deployment, Service |
| `clusters-example.yaml` | example cluster declarations |
| `Dockerfile` | build and runtime image |
| `build.sh` | builds and pushes |

## The image

Two stages. The first builds with Maven against the laptop's `~/.m2`, passed in
as a build context and bind-mounted read-write: Kyuubi's dependency tree is
about a gigabyte, so a build that fetched it would take far longer than the
compile, every time. `rw` makes the mount an overlay, so a build cannot corrupt
the cache it was handed, and `-o` turns a missing artifact into an immediate
failure rather than a slow reach for a remote.

The second stage is distroless. The gateway is a classpath launch rather than a
shaded jar, so a vulnerable dependency shows up in an image scan under its own
name instead of being buried inside one artifact.

`deploy/Dockerfile.dockerignore` is read in preference to the repository's own,
so the upstream file stays untouched. It keeps every `pom.xml` — the root pom is
a reactor over all of them and Maven refuses to start if one is missing — and
drops the other modules' sources, which come from the mounted repository already
built.

## Permissions

The Role grants only what the enabled features need:

| Rule | Needed for | |
|---|---|---|
| `services: get, list` | discovery | always |
| `secrets: get, list, create, update, delete` | the shared reservation ledger | only with `admission.shared` |
| `clusters/scale: get, update, patch` | resizing a cluster | only with `scaling.enabled` |
| `clusters: get` | reaching the scale subresource | only with `scaling.enabled` |

A Role, not a ClusterRole: the gateway discovers clusters in its own namespace,
and watching every namespace would mean reading every Secret in the cluster.
The scale *subresource* rather than the resource, so a gateway that can resize a
cluster still cannot change its image, its catalogs or its credentials.

Drop the rules you do not need. Running one replica needs neither Secrets nor
`clusters`.

## Declaring a cluster

A cluster is a Service carrying the selector label and the annotations. Nothing
else registers one - there is no operator of the gateway's own and no file
listing backends - so adding a cluster is adding a Service.

Discovery reads annotations and never connects, so a Service pointing at a host
that does not exist is enough to watch the set change. Only a query needs a
cluster behind it.

## Watching discovery

Metrics are on their own port, so the gateway can be scraped without exposing
anything else:

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
count alone - renaming a user, correcting a port, changing a capacity - and
those change where queries go just as much as adding a cluster does. Watching
the revision needs no knowledge of what the set is supposed to contain.

`refresh_failures` matters because a failed poll keeps serving the last good
set: routing stays up, quietly stale, and nothing else says so.

## Changing the configuration

The gateway reads `kyuubi-defaults.conf` once, at startup:

```bash
kubectl -n gateway rollout restart deploy/kyuubi-routing-gateway
```
