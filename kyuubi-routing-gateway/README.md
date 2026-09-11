# Kyuubi Routing Gateway

A gateway that routes each session to an **already running** Trino cluster chosen
by the authenticated identity, instead of launching an engine process.

Composition is the whole design — see `RoutingGateway`:

```scala
override val backendService = new RoutingBackendService()   // routes by identity
override lazy val frontendServices = ...                    // stock Kyuubi frontends
```

Because no engine is launched, query results traverse one intermediary rather
than two. That matters when the gateway also carries bulk traffic.

## Routing

`RoutingSessionManager` resolves the cluster for the user and puts its address
into the session conf. `TrinoSessionImpl` reads the connection url from there,
so the Trino session and operation layer is reused unmodified.

Resolvers are selected with `kyuubi.gateway.cluster.resolver`:

* `static` — clusters declared in configuration, see `StaticClusterResolver`
* `kubernetes` — clusters discovered from core Services, see `KubernetesClusterResolver`

A user with no allowed cluster is rejected, never redirected to a default:
a routing mistake must not become an access control bypass.

## Capacity

Neither engine binds a query to a subset of workers - every query spreads over
every active node - so "reserve workers for this query" is not expressible
inside them. What is expressible is accounting, which is what `capacity/` does:
track what a cluster can hold, subtract what is already admitted, admit only
when the remainder covers it.

Memory is the tracked quantity because it is the one the engine enforces. CPU is
not: an estimate gives work, not power, so dividing it by capacity yields a
duration rather than a requirement.

Denials are three cases rather than a boolean, because they call for different
responses:

| Denial | Meaning | What helps |
|---|---|---|
| `Busy` | capacity exists but is taken | waiting |
| `NeedsScaleUp(n)` | cluster idle but too small | scaling to `n` |
| `TooLarge(n, max)` | beyond the ceiling | neither |

`AdmissionPolicy` picks how a cluster is shared. `PackByMemory` admits while
memory is left, which is safe for memory but not for time - co-resident queries
share CPU and both slow down. `Exclusive` runs one query at a time: predictable
duration, idle gaps.

## Sizing

`sizing/` reads `EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)`. The parser walks the
document for `estimates` arrays rather than following a path, because the
top-level shape differs between plan types and is not a stable contract across
Trino versions.

Trino writes an unestimatable value as the quoted string `"NaN"`, typically when
table statistics are missing. That reads as *unknown*, never as zero: sizing a
query as free would put an unbounded one on a minimal cluster and fail it on
memory.

`memoryFactor` is the calibration knob and its default is a guess. The planner's
per-operator estimates are not the query's peak - the peak lies between the
largest operator and the sum of the live ones. Turning the guess into a number
needs these estimates compared against the `peakUserMemoryBytes` of finished
queries.

## Known limits

* Reservations live in memory, so the accountant is correct for one process.
  Several replicas admitting against one cluster would each see only their own
  reservations and together overcommit it.
* Release depends on the caller. `staleReservations` exists because a missed
  release would otherwise shrink a cluster's apparent capacity permanently; the
  precise signal is Trino's `EventListener`, which is not wired up yet.
* One gateway instance serves one engine - see `RoutingSessionManager` for why
  the interfaces make a single instance serving both impractical.

## Building

No JDK is needed on the host; the build runs in a container. A warm `~/.m2`
makes repeat builds fast, so mount it rather than a throwaway volume.

```bash
docker run --rm -u "$(id -u):$(id -g)" \
  -e HOME=/var/maven -e MAVEN_CONFIG=/var/maven/.m2 \
  -v "$HOME/.kyuubi-build-home:/var/maven" \
  -v "$HOME/.m2:/var/maven/.m2" \
  -v "$PWD:/src" -w /src \
  maven:3.9-eclipse-temurin-17 \
  mvn -B -Duser.home=/var/maven -pl kyuubi-routing-gateway test
```

Four things that are easy to get wrong:

* `/var/maven` must be a writable mount, not just `/var/maven/.m2` — the Scala
  plugin writes its zinc cache to `$HOME/.sbt`.
* `MAVEN_CONFIG` and `-Duser.home` are both required when running non-root,
  otherwise `settings.xml` is ignored and nothing is cached on the host.
* The root pom points Maven Central at a GCS Asia mirror that measured ~1.9 MB/s
  here against ~7.2 MB/s from repo1. A `<mirror>` for `central,gcs-maven-central-mirror`
  in `~/.m2/settings.xml` fixes it. Do not use `mirrorOf=*` — the Arenadata
  repositories are not a Central mirror and hold different artifacts.
* The first build needs `-am install` to populate dependencies. Add
  `-DskipTests -Dmaven.test.skip=true -Dspark.archive.download.skip=true
  -Dflink.archive.download.skip=true -Dhive.archive.download.skip=true`:
  the external archives are ~1 GB and unrelated to this module, and
  `kyuubi-server` test sources do not compile without the Spark distribution.
