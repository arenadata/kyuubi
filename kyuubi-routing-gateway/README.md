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
