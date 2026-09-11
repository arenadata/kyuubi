# syntax=docker/dockerfile:1.7
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
# Build with deploy/build.sh - it passes the laptop's ~/.m2 as a build context,
# which this file expects by the name `m2`.

FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /src
COPY . .

# The local repository is bind-mounted from the laptop rather than downloaded.
# Kyuubi's dependency tree is around a gigabyte, so a build that fetched it
# would take longer than the compile by an order of magnitude, every time.
#
# `rw` makes the mount a writable overlay: Maven gets to write its resolver
# bookkeeping, and none of it lands back in the real ~/.m2 - so a build cannot
# corrupt the cache it was given.
#
# `-o` because everything needed is already in that repository. It also turns a
# missing artifact into an immediate, legible failure instead of a slow reach
# for a remote that may not be configured here.
RUN --mount=type=bind,from=m2,target=/root/.m2,rw \
    mvn -B -o -pl kyuubi-routing-gateway \
        -DskipTests -Dmaven.test.skip=true \
        -Dspotless.check.skip=true -Dscalastyle.skip=true -Drat.skip=true \
        package dependency:copy-dependencies \
        -DoutputDirectory=/runtime/lib -DincludeScope=runtime && \
    cp kyuubi-routing-gateway/target/kyuubi-routing-gateway_*.jar /runtime/lib/

FROM gcr.io/distroless/java17-debian12:nonroot

COPY --from=build /runtime/lib /opt/gateway/lib

# The base image runs as 65532 already; stated so a reader does not have to know
# that, and so a change of base cannot quietly promote the gateway to root.
USER 65532:65532

EXPOSE 10009 10099

# Overridden because the base entrypoint is `java -jar`, and this is a classpath
# launch: the gateway is assembled from its module and the dependencies beside
# it rather than shaded into one jar, so that a CVE in a dependency can be seen
# in an image scan instead of being buried.
#
# MaxRAMPercentage rather than a fixed heap: the pod's limit is the only number
# that knows how much memory there is, and hard-coding one here would be wrong
# on every deployment that sets a different limit.
ENTRYPOINT ["/usr/bin/java", \
    "-XX:MaxRAMPercentage=70.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-cp", "/opt/gateway/lib/*", \
    "org.apache.kyuubi.gateway.RoutingGateway"]
