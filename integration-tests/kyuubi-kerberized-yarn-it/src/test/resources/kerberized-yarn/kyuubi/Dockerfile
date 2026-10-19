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

# Runtime of a Kyuubi server built from the repository: the Kyuubi jars and the Spark
# distribution are mounted. kinit is required by the Kyuubi server in a kerberized cluster, and
# openssl verifies the TLS listeners of the engines.

ARG JRE_IMAGE=eclipse-temurin:17-jre-noble

FROM ${JRE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-user openssl procps \
    && rm -rf /var/lib/apt/lists/* \
    && for tool in kinit klist openssl ps; do command -v "${tool}" || exit 1; done \
    && useradd --create-home --uid 1001 kyuubi

USER kyuubi
