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

# The Hadoop daemons keep the Java 8 runtime of the image. YARN containers of Spark run with the
# Java 17 runtime under /opt/java17, since the Kyuubi engine jar targets Java 17. The Hadoop image
# is only published for linux/amd64, so the runtime is taken from the linux/amd64 manifest of the
# JRE image, which every builder resolves to the same architecture.

ARG HADOOP_IMAGE=apache/hadoop:3.4.1
ARG JRE_IMAGE=eclipse-temurin:17-jre-jammy@sha256:6faecf4bbb90d538fd739b8dd7ba7c3fb69bc58905b654a3d112452b83e877cb

FROM ${JRE_IMAGE} AS jre

FROM ${HADOOP_IMAGE}

COPY --from=jre /opt/java/openjdk /opt/java17

RUN /opt/java17/bin/java -version
