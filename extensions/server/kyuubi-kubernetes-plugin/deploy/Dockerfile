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

# A Kyuubi server laid out as build/dist lays it out, from what setup.sh stages:
# the server's jars, the Trino and JDBC engines, beeline, and the plugin.
FROM eclipse-temurin:17-jre

ENV KYUUBI_HOME=/opt/kyuubi \
    KYUUBI_LOG_DIR=/opt/kyuubi/logs \
    KYUUBI_PID_DIR=/opt/kyuubi/pid \
    KYUUBI_WORK_DIR_ROOT=/opt/kyuubi/work

RUN useradd -u 10009 -g root -m -d /home/kyuubi kyuubi && \
    mkdir -p /opt/kyuubi/logs /opt/kyuubi/pid /opt/kyuubi/work && \
    chown -R 10009:0 /opt/kyuubi && chmod -R g+rwX /opt/kyuubi
# Owner and mode set while copying: a chown afterwards would copy every file into a
# layer of its own and double the image.
COPY --chown=10009:0 --chmod=0775 stage/ /opt/kyuubi/

WORKDIR /opt/kyuubi
USER 10009
CMD ["./bin/kyuubi", "run"]
