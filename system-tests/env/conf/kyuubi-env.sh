#!/usr/bin/env bash

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

export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf
# Client homes mounted from spark3/spark4/hive volume providers (ADH-like paths)
export SPARK_HOME=/usr/lib/spark3
export SPARK_CONF_DIR=/etc/spark3/conf
export HIVE_HOME=/usr/lib/hive
export KYUUBI_JAVA_OPTS="-Djava.security.krb5.conf=/dev/null"
