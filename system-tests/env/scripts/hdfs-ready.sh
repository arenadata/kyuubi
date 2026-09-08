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
set -euo pipefail

FS="${HDFS_FS:-hdfs://namenode:8020}"

hdfs dfs -fs "${FS}" -mkdir -p \
  /tmp/hive \
  /user/hive/warehouse \
  /user/kyuubi \
  /user/spark \
  /user/anonymous

hdfs dfs -fs "${FS}" -chmod 777 \
  /tmp \
  /tmp/hive \
  /user \
  /user/hive \
  /user/hive/warehouse \
  /user/kyuubi \
  /user/spark \
  /user/anonymous

exec tail -f /dev/null
