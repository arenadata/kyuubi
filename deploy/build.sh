#!/usr/bin/env bash
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
# Builds and pushes the gateway image.
#
# The laptop's ~/.m2 is passed as a build context so the build reads the
# dependencies that are already there instead of fetching a gigabyte again.
set -euo pipefail

REGISTRY="${REGISTRY:-localhost:5001}"
IMAGE="${IMAGE:-$REGISTRY/kyuubi-routing-gateway}"
TAG="${TAG:-$(git -C "$(dirname "$0")/.." rev-parse --short HEAD)}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker buildx build \
  --file "$ROOT/deploy/Dockerfile" \
  --build-context "m2=${M2_DIR:-$HOME/.m2}" \
  --tag "$IMAGE:$TAG" \
  --tag "$IMAGE:latest" \
  --push \
  "$ROOT"

echo "pushed $IMAGE:$TAG"
