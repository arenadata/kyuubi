#!/usr/bin/env bash
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
