#!/usr/bin/env bash

set -e

image=fluree/${PWD##*/}

echo "Running in ${image} container..."

export DOCKER_BUILDKIT=1
if [ "$GITHUB_ACTIONS" != "true" ]; then
  docker build --quiet --load --tag "${image}" .
fi
docker run --security-opt seccomp=docker-chrome-seccomp.json --rm "${image}" "$@"
