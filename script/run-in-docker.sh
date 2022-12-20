#!/usr/bin/env bash

set -e

image=fluree/${PWD##*/}

echo "Running in ${image} container..."

export DOCKER_BUILDKIT=1
docker build --cache-from type=gha --quiet --load --tag "${image}" .
docker run --security-opt seccomp=docker-chrome-seccomp.json --rm "${image}" "$@"
