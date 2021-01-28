#!/usr/bin/env bash

set -e

image=fluree/${PWD##*/}

echo "Running in ${image} container..."

docker build --quiet --tag "${image}" .
docker run --security-opt seccomp=docker-chrome-seccomp.json --rm "${image}" "$@"
