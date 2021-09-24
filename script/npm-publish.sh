#!/usr/bin/env bash

set -eux

version=$(clojure -M:meta version)

if [[ $version =~ -rc[0-9]+$ || $version =~ -beta[0-9]+$ ]]; then
  npm publish --tag=beta
else
  npm publish
fi
