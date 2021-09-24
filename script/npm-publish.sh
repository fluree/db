#!/usr/bin/env bash

version=$(clojure -M:meta version)

if [[ $version =~ -rc\d+$ || $version =~ -beta\d+$ ]]; then
  npm publish --tag=beta
else
  npm publish
fi
