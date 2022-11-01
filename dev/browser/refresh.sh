#!/usr/bin/env sh

rm -rf node_modules
npm install
clj -M:serve :port 1339 :dir "."
