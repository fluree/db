#!/usr/bin/env bash

set -e

VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)

if [[ "$VERSION" == *"-SNAPSHOT" ]]; then
  mvn deploy:deploy-file -DpomFile=pom.xml -Dfile=target/fluree-db.jar -DrepositoryId=deps.co-snapshots -Durl=https://repo.deps.co/fluree/snapshots
else
  mvn deploy:deploy-file -DpomFile=pom.xml -Dfile=target/fluree-db.jar -DrepositoryId=deps.co-releases -Durl=https://repo.deps.co/fluree/releases
fi
