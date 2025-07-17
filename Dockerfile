FROM clojure:temurin-17-tools-deps-1.11.1.1165-bullseye-slim

RUN mkdir -p /usr/src/flureedb
WORKDIR /usr/src/flureedb

# Install the tools we need to install the tools we need
RUN apt-get update && apt-get install -y wget curl gnupg2 software-properties-common chromium
ENV CHROME_BIN=/usr/bin/chromium

# Add node PPA to get newer versions
RUN curl -sL https://deb.nodesource.com/setup_20.x | bash -
RUN apt-get update && apt-get install -y nodejs build-essential

COPY deps.edn Makefile ./
RUN make deps

COPY package.json ./
RUN npm install && npm install -g karma-cli

COPY . ./

RUN make jar

# Pre-build JavaScript SDKs to ensure dependencies are downloaded
RUN make out/fluree-node-sdk.js out/fluree-browser-sdk.js

# Create a user to own the fluree code
RUN groupadd fluree && useradd --no-log-init -g fluree -m fluree

# move clj deps to fluree's home
# double caching in image layers is unfortunate, but setting this user
# earlier in the build caused its own set of issues
RUN mv /root/.m2 /home/fluree/.m2 && chown -R fluree.fluree /home/fluree/.m2

RUN chown -R fluree.fluree .
USER fluree

ENTRYPOINT []
