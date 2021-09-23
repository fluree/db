FROM clojure:tools-deps-1.10.3.967-slim-bullseye

RUN mkdir -p /usr/src/flureedb
WORKDIR /usr/src/flureedb

# Install the tools we need to install the tools we need
RUN apt-get update && apt-get install -y wget curl gnupg2 software-properties-common

# Add Chrome source for running CLJS tests
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'

# Add node PPA to get newer versions
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get update && apt-get install -y nodejs google-chrome-stable

COPY deps.edn Makefile ./
RUN make deps

COPY package.json ./
RUN npm install && npm install -g karma-cli

COPY . ./

RUN make jar

# Create a user to own the fluree code
RUN groupadd fluree && useradd --no-log-init -g fluree -m fluree

# move clj deps to fluree's home
# double caching in image layers is unfortunate, but setting this user
# earlier in the build caused its own set of issues
RUN mv /root/.m2 /home/fluree/.m2 && chown -R fluree.fluree /home/fluree/.m2

RUN chown -R fluree.fluree .
USER fluree

ENTRYPOINT []
