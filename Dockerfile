FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY docker-artifacts/${TARGETARCH}/fluree /usr/local/bin/fluree
RUN chmod +x /usr/local/bin/fluree

RUN mkdir -p /var/lib/fluree
VOLUME /var/lib/fluree
WORKDIR /var/lib/fluree

EXPOSE 8090

ENTRYPOINT ["fluree", "server", "run"]
