FROM debian:trixie-slim AS server-base

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tini curl bash \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r fluree --gid=1000 \
    && useradd -r -g fluree --uid=1000 --home-dir=/var/lib/fluree --shell=/bin/bash fluree \
    && mkdir -p /var/lib/fluree \
    && chown -R fluree:fluree /var/lib/fluree

ARG TARGETARCH
COPY --chown=root:root --chmod=0755 docker-artifacts/${TARGETARCH}/fluree /usr/local/bin/fluree

COPY --chown=root:root --chmod=0755 <<'EOF' /usr/local/bin/fluree-entrypoint.sh
#!/bin/sh
set -e
if [ ! -d "$PWD/.fluree" ]; then
  fluree init
fi
exec /usr/bin/tini -- fluree server run "$@"
EOF

LABEL org.opencontainers.image.source="https://github.com/fluree/db"
LABEL org.opencontainers.image.description="Fluree — semantic graph database"
LABEL org.opencontainers.image.licenses="BUSL-1.1"

USER fluree
WORKDIR /var/lib/fluree
VOLUME /var/lib/fluree

EXPOSE 8090
ENV RUST_LOG=info

HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8090/health || exit 1

ENTRYPOINT ["/usr/local/bin/fluree-entrypoint.sh"]

# The server with the prover permanent erasure needs (`fluree prover`), which
# it finds beside `fluree`: `docker build --target erasure`, tagged `-erasure`.
FROM server-base AS erasure
ARG TARGETARCH
COPY --chown=root:root --chmod=0755 docker-artifacts/${TARGETARCH}/fluree-prover /usr/local/bin/fluree-prover
# Fails the build unless it runs here.
RUN ["/usr/local/bin/fluree-prover", "--image-id"]

# The server: the default target.
FROM server-base AS server
