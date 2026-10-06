# Dev-only image for the local-cluster scripts.
#
# Distinct from the workspace-root Dockerfile:
# - Uses the `fluree-server` binary directly (the raft entrypoint),
#   not the `fluree` CLI wrapper. No `fluree init` step — config is
#   driven entirely by FLUREE_* env vars set in compose.generated.yml.
# - Assumes up.sh has copied a release build into ./.build/fluree-server
#   first. Build orchestration is the script's job, not the image's.
#
# Not intended for production. The workspace Dockerfile remains the
# canonical prod image.

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 .build/fluree-server /usr/local/bin/fluree-server

WORKDIR /var/lib/fluree

EXPOSE 8090 9090

HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=5 \
  CMD curl -fsS http://127.0.0.1:8090/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/fluree-server"]
