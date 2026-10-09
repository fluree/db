# Shared settings for the local Glue + S3 Iceberg harness. Source this file; don't execute it.
# Everything is local: moto mocks S3 and the Glue Data Catalog in one process. No AWS account,
# no Docker. Credentials are moto's dummies. One-time setup: see README.md.

export HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RUN_DIR="${RUN_DIR:-${TMPDIR:-/tmp}/fluree-glue-local}"   # venv, moto log, fluree projects
export PY="$RUN_DIR/.venv/bin/python"
export MOTO_PORT="${MOTO_PORT:-5055}"
export MOTO_ENDPOINT="http://127.0.0.1:$MOTO_PORT"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
# The AWS SDKs route Glue to moto through the standard endpoint override, which is how
# fluree's Glue catalog mode is pointed at a private endpoint too. S3 deliberately gets NO
# override: fluree must reach moto's S3 through --s3-endpoint, so check.sh proves that flag is
# honored (the seed scripts pass their endpoint explicitly).
export AWS_ENDPOINT_URL_GLUE="$MOTO_ENDPOINT"
unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL
# Don't let ~/.aws/{config,credentials} leak a real profile into the run.
export AWS_CONFIG_FILE=/dev/null
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
unset AWS_PROFILE AWS_SESSION_TOKEN

export BUCKET="${BUCKET:-lake}"
export GLUE_DB="${GLUE_DB:-demo}"
export WAREHOUSE="s3://$BUCKET/warehouse"
