#!/usr/bin/env bash
# Bootstrap script for Apache Polaris with MinIO
# Creates catalog, namespace, and configures storage for vended credentials
set -euo pipefail

POLARIS_HOST="${POLARIS_HOST:-localhost}"
POLARIS_PORT="${POLARIS_PORT:-8182}"
POLARIS_URL="http://${POLARIS_HOST}:${POLARIS_PORT}"

CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"

CATALOG_NAME="${CATALOG_NAME:-openflights}"
NAMESPACE="${NAMESPACE:-openflights}"

echo "=== Polaris Bootstrap ==="
echo "URL: ${POLARIS_URL}"
echo "Catalog: ${CATALOG_NAME}"
echo ""

# Wait for Polaris to be healthy (management port is one higher)
MGMT_PORT=$((POLARIS_PORT + 1))
echo "Waiting for Polaris to be ready..."
until curl -sf "http://${POLARIS_HOST}:${MGMT_PORT}/q/health" > /dev/null 2>&1; do
  echo "  waiting..."
  sleep 2
done
echo "Polaris is ready!"
echo ""

# Get OAuth token
echo "Getting OAuth token..."
TOKEN_RESPONSE=$(curl -sf -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=${CLIENT_ID}" \
  -d "client_secret=${CLIENT_SECRET}" \
  -d "scope=PRINCIPAL_ROLE:ALL")

ACCESS_TOKEN=$(echo "${TOKEN_RESPONSE}" | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")
echo "Got token: ${ACCESS_TOKEN:0:20}..."
echo ""

# Create catalog with S3/MinIO storage and vended credentials
echo "Creating catalog: ${CATALOG_NAME}..."
CATALOG_RESPONSE=$(curl -sf -X POST "${POLARIS_URL}/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "'"${CATALOG_NAME}"'",
      "type": "INTERNAL",
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://polaris-warehouse/'"${CATALOG_NAME}"'/"],
        "endpoint": "http://localhost:9000",
        "endpointInternal": "http://iceberg-minio:9000",
        "pathStyleAccess": true
      },
      "properties": {
        "default-base-location": "s3://polaris-warehouse/'"${CATALOG_NAME}"'/",
        "enable.credential.vending": "true"
      }
    }
  }' 2>&1) || echo "Catalog may already exist"

echo "Catalog response: ${CATALOG_RESPONSE:-created}"

# Grant TABLE_READ_DATA for vended credentials
echo "Granting TABLE_READ_DATA privilege for credential vending..."
curl -sf -X PUT "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"TABLE_READ_DATA"}}' > /dev/null 2>&1 || true
echo ""

# Create namespace
echo "Creating namespace: ${NAMESPACE}..."
NS_RESPONSE=$(curl -sf -X POST "${POLARIS_URL}/api/catalog/v1/${CATALOG_NAME}/namespaces" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": ["'"${NAMESPACE}"'"],
    "properties": {}
  }' 2>&1) || echo "Namespace may already exist"

echo "Namespace response: ${NS_RESPONSE:-created}"
echo ""

# Verify setup
echo "=== Verification ==="
echo "Listing namespaces..."
curl -sf "${POLARIS_URL}/api/catalog/v1/${CATALOG_NAME}/namespaces" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" | python3 -m json.tool

echo ""
echo "=== Bootstrap Complete ==="
echo ""
echo "Polaris REST API: ${POLARIS_URL}/api/catalog/v1/${CATALOG_NAME}"
echo "OAuth endpoint:   ${POLARIS_URL}/api/catalog/v1/oauth/tokens"
echo ""
echo "To get a token:"
echo "  curl -X POST ${POLARIS_URL}/api/catalog/v1/oauth/tokens \\"
echo "    -d 'grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL'"
echo ""
