#!/bin/bash
set -euo pipefail

# Usage check
if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <schema-file> <subject>" >&2
  exit 1
fi

SCHEMA_FILE="$1"
SUBJECT="$2"
REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

# Validate file exists and is readable
if [[ ! -r "$SCHEMA_FILE" ]]; then
  echo "Error: schema file '$SCHEMA_FILE' not found or not readable" >&2
  exit 1
fi

# Validate the schema file is valid JSON before sending
if ! jq empty "$SCHEMA_FILE" 2>/dev/null; then
  echo "Error: '$SCHEMA_FILE' is not valid JSON" >&2
  exit 1
fi

echo "Ingesting schema from file '${SCHEMA_FILE}' into subject '${SUBJECT}'"

curl -fsS -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(jq -n --rawfile schema "$SCHEMA_FILE" '{schema: $schema}')" \
  "${REGISTRY_URL}/subjects/${SUBJECT}/versions"

echo   # trailing newline after curl's response