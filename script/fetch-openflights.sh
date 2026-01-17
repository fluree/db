#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="${ROOT}/dev-resources/openflights/raw"

mkdir -p "${RAW_DIR}"

# Files to download (portable approach without associative arrays)
download_file() {
  local name="$1"
  local url="$2"
  local target="${RAW_DIR}/${name}"

  if [[ -f "${target}" ]]; then
    echo "File exists, skipping download: ${name}"
    return
  fi

  echo "Downloading ${name}..."
  curl -fsSL "${url}" -o "${target}"
}

# Download the OpenFlights data files
download_file "airports.dat" "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
download_file "airlines.dat" "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
download_file "routes.dat" "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

echo "OpenFlights raw data ready in ${RAW_DIR}"

