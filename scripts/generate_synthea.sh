#!/usr/bin/env bash
# Generate synthetic patients with Synthea (needs Java 17+).
# Usage: scripts/generate_synthea.sh [patient_count] [seed]
set -euo pipefail

PATIENTS="${1:-1000}"
SEED="${2:-42}"
SYNTHEA_DIR="data/synthea"
JAR="$SYNTHEA_DIR/synthea-with-dependencies.jar"

mkdir -p "$SYNTHEA_DIR"
if [[ ! -f "$JAR" ]]; then
  echo "Downloading Synthea..."
  curl -fL -o "$JAR" \
    https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
fi

# -s fixes the random seed so the dataset is reproducible.
# CSV only for now; FHIR export is a later phase.
java -jar "$JAR" \
  -p "$PATIENTS" -s "$SEED" \
  --exporter.baseDirectory "$SYNTHEA_DIR/output" \
  --exporter.csv.export true \
  --exporter.fhir.export false \
  --exporter.hospital.fhir.export false \
  --exporter.practitioner.fhir.export false

echo "Done. CSV files are in $SYNTHEA_DIR/output/csv"
