#!/usr/bin/env bash
# Generate synthetic patients with Synthea (needs Java 17+).
# Usage: scripts/generate_synthea.sh [patient_count] [seed]
#
# Only the CSV files HealthFlow ingests are written (patients, encounters); the full
# export is several GB at 10,000 patients. To export everything, set SYNTHEA_FILES="".
set -euo pipefail

PATIENTS="${1:-1000}"
SEED="${2:-42}"
SYNTHEA_DIR="data/synthea"
JAR="$SYNTHEA_DIR/synthea-with-dependencies.jar"
FILES="${SYNTHEA_FILES-patients.csv,encounters.csv}"

mkdir -p "$SYNTHEA_DIR"
if [[ ! -f "$JAR" ]]; then
  echo "Downloading Synthea..."
  curl -fL -o "$JAR" \
    https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
fi

included=()
if [[ -n "$FILES" ]]; then
  included=(--exporter.csv.included_files "$FILES")
fi

# -s fixes the random seed so the dataset is reproducible.
# CSV only for now; FHIR export is a later phase.
java -jar "$JAR" \
  -p "$PATIENTS" -s "$SEED" \
  --exporter.baseDirectory "$SYNTHEA_DIR/output" \
  --exporter.csv.export true \
  "${included[@]}" \
  --exporter.fhir.export false \
  --exporter.hospital.fhir.export false \
  --exporter.practitioner.fhir.export false \
  > "$SYNTHEA_DIR/generate.log"

echo "Done: $(($(wc -l < "$SYNTHEA_DIR/output/csv/patients.csv") - 1)) patients. CSV files are in $SYNTHEA_DIR/output/csv"
