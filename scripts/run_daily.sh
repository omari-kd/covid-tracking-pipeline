#!/bin/bash
# Run full COVID ETL: Ingest -> Transform

 set -euo pipefail # safer script execution

 # Absolute project directory
PROJECT_DIR="/home/kojo/Projects/Data-Engineering-Projects/covid-tracking-pipeline"

# Activate virtual environment
source "$PROJECT_DIR/.venv/bin/activate"
# Move to project directory
cd "$PROJECT_DIR" || exit

echo "Starting COVID ETL pipeline..."

echo "Step 1: Ingesting raw data from API..."
python3 "$PROJECT_DIR/src/etl/ingest_covid.py"
echo "Ingestion complete."

echo "Step 2: Transformation and cleaning data..."
python3 "$PROJECT_DIR/src/etl/transform_covid.py"
echo "Transformation complete."

echo "ETL pipeline finished successfully."

# Deactivate venv
deactivate