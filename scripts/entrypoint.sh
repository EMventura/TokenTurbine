#!/bin/bash
set -e

# Define paths
DATA_DIR="/app/data/raw"
DATA_FILE="$DATA_DIR/mainpipe_data_v1.jsonl"
URL="https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl"

echo "Starting TokenTurbine Container..."

# 1. Check if data exists in the mounted volume
if [ -f "$DATA_FILE" ]; then
    echo "✅ Raw data found at $DATA_FILE. Skipping download."
else
    echo "⚠️  Raw data not found. Downloading dataset..."
    mkdir -p "$DATA_DIR"
    
    # Download with progress bar
    curl -L "$URL" -o "$DATA_FILE"
    
    if [ $? -eq 0 ]; then
        echo "✅ Download complete: $DATA_FILE"
    else
        echo "❌ Download failed!"
        exit 1
    fi
fi

# 2. Execute the main pipeline command passed to docker run
# This allows passing arguments like --config configs/experiment.yaml
echo "Executing pipeline..."
exec "$@"