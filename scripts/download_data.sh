#!/bin/bash
# =============================================================================
# TokenTurbine Data Download Script
# =============================================================================
# Downloads the input dataset for processing
# Usage: ./scripts/download_data.sh

set -e  # Exit on error

# Configuration
DATA_URL="https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl"
OUTPUT_DIR="data/raw"
OUTPUT_FILE="$OUTPUT_DIR/mainpipe_data_v1.jsonl"

echo "================================"
echo "TokenTurbine - Data Download"
echo "================================"
echo ""

# Check if file already exists
if [ -f "$OUTPUT_FILE" ]; then
    echo "⚠️  Data file already exists at: $OUTPUT_FILE"
    read -p "Do you want to re-download? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping download."
        exit 0
    fi
fi

# Create output directory
echo "Creating directory: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Download the file
echo "Downloading dataset..."
echo "Source: $DATA_URL"
echo "Destination: $OUTPUT_FILE"
echo ""

# Use curl with progress bar
if command -v curl &> /dev/null; then
    curl -L --progress-bar "$DATA_URL" -o "$OUTPUT_FILE"
elif command -v wget &> /dev/null; then
    wget --show-progress "$DATA_URL" -O "$OUTPUT_FILE"
else
    echo "❌ Error: Neither curl nor wget found."
    echo "Please install curl or wget and try again."
    exit 1
fi

# Verify download
if [ -f "$OUTPUT_FILE" ]; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    LINE_COUNT=$(wc -l < "$OUTPUT_FILE")
    
    echo ""
    echo "================================"
    echo "✅ Download complete!"
    echo "================================"
    echo "File: $OUTPUT_FILE"
    echo "Size: $FILE_SIZE"
    echo "Documents: $(printf "%'d" $LINE_COUNT)"
    echo ""
else
    echo ""
    echo "❌ Download failed!"
    exit 1
fi