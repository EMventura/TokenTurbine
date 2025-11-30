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

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}TokenTurbine Data Download${NC}"
echo "================================"

# Check if file already exists
if [ -f "$OUTPUT_FILE" ]; then
    echo -e "${YELLOW}Data file already exists at: $OUTPUT_FILE${NC}"
    read -p "Do you want to re-download? (y/n) " -n 1 -r
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
echo -e "${GREEN}Downloading dataset...${NC}"
echo "Source: $DATA_URL"
echo "Destination: $OUTPUT_FILE"
echo ""

# Use curl with progress bar
if command -v curl &> /dev/null; then
    curl -L --progress-bar "$DATA_URL" -o "$OUTPUT_FILE"
elif command -v wget &> /dev/null; then
    wget --show-progress "$DATA_URL" -O "$OUTPUT_FILE"
else
    echo -e "${RED}Error: Neither curl nor wget found. Please install one of them.${NC}"
    exit 1
fi

# Verify download
if [ -f "$OUTPUT_FILE" ]; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    LINE_COUNT=$(wc -l < "$OUTPUT_FILE")
    
    echo ""
    echo -e "${GREEN}✓ Download complete!${NC}"
    echo "File: $OUTPUT_FILE"
    echo "Size: $FILE_SIZE"
    echo "Documents: $LINE_COUNT"
    echo ""
    echo -e "${GREEN}You can now run: make run${NC}"
else
    echo -e "${RED}✗ Download failed!${NC}"
    exit 1
fi