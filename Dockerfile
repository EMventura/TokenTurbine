# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
# PYTHONUNBUFFERED=1 ensures logs are printed immediately
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies (needed for some Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/
COPY configs/ ./configs/
# We do NOT copy data/ here. We mount it at runtime.

# Download FastText language model (critical for pipeline)
# This ensures the model is baked into the image
RUN mkdir -p data/ && \
    wget -q https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin \
    -O data/lid.176.bin

# Set proper permissions for data directory
RUN chmod -R 755 data/

# Create data directories to avoid permission issues
RUN mkdir -p data/raw data/processed data/reports data/processed/tokenized

# Default command
ENTRYPOINT ["python", "src/main.py"]

# Default command (can be overridden)
CMD ["--config", "configs/base.yaml"]
