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
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/
COPY configs/ ./configs/
# We do NOT copy data/ here. We mount it at runtime.

# Create data directories to avoid permission issues
RUN mkdir -p data/raw data/processed data/reports

# Default command
ENTRYPOINT ["python", "src/main.py"]