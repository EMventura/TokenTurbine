# =============================================================================
# TOKENTURBINE MAKEFILE
# =============================================================================
# Helper commands for building and running the containerized pipeline

.PHONY: help build run clean test shell logs stop

# Default target
help:
	@echo "TokenTurbine - Available Commands:"
	@echo ""
	@echo "  make download-data - Download the input dataset"
	@echo "  make build       - Build the Docker image"
	@echo "  make run         - Run the pipeline with default config"
	@echo "  make shell       - Open a shell in the container"
	@echo "  make logs        - View pipeline logs"
	@echo "  make stop        - Stop running containers"
	@echo "  make clean       - Remove containers and images"
	@echo "  make clean-data  - Clean processed data (WARNING: deletes outputs)"
	@echo ""

# Download input dataset
download-data:
	@echo "Downloading input dataset..."
	@chmod +x scripts/download_data.sh
	@./scripts/download_data.sh

# Build the Docker image
build:
	@echo "Building TokenTurbine Docker image..."
	docker-compose build

# Check if data exists before running
check-data:
	@if [ ! -f data/raw/mainpipe_data_v1.jsonl ]; then \
		echo "❌ Data file not found: data/raw/mainpipe_data_v1.jsonl"; \
		echo ""; \
		echo "Please run one of:"; \
		echo "  make download-data  (to download sample dataset)"; \
		echo "  OR"; \
		echo "  cp your-data.jsonl data/raw/mainpipe_data_v1.jsonl"; \
		echo ""; \
		exit 1; \
	fi
	@echo "✅ Data file found"

# Run the pipeline with default config
run:
	@echo "Running TokenTurbine pipeline..."
	docker-compose up

# Open interactive shell in container
shell:
	@echo "Opening shell in TokenTurbine container..."
	docker-compose run --rm --entrypoint /bin/bash pipeline

# View logs
logs:
	docker-compose logs -f pipeline

# Stop running containers
stop:
	@echo "Stopping TokenTurbine containers..."
	docker-compose down

# Clean up containers and images
clean:
	@echo "Cleaning up containers and images..."
	docker-compose down --rmi all --volumes --remove-orphans

# Clean processed data (WARNING: destructive)
clean-data:
	@echo "WARNING: This will delete all processed data!"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		rm -rf data/processed/* data/reports/* data/checkpoints/*; \
		echo "Data cleaned."; \
	else \
		echo "Cancelled."; \
	fi

# Quick validation (check if container runs)
validate:
	@echo "Validating Docker setup..."
	docker-compose run --rm --entrypoint python pipeline -c "import ray, tiktoken, fasttext; print('✅ All imports successful')"

# Build and run in one command
build-run: build run

# Development mode (mount source code)
dev:
	@echo "Running in development mode (with source code mounted)..."
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
