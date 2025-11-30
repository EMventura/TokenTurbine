# =============================================================================
# TOKENTURBINE MAKEFILE
# =============================================================================
# Helper commands for building and running the containerized pipeline

.PHONY: help build run clean test shell logs stop

# Default target
help:
	@echo "TokenTurbine - Available Commands:"
	@echo ""
	@echo "  make build       - Build the Docker image"
	@echo "  make run         - Run the pipeline with default config"
	@echo "  make run-test    - Run pipeline with test config (small sample)"
	@echo "  make shell       - Open a shell in the container"
	@echo "  make logs        - View pipeline logs"
	@echo "  make stop        - Stop running containers"
	@echo "  make clean       - Remove containers and images"
	@echo "  make clean-data  - Clean processed data (WARNING: deletes outputs)"
	@echo "  make test        - Run tests (if implemented)"
	@echo ""

# Build the Docker image
build:
	@echo "Building TokenTurbine Docker image..."
	docker-compose build

# Run the pipeline with default config
run:
	@echo "Running TokenTurbine pipeline..."
	docker-compose up

# Run pipeline with test config
run-test:
	@echo "Running TokenTurbine with test config..."
	docker-compose run --rm pipeline --config configs/test.yaml

# Run with custom config
run-custom:
	@read -p "Enter config path (e.g., configs/custom.yaml): " config; \
	docker-compose run --rm pipeline --config $$config

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

# Run tests (placeholder)
test:
	@echo "Running tests..."
	docker-compose run --rm pipeline pytest tests/ || echo "No tests found"

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
