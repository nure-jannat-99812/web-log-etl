.PHONY: build up down logs clean shell status generate

# Use docker compose (new version) instead of docker-compose
DOCKER_COMPOSE = docker compose

# Build the Docker image
build:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml build

# Start the generator service
up:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml up -d
	@echo "✅ Log generator started"
	@echo "📊 View logs: make logs"

# Stop the service
down:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml down

# View generator logs
logs:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml logs -f

# Clean all generated data
clean:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml down -v
	sudo rm -rf data/logs/*
	@echo "✅ Cleaned all logs"

# Enter container shell
shell:
	docker exec -it web-log-generator /bin/bash

# Check service status
status:
	docker ps | grep web-log-generator || echo "❌ Generator not running"

# Generate logs for today (manual)
generate:
	docker exec web-log-generator python /app/scripts/run_generator.py \
		--date $(shell date +%Y-%m-%d) \
		--format all \
		--logs-per-hour $(or $(LOGS),100)

# Generate logs for specific date
generate-date:
	docker exec web-log-generator python /app/scripts/run_generator.py \
		--date $(DATE) \
		--format all \
		--logs-per-hour $(or $(LOGS),100)

# Backfill logs for past N days
backfill:
	@echo "Backfilling $(DAYS) days..."
	@for i in $$(seq 1 $(DAYS)); do \
		date=$$(date -d "$(shell date +%Y-%m-%d) -$$i days" +%Y-%m-%d); \
		echo "Generating $$date"; \
		docker exec web-log-generator python /app/scripts/run_generator.py \
			--date $$date --format all --logs-per-hour $(or $(LOGS),50); \
	done

# Show disk usage
usage:
	@echo "📊 Log storage usage:"
	@du -sh data/logs/* 2>/dev/null || echo "No logs yet"
	@echo ""
	@echo "📁 Total logs:"
	@find data/logs -name "*.jsonl" -o -name "*.csv" -o -name "*.log" 2>/dev/null | wc -l

# Quick test
test:
	docker exec web-log-generator python /app/scripts/run_generator.py \
		--date $(shell date +%Y-%m-%d) \
		--format jsonl \
		--logs-per-hour 10

# Build without cache
rebuild:
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml build --no-cache
	$(DOCKER_COMPOSE) -f docker/docker-compose.yml up -d
