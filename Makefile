.PHONY: help start start-clean stop logs run test integration status clean \
       docker-build docker-run docker-run-detached docker-stop docker-logs \
       curl-check curl-status curl-health curl-ready all-tests dev dev-docker docker-restart \
       tf-validate

# Unique suffix per make invocation (for curl-check key). Works when recipe shell lacks date +%s (e.g. Windows).
CURL_CHECK_ID := $(shell date +%s 2>/dev/null || echo $$RANDOM 2>/dev/null || echo 0)

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make <command>"
	@echo ""
	@echo "Local Development (sbt + LocalStack):"
	@echo "  make start        Start LocalStack only (for sbt development)"
	@echo "  make start-clean  Rebuild LocalStack image (no cache) then start"
	@echo "  make stop         Stop LocalStack"
	@echo "  make logs         Follow LocalStack logs"
	@echo "  make run          Run the application with sbt (requires LocalStack)"
	@echo "  make dev          Start LocalStack + run app with sbt"
	@echo "  make dev-docker   Start LocalStack + run app in Docker (no sbt)"
	@echo "  make test         Run unit tests"
	@echo "  make integration  Run integration tests"
	@echo "  make status       Check LocalStack and AWS resource status"
	@echo "  make clean        Stop all containers and clean build artifacts"
	@echo ""
	@echo "Docker (full containerized stack):"
	@echo "  make docker-build         Build the application Docker image"
	@echo "  make docker-run           Start full stack (LocalStack + App)"
	@echo "  make docker-run-detached  Start full stack in background"
	@echo "  make docker-stop          Stop Docker services"
	@echo "  make docker-logs          Follow Docker logs"
	@echo "  make docker-restart       Restart Docker stack"
	@echo ""
	@echo "API Testing:"
	@echo "  make curl-check   Smoke test: 100 requests, verify X-RateLimit-* headers"
	@echo "  make curl-status  Test GET /v1/ratelimit/status/:key"
	@echo "  make curl-health  Test GET /health"
	@echo "  make curl-ready   Test GET /ready"
	@echo ""
	@echo "Terraform:"
	@echo "  make tf-validate  Validate Terraform configuration"

# =============================================================================
# Local Development (sbt + LocalStack)
# =============================================================================

start: ## Start LocalStack only (for sbt development)
	@echo "Starting LocalStack..."
	docker-compose up -d --build localstack

start-clean: ## Rebuild LocalStack image without cache, then start (use if init script is missing)
	@echo "Rebuilding LocalStack image (no cache)..."
	docker-compose build --no-cache localstack
	@$(MAKE) start
	@echo "Waiting for LocalStack to be ready..."
	@timeout=120; \
	while [ $$timeout -gt 0 ]; do \
		if curl -s http://localhost:4566/_localstack/health | grep -q '"dynamodb": "running"'; then \
			echo "LocalStack is ready!"; \
			echo ""; \
			echo "To run the application:"; \
			echo "  make run"; \
			exit 0; \
		fi; \
		echo "  Waiting for DynamoDB..."; \
		sleep 2; \
		timeout=$$((timeout-2)); \
	done; \
	echo "LocalStack failed to start within timeout period"; \
	exit 1

stop: ## Stop LocalStack
	@echo "Stopping LocalStack..."
	docker-compose down

logs: ## Follow LocalStack logs
	docker-compose logs -f localstack

run: ## Run the application with sbt (requires LocalStack)
	@echo "Starting Gate..."
	@echo ""
	@command -v sbt >/dev/null 2>&1 || { \
		echo "Error: sbt not found in PATH."; \
		echo "  Install sbt: https://www.scala-sbt.org/download.html"; \
		echo "  Or run app in Docker: make dev-docker   (LocalStack + app, no sbt)"; \
		echo "  Or full stack:         make docker-run"; \
		exit 1; \
	}
	USE_LOCALSTACK=true \
	DYNAMODB_ENDPOINT=http://localhost:4566 \
	KINESIS_ENDPOINT=http://localhost:4566 \
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_REGION=us-east-1 \
	sbt run

dev: start run ## Start LocalStack and run the application with sbt

dev-docker: start ## Start LocalStack, then run the app in Docker (no sbt required)
	@echo "Starting rate-limiter container (LocalStack already running)..."
	docker-compose up --build rate-limiter

test: ## Run unit tests
	@echo "Running unit tests..."
	@command -v sbt >/dev/null 2>&1 || { \
		echo "Error: sbt not found. Install sbt or use Docker."; \
		exit 1; \
	}
	sbt test

integration: ## Run integration tests (uses TestContainers, no LocalStack needed)
	@echo "Running integration tests..."
	@command -v sbt >/dev/null 2>&1 || { \
		echo "Error: sbt not found. Install sbt or use Docker."; \
		exit 1; \
	}
	sbt "testOnly *IntegrationSpec"

all-tests: test integration ## Run all tests (unit + integration)
	@echo "All tests completed!"

status: ## Check LocalStack and AWS resource status
	@echo "Checking LocalStack status..."
	@curl -s http://localhost:4566/_localstack/health 2>/dev/null | python3 -m json.tool 2>/dev/null | \
		perl -pe 's/": "running"/": "\e[0;32mrunning\e[0m"/g; \
		          s/": "available"/": "\e[0;32mavailable\e[0m"/g; \
		          s/": "disabled"/": "\e[0;31mdisabled\e[0m"/g' || echo "  (LocalStack not running)"
	@echo ""
	@if command -v aws >/dev/null 2>&1; then \
		echo "DynamoDB Tables:"; \
		aws --endpoint-url=http://localhost:4566 dynamodb list-tables 2>/dev/null || echo "  (Unable to query - check LocalStack is running)"; \
		echo ""; \
		echo "Kinesis Streams:"; \
		aws --endpoint-url=http://localhost:4566 kinesis list-streams 2>/dev/null || echo "  (Unable to query - check LocalStack is running)"; \
	else \
		echo "Note: AWS CLI not installed (optional - not required for the application)"; \
		echo "  To list tables/streams, install AWS CLI or check via Docker:"; \
		echo "    docker-compose exec localstack awslocal dynamodb list-tables"; \
		echo "    docker-compose exec localstack awslocal kinesis list-streams"; \
	fi

clean: ## Stop all containers and clean build artifacts
	@echo "Cleaning up..."
	-docker-compose --profile app down -v 2>/dev/null
	-docker-compose down -v 2>/dev/null
	rm -rf target project/target .bsp .metals

# =============================================================================
# Docker (full containerized stack)
# =============================================================================

docker-build: ## Build the application Docker image
	@echo "Building Docker image..."
	docker build -t rate-limiter-platform:latest .
	@echo ""
	@echo "Image built: rate-limiter-platform:latest"
	@echo "Run with: make docker-run"

docker-run: ## Start full stack (LocalStack + App) with Docker
	@echo "Starting full stack with Docker..."
	@echo "This will build the app image and start LocalStack + Application"
	@echo ""
	docker-compose --profile app up --build

docker-run-detached: ## Start full stack in background
	@echo "Starting full stack with Docker (detached)..."
	docker-compose --profile app up --build -d
	@echo ""
	@echo "Services started. Useful commands:"
	@echo "  make docker-logs   - Follow logs"
	@echo "  make curl-health   - Check health"
	@echo "  make docker-stop   - Stop services"

docker-stop: ## Stop Docker services
	@echo "Stopping Docker services..."
	-docker-compose --profile app down

docker-logs: ## Follow Docker logs
	docker-compose --profile app logs -f

docker-restart: docker-stop docker-run ## Restart Docker stack

# =============================================================================
# API Testing
# =============================================================================

curl-check: ## Smoke test: 100 requests, verify X-RateLimit-* headers
	@echo "Smoke test: sending 100 requests, verifying X-RateLimit-* headers..."
	@key="smoke-$(CURL_CHECK_ID)-$$$$"; \
	first_headers="/tmp/curl-check-headers.$$$$"; \
	first_status="/tmp/curl-check-status.$$$$"; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100; do \
	  if [ "$$i" = "1" ]; then \
	    curl -s -D "$$first_headers" -w "%{http_code}" -o /dev/null -X POST http://localhost:8080/v1/ratelimit/check \
	      -H "Content-Type: application/json" \
	      -H "Authorization: Bearer test-api-key" \
	      -d "{\"key\":\"$$key\",\"cost\":1}" > "$$first_status" || { echo "Error: Service not available at http://localhost:8080"; echo "Start with: make dev  OR  make docker-run"; rm -f "$$first_headers" "$$first_status"; exit 1; }; \
	  else \
	    curl -s -o /dev/null -X POST http://localhost:8080/v1/ratelimit/check \
	      -H "Content-Type: application/json" \
	      -H "Authorization: Bearer test-api-key" \
	      -d "{\"key\":\"$$key\",\"cost\":1}" || { echo "Error: Service not available"; rm -f "$$first_headers" "$$first_status"; exit 1; }; \
	  fi; \
	done; \
	read code < "$$first_status"; \
	[ "$$code" = "000" ] && { echo "Error: Service not available"; rm -f "$$first_headers" "$$first_status"; exit 1; }; \
	[ "$$code" != "200" ] && { echo "FAIL: First request returned HTTP $$code (expected 200). X-RateLimit-* headers only appear on allowed responses."; rm -f "$$first_headers" "$$first_status"; exit 1; }; \
	grep -qi 'X-RateLimit-Limit' "$$first_headers" && grep -qi 'X-RateLimit-Remaining' "$$first_headers" && grep -qi 'X-RateLimit-Reset' "$$first_headers" || { echo "FAIL: Missing X-RateLimit-* headers (first response was 200)"; rm -f "$$first_headers" "$$first_status"; exit 1; }; \
	rm -f "$$first_headers" "$$first_status"; \
	echo "Smoke test OK: 100 requests sent, X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset verified."

curl-status: ## Test GET /v1/ratelimit/status/:key
	@echo "Testing rate limit status endpoint..."
	@curl -s http://localhost:8080/v1/ratelimit/status/user:123 | python3 -m json.tool 2>/dev/null || \
		echo "Error: Service not available"

curl-health: ## Test GET /health
	@echo "Testing health endpoint..."
	@curl -s http://localhost:8080/health | python3 -m json.tool 2>/dev/null || \
		echo "Error: Service not available"

curl-ready: ## Test GET /ready
	@echo "Testing readiness endpoint..."
	@curl -s http://localhost:8080/ready | python3 -m json.tool 2>/dev/null || \
		echo "Error: Service not available"

# =============================================================================
# Terraform
# =============================================================================

tf-validate: ## Validate Terraform configuration
	@echo "Validating Terraform configuration..."
	@command -v terraform >/dev/null 2>&1 || { \
		echo "Error: terraform not found in PATH."; \
		echo "Install Terraform: https://www.terraform.io/downloads"; \
		exit 1; \
	}
	@echo "Initializing Terraform providers..."
	cd terraform && terraform init -upgrade
	@echo "Validating Terraform configuration..."
	cd terraform && terraform validate
