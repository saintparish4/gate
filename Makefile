# Gate — developer entry points. Run `make` for the grouped target list.
#
# Docker Compose v2 is required (the `--wait` flag replaces hand-rolled polling
# loops). Override any of these on the command line, e.g. `make APP_URL=... smoke`.
COMPOSE ?= docker compose
APP_URL ?= http://localhost:8080
LOCALSTACK_URL ?= http://localhost:4566
API_KEY ?= test-api-key

# Unique-per-invocation suffix for the smoke test key, so repeat runs never
# land on a bucket a previous run already drained. Falls back when the recipe
# shell has no `date +%s` (e.g. some Windows shells).
SMOKE_ID := $(shell date +%s 2>/dev/null || echo 0)

# Every sbt target opens with this, so a missing sbt gives instructions instead
# of "command not found".
NEED_SBT = @command -v sbt >/dev/null 2>&1 || { echo "Error: sbt not found in PATH."; echo "  Install it: https://www.scala-sbt.org/download.html"; echo "  Or skip sbt entirely and use Docker: make stack"; exit 1; }

.DEFAULT_GOAL := help
.PHONY: help up up-clean down clean logs status run dev stack obs \
        fmt test test-it test-all smoke health correctness tf-validate

help: ## Show this help
	@awk 'BEGIN { FS = ":.*##"; printf "Usage: make <target>\n" } \
	     /^##@/      { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } \
	     /^[a-z][a-z0-9-]*:.*##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' \
	     $(MAKEFILE_LIST)
	@echo ""

##@ Stack

up: ## Start LocalStack alone and wait until its tables/streams exist
	$(COMPOSE) up -d --build --wait localstack

up-clean: ## Rebuild the LocalStack image from scratch, then start it
	$(COMPOSE) build --no-cache localstack
	$(COMPOSE) up -d --wait localstack

stack: ## Start the whole stack in Docker (LocalStack + app) — no sbt needed
	$(COMPOSE) up -d --build --wait
	@echo "Gate is up at $(APP_URL). Follow it with 'make logs'."

obs: ## Start the stack plus Prometheus, Grafana and Jaeger
	$(COMPOSE) --profile obs up -d --build --wait
	@echo "Grafana http://localhost:3000 | Prometheus http://localhost:9090 | Jaeger http://localhost:16686"

down: ## Stop every container, keeping volumes
	-$(COMPOSE) --profile obs down

clean: ## Stop everything, drop volumes, and delete build artifacts
	-$(COMPOSE) --profile obs down -v
	rm -rf target project/target loadSim/target .bsp .metals

logs: ## Follow container logs (SERVICE=localstack to narrow to one)
	$(COMPOSE) --profile obs logs -f $(SERVICE)

status: ## Show LocalStack health plus the DynamoDB tables and Kinesis streams
	@echo "LocalStack health:"
	@curl -s $(LOCALSTACK_URL)/_localstack/health | python3 -m json.tool 2>/dev/null || echo "  not running — start it with 'make up'"
	@echo ""; echo "DynamoDB tables:"
	@$(COMPOSE) exec -T localstack awslocal dynamodb list-tables 2>/dev/null || echo "  (unavailable)"
	@echo ""; echo "Kinesis streams:"
	@$(COMPOSE) exec -T localstack awslocal kinesis list-streams 2>/dev/null || echo "  (unavailable)"

##@ Application

run: ## Run the app with sbt against a running LocalStack
	$(NEED_SBT)
	USE_LOCALSTACK=true \
	DYNAMODB_ENDPOINT=$(LOCALSTACK_URL) \
	KINESIS_ENDPOINT=$(LOCALSTACK_URL) \
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_REGION=us-east-1 \
	sbt run

dev: up run ## Start LocalStack, then run the app with sbt

##@ Tests

fmt: ## Format all Scala sources (CI fails on unformatted code)
	$(NEED_SBT)
	sbt scalafmtAll

test: ## Run unit tests
	$(NEED_SBT)
	sbt unitTest

test-it: ## Run integration tests (TestContainers — needs Docker, not LocalStack)
	$(NEED_SBT)
	sbt "testOnly *IntegrationSpec"

test-all: test test-it ## Run unit and integration tests

correctness: ## Check the correctness invariants against a running stack
	$(NEED_SBT)
	sbt "loadSim/run --scenario correctness"

##@ Probes

health: ## Print /health, /ready and a sample rate-limit status
	@for path in /health /ready /v1/ratelimit/status/user:123; do \
		echo "GET $$path"; \
		curl -s -H 'Authorization: Bearer $(API_KEY)' $(APP_URL)$$path \
			| python3 -m json.tool 2>/dev/null \
			|| echo "  no JSON response — is the service up? ('make dev' or 'make stack')"; \
	done

smoke: ## Send 100 rate-limit checks and verify the X-RateLimit-* headers
	@echo "Smoke test: 100 requests to $(APP_URL)/v1/ratelimit/check"
	@key="smoke-$(SMOKE_ID)-$$$$"; hdr=$$(mktemp); trap 'rm -f "$$hdr"' EXIT; \
	post() { curl -s -X POST $(APP_URL)/v1/ratelimit/check \
	           -H 'Content-Type: application/json' \
	           -H 'Authorization: Bearer $(API_KEY)' \
	           -d "{\"key\":\"$$key\",\"cost\":1}" "$$@"; }; \
	code=$$(post -D "$$hdr" -o /dev/null -w '%{http_code}'); \
	case "$$code" in \
	  000) echo "FAIL: nothing listening on $(APP_URL). Start it with 'make dev' or 'make stack'."; exit 1 ;; \
	  200) ;; \
	  *)   echo "FAIL: first request returned HTTP $$code (expected 200). X-RateLimit-* headers only appear on allowed responses."; exit 1 ;; \
	esac; \
	for h in Limit Remaining Reset; do \
		grep -qi "X-RateLimit-$$h" "$$hdr" || { echo "FAIL: response is missing the X-RateLimit-$$h header."; exit 1; }; \
	done; \
	i=1; \
	while [ $$i -lt 100 ]; do \
		post -o /dev/null || { echo "FAIL: service stopped responding after $$i requests."; exit 1; }; \
		i=$$((i + 1)); \
	done; \
	echo "OK: 100 requests sent; X-RateLimit-Limit, -Remaining and -Reset all present."

##@ Infrastructure

tf-validate: ## Initialise providers and validate the Terraform config
	@command -v terraform >/dev/null 2>&1 || { echo "Error: terraform not found in PATH."; echo "  Install it: https://www.terraform.io/downloads"; exit 1; }
	cd terraform && terraform init -upgrade && terraform validate
