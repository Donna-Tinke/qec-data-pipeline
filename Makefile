.DEFAULT_GOAL := help

PYTHON ?= python3
COMPOSE ?= docker compose

.PHONY: help config data-verify docs-verify build up seed verify bootstrap ps logs down reset-platform

help:
	@echo "Quantum data lake course platform"
	@echo ""
	@echo "  make bootstrap       Start services, seed inputs, and verify everything"
	@echo "  make data-verify     Verify the supplied student data"
	@echo "  make docs-verify     Check assignment documentation and links"
	@echo "  make up              Start PostgreSQL, MinIO, Adminer, and JupyterLab"
	@echo "  make seed            Copy course inputs into the Bronze data area"
	@echo "  make verify          Check that the platform and data are ready"
	@echo "  make ps              Show service status"
	@echo "  make logs            Follow service logs"
	@echo "  make down            Stop services while retaining data volumes"
	@echo "  make reset-platform  Remove course containers and their data volumes"

config:
	@test -f .env || cp .env.example .env
	@echo "Local configuration is available in .env"

data-verify:
	$(PYTHON) scripts/verify_datasets.py

docs-verify:
	$(PYTHON) scripts/verify_documentation.py

build:
	$(COMPOSE) build workspace

up: config
	$(COMPOSE) up --detach postgres minio adminer workspace

seed: up
	$(COMPOSE) --profile tools run --rm seed

verify: data-verify
	$(PYTHON) scripts/verify_environment.py

bootstrap: config build up seed verify
	@echo ""
	@echo "Platform ready:"
	@echo "  JupyterLab: http://localhost:8888/lab?token=quantum-course"
	@echo "  MinIO:      http://localhost:9001"
	@echo "  Adminer:    http://localhost:8080"

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --follow --tail=100

down:
	$(COMPOSE) down

reset-platform:
	@echo "Removing only the Docker volumes owned by this course platform."
	$(COMPOSE) down --volumes --remove-orphans
