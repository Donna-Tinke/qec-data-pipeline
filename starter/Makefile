.DEFAULT_GOAL := help

PYTHON ?= python

.PHONY: help check inventory run train test

help:
	@echo "Student pipeline commands"
	@echo ""
	@echo "  make check      Verify object-store and PostgreSQL connections"
	@echo "  make inventory  List the supplied course data files"
	@echo "  make run        Run your pipeline after implementing the stages"
	@echo "  make train      Train/evaluate models from the required ML input tables"
	@echo "  make test       Run automated tests"

check:
	$(PYTHON) -m quantum_lake_student.cli check

inventory:
	$(PYTHON) -m quantum_lake_student.cli inventory

run:
	$(PYTHON) -m quantum_lake_student.cli run

train:
	$(PYTHON) -m quantum_lake_student.cli train

test:
	$(PYTHON) -m pytest
