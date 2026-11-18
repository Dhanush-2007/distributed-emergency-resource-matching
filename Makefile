ifeq ($(OS),Windows_NT)
PY := py -3.11
VENV_PY := .venv/Scripts/python.exe
else
PY := python3.11
VENV_PY := .venv/bin/python
endif

.PHONY: install test lint up down image-storage

image-storage:
	docker build -f services/storage-node/Dockerfile -t ems-storage-node:latest .

install:
	$(PY) -m venv .venv
	$(VENV_PY) -m pip install --upgrade pip
	$(VENV_PY) -m pip install -e common
	$(VENV_PY) -m pip install -r services/storage-node/requirements.txt
	$(VENV_PY) -m pip install -r services/coordinator/requirements.txt
	$(VENV_PY) -m pip install -r services/request-service/requirements.txt
	$(VENV_PY) -m pip install -r services/resource-service/requirements.txt
	$(VENV_PY) -m pip install -r services/matching-service/requirements.txt
	$(VENV_PY) -m pip install -r requirements-dev.txt

test:
	$(VENV_PY) -m pytest -m "unit or component"

lint:
	$(VENV_PY) -m ruff check .

up:
	@echo not implemented yet

down:
	@echo not implemented yet
