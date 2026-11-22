ifeq ($(OS),Windows_NT)
PY := py -3.11
VENV_PY := .venv/Scripts/python.exe
else
PY := python3.11
VENV_PY := .venv/bin/python
endif

.PHONY: install test itest ftest atest lint up down reset all-tests image-storage image-coordinator image-request image-resource image-matching images

image-storage:
	docker build -f services/storage-node/Dockerfile -t ems/storage-node:1.0 .

image-coordinator:
	docker build -f services/coordinator/Dockerfile -t ems/coordinator:1.0 .

image-request:
	docker build -f services/request-service/Dockerfile -t ems/request-service:1.0 .

image-resource:
	docker build -f services/resource-service/Dockerfile -t ems/resource-service:1.0 .

image-matching:
	docker build -f services/matching-service/Dockerfile -t ems/matching-service:1.0 .

images: image-storage image-coordinator image-request image-resource image-matching

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
	$(VENV_PY) -m pytest --timeout=60 --timeout-method=thread -m "unit or component"

itest:
	$(VENV_PY) -m pytest --timeout=60 --timeout-method=thread -m integration

ftest:
	$(VENV_PY) -m pytest --timeout=300 --timeout-method=thread -m fault

lint:
	$(VENV_PY) -m ruff check .

up:
	docker compose up -d --wait

down:
	docker compose down

reset:
	./scripts/ops/reset.sh compose

atest:
	$(VENV_PY) scripts/ops/check_replication.py

all-tests: lint test itest ftest
