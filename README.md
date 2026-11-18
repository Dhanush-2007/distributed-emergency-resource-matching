# Distributed Emergency Resource Matching System

Scaffold for the system specified in [docs/SPEC.md](docs/SPEC.md). Service logic is not implemented yet.

Python 3.11. Shared code lives in `common/ems_common` (package name `ems-common`).

```
make install   # create .venv and install the shared package plus requirements
make test      # pytest -m "unit or component"
make lint      # ruff check .
make up        # not implemented yet
make down      # not implemented yet
```
