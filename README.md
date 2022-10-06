## snovault-index

Queue-based services for invalidating and indexing Opensearch documents.

## Run tests
```bash
$ docker compose -f docker-compose.test.yml up --exit-code-from snoindex
```

## Run mypy
```bash
$ pip install mypy
$ mypy . --strict --ignore-missing-imports
```

## Run pre-commit
```bash
$ pip install pre-commit==2.17.0
$ pre-commit install
$ pre-commit run --all-files
```
