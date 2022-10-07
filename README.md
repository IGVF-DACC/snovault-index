[![CircleCI](https://dl.circleci.com/status-badge/img/gh/IGVF-DACC/snovault-index/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/IGVF-DACC/snovault-index/tree/main)
[![Coverage Status](https://coveralls.io/repos/github/IGVF-DACC/snovault-index/badge.svg?branch=main)](https://coveralls.io/github/IGVF-DACC/snovault-index?branch=main)

## snovault-index

Queue-based services for invalidating and indexing Opensearch documents.

## Run tests
```bash
$ docker compose -f docker-compose.unit-test.yml up --exit-code-from snoindex
$ docker compose -f docker-compose.integration-test.yml up --exit-code-from snoindex
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
