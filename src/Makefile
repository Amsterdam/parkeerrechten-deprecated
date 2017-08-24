.PHONY: test testcov flake run runimport

PYTHON = python3
PYTEST = pytest
PYTEST_OPTS ?= -p no:cacheprovider --capture=no --verbose
TESTS ?= parkeerrechten/tests

test:
	$(PYTEST) $(PYTEST_OPTS) $(TESTS)

testcov:
	$(PYTEST) $(PYTEST_OPTS) --cov-config .coveragerc --cov=parkeerrechten --cov-report=term $(TESTS)

flake:
	flake8 --config=./flake.cfg parkeerrechten tests

runimport:
	$(PYTHON) -c 'from parkeerrechten import importer; importer.run_import()'

