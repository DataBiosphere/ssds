include common.mk

MODULES=ssds tests

tests:=$(wildcard tests/test_*.py)
test: lint mypy tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test: $(tests)
	coverage combine
	rm -f .coverage.*

# A pattern rule that runs a single test script
$(tests): %.py : mypy lint
	coverage run -p --source=ssds $*.py --verbose

version: ssds/version.py

ssds/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

sdist: clean version
	python setup.py sdist

build: clean version
	-rm -rf dist
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl

.PHONY: test lint mypy tests clean sdist build install package_samtools
