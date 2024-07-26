NAME := kumeza
POETRY := $(shell command -v poetry 2> /dev/null)

.DEFAULT_GOAL := help

.PHONY: help
help:
		@echo "Please use 'make <target> where <target> is one of:"
		@echo ""
		@echo "	install		install packages and prepare environment"
		@echo "	clean		remove all temporary files"
		@echo "	lint		run the code linters"
		@echo "	format		reformat code"
		@echo "	test 		run all the tests"
		@echo ""
		@echo "Check the Makefile to know exactly what each target is doing."

.PHONY: init
init:
		yum install -y zip jq java
		python -m pip install --upgrade pip
		java -version
		
.PHONY: install-poetry
install-poetry:
		@echo "Installing poetry..."
		pip install poetry
		$(POETRY) --version

.PHONY: install-pyspark
install-pyspark:
		@echo "Installing PySpark..."
		pip install pyspark
		@pyspark --version

.PHONY: install
install:
		@if [ -z $(POETRY) ]; then echo "Poetry could not be found. See https://python-poetry.org/docs/"; exit 2; fi
		$(POETRY) install

.PHONY: clean
clean:
		find . -type d -name "__pycache__" | xargs rm -rf {};
		rm -rf .coverage .mypy_cache .pytest_cache ./dist

.PHONY: lint
lint: 
		$(POETRY) run isort --profile=black --lines-after-imports=2 --check-only ./tests/ $(NAME)
		$(POETRY) run black --check ./tests/ $(NAME) --diff
		$(POETRY) run flake8 --ignore=W503,E501 ./tests/ $(NAME)
		$(POETRY) run mypy ./tests/ $(NAME) --ignore-missing-imports
		$(POETRY) run bandit -r $(NAME) -s B608

.PHONY: format
format: 
		$(POETRY) run isort --profile=black --lines-after-imports=2 ./tests/ $(NAME)
		$(POETRY) run black ./tests/ $(NAME)

.PHONY: test
test: 
		$(POETRY) run pytest ./tests/ --cov-report term-missing --cov-fail-under 100 --cov $(NAME) --cov-report html

build:
		$(POETRY) build