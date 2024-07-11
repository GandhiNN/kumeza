NAME := kumeza
INSTALL_STAMP := .install.stamp
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
		yum install -y zip jq
		python -m pip install --upgrade pip
		pip install -r requirements.txt
		
.PHONY: install-poetry
install-poetry:
		@echo "Installing poetry..."
		@curl -sSL https://install.python-poetry.org | POETRY_HOME=/etc/poetry python3 -
		PATH=/etc/poetry/bin:$(PATH)
		${POETRY} --version

.PHONY: install
install: $(INSTALL_STAMP)
$(INSTALL_STAMP): pyproject.toml poetry.lock
		PATH=/etc/poetry/bin:$(PATH)
		@if [ -z $(POETRY) ]; then echo "Poetry could not be found. See https://python-poetry.org/docs/"; exit 2; fi
		$(POETRY) install
		touch $(INSTALL_STAMP)

.PHONY: clean
clean:
		find . -type d -name "__pycache__" | xargs rm -rf {};
		rm -rf $(INSTALL_STAMP) .coverage .mypy_cache .pytest_cache ./dist

.PHONY: lint
lint: $(INSTALL_STAMP)
		$(POETRY) run isort --profile=black --lines-after-imports=2 --check-only ./tests/ $(NAME)
		$(POETRY) run black --check ./tests/ $(NAME) --diff
		$(POETRY) run flake8 --ignore=W503,E501 ./tests/ $(NAME)
		$(POETRY) run mypy ./tests/ $(NAME) --ignore-missing-imports
		$(POETRY) run bandit -r $(NAME) -s B608

.PHONY: format
format: $(INSTALL_STAMP)
		$(POETRY) run isort --profile=black --lines-after-imports=2 ./tests/ $(NAME)
		$(POETRY) run black ./tests/ $(NAME)

.PHONY: test
test: $(INSTALL_STAMP)
		$(POETRY) run pytest ./tests/ --cov-report term-missing --cov-fail-under 100 --cov $(NAME)