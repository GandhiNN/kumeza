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
		pwd
		ls -lrt .
		ls -lrt /tmp
		ls -lrt ~/.local/lib
		@curl https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz
		@tar -xvzf spark-3.5.1-bin-hadoop3.tgz -C /opt/spark 
		@rm -rf spark-3.5.1-bin-hadoop3.tgz
		@echo export PATH=/opt/spark/sbin:/opt/spark/bin:${PATH} >> ~/bashrc
		@echo export SPARK_HOME=/opt/spark/ >> ~/bashrc
		@echo export PYSPARK_PYTHON=python3 >> ~/bashrc

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