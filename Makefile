NAME := kumeza
UV := $(shell command -v uv 2> /dev/null)
RUFF := $(shell command -v ruff 2> /dev/null)

.DEFAULT_GOAL := help

.PHONY: help
help:
		@echo "Please use 'make <target> where <target> is one of:"
		@echo ""
		@echo " init				install prerequisites packages"
		@echo "	install				install uv package manager and ruff linter"
		@echo "	sync				install package dependencies"
		@echo "	clean				remove all temporary files"
		@echo "	lint				run the code linters"
		@echo "	format				format code according to formatter config"
		@echo "	test 				run all the tests"
		@echo " lambda-layer		build package as lambda layer"
		@echo ""
		@echo "Check the Makefile to know exactly what each target is doing."

.PHONY: init
init:
		python -m pip install --upgrade pip
		python -m pip install awscli
		python -m pip install git+https://github.com/awslabs/aws-glue-libs.git
		
.PHONY: install
install:
		@echo "Installing uv..."
		pip install uv
		@echo "Installing ruff..."
		pip install ruff
		@ruff --version

.PHONY: sync
sync:
		@if [ -z $(UV) ]; then echo "UV could not be found. See https://docs.astral.sh/uv/getting-started/installation/"; exit 2; fi
		$(UV) sync

.PHONY: clean
clean:
		find . -type d -name "__pycache__" | xargs rm -rf {};
		rm -rf .coverage .mypy_cache .pytest_cache ./dist ./htmlcov ./package

# .PHONY: format
# format: 
# 		$(RUFF) format --verbose

.PHONY: format
format: 
		$(UV) run isort --profile=black --lines-after-imports=2 ./tests/ $(NAME)
		$(UV) run black ./tests/ $(NAME)

# .PHONY: lint
# lint: 
# 		$(RUFF) check

.PHONY: lint
lint: 
		$(UV) run isort --profile=black --lines-after-imports=2 --check-only ./tests/ $(NAME)
		$(UV) run black --check ./tests/ $(NAME) --diff
		$(UV) run flake8 --ignore=W503,E501 ./tests/ $(NAME)
		$(UV) run mypy ./tests/ $(NAME) --ignore-missing-imports
		$(UV) run bandit -r $(NAME) -s B608

.PHONY: test
test: 
		$(UV) run pytest --cov $(NAME) --cov-report=term-missing --cov-report html --cov-fail-under 100 

build:
		$(UV) build

lambda-layer:
		$(UV) sync
		$(UV) build
		$(UV) run pip install --upgrade -t python dist/*.whl
		mkdir -p out; zip -r -q out/kumeza.zip python/* -i 'python/kumeza*' -x 'python/*.pyc'
		zip -r -q out/pyarrow.zip python/* -i 'python/*arrow*' -x 'python/*.pyc'