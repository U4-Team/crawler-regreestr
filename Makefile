include ./src/.env
export

VENV_PATH?=./env

.PHONY: run
run:
	crawler_regreestr run

.PHONY: repl
repl:
	ipython

.PHONY: test
test:
	pytest --cov=src -vv -s

.PHONY: lint
lint:
	pylint src

.PHONY: install
install:
	if [ ! -d $(VENV_PATH) ]; then python3 -m venv $(VENV_PATH); fi;
	$(VENV_PATH)/bin/pip install -r requirements.txt && $(VENV_PATH)/bin/pip install -e '.[test]';

.PHONY: build
build:
	docker build -t crawler-regreestr .
