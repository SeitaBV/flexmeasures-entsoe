# Note: use tabs
# actions which are virtual, i.e. not a script
.PHONY: install install-for-dev test 


install:
	pip install -e .


# ---- Development ---

test:
	make install-for-dev
	pytest

install-for-dev:
	pip install -r requirements/app.in -r requirements/dev.in -r requirements/test.in
	make install
	pre-commit install

