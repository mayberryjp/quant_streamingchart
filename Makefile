.PHONY: install lint typecheck security test migrate docker-build docker-run

install:
	pip install .[dev]

lint:
	ruff check .

typecheck:
	mypy src

security:
	bandit -r src

test:
	pytest -q

migrate:
	alembic upgrade head

docker-build:
	docker build -t quant-streamingchart:dev .

docker-run:
	docker compose up
