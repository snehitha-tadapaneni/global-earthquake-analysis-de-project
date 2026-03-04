.PHONY: setup up down run test lint format dbt-run dbt-test dashboard

setup:
	pip install -r requirements.txt
	prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

up:
	docker compose up -d

down:
	docker compose down

format:
	black src flows dashboard tests
	ruff check --fix src flows dashboard tests

lint:
	black --check src flows dashboard tests
	ruff check src flows dashboard tests

test:
	pytest tests/

run:
	python flows/main_flow.py

dbt-run:
	cd dbt_project && dbt run --profiles-dir .

dbt-test:
	cd dbt_project && dbt test --profiles-dir .

dashboard:
	streamlit run dashboard/app.py
