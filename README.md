# GLOBAL EARTHQUAKE ANALYSIS 🌍

A production-style, end-to-end data pipeline demonstrating data ingestion, warehousing, transformation, orchestration, and visualization using modern open-source tools.

## Architecture

![Architecture](https://img.shields.io/badge/Architecture-Data%20Pipeline-blue)

```text
[USGS API] --(Extract & Load)--> [PostgreSQL (Raw)]
   Python       (Pandas/Prefect)        Docker
                                          |
                                          | (Transform)
                                          v
[Streamlit Dashboard] <--(Serve)-- [PostgreSQL (Marts)]
                                        dbt (Core)
```

## Tech Stack
- **Languages:** Python, SQL
- **Extraction:** Requests, Pandas
- **Orchestration:** Prefect
- **Data Warehouse:** PostgreSQL (Dockerized)
- **Transformations:** dbt (data build tool)
- **Data Visualization:** Streamlit, Plotly
- **CI/CD & Code Quality:** GitHub Actions, Pytest, Black, Ruff

## Data Model (Star Schema)

- **Source:** USGS Earthquakes GeoJSON feed (Last 7 Days)
- **Staging (`stg_earthquakes`):** Cleansed raw data, time casting, deduplication.
- **Dimensions:**
  - `dim_date`: Extracted day, month, year, day of week.
  - `dim_location`: Surrogate keys for region strings.
- **Facts:**
  - `fact_earthquake`: Central fact table linking earthquakes to locations and dates.
- **Marts:**
  - `mart_earthquake_stats`: Aggregated KPIs by date and event type.

## Setup Instructions

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- `make` utility

### 1. Clone & Setup Environment
```bash
git clone <your-repo-link>
cd data-eng-portfolio
make setup
cp .env.example .env
```

### 2. Start PostgreSQL Warehouse
```bash
make up
```

### 3. Run the E2E Pipeline
Orchestrates extracting from USGS, loading to Postgres, and running dbt.
```bash
make run
```

### 4. Launch the Dashboard
```bash
make dashboard
```

## Data Quality & Testing
- **Ingestion Tests:** `pytest` validates schema integrity and filters out anomalous magnitudes during ingestion.
- **dbt Tests:** Checks for unique keys, nulls, and schema relationships (`make dbt-test`).
- **CI/CD:** Pushes to `main` trigger a GitHub Actions workflow that lints (Ruff, Black) and tests (Pytest) the codebase automatically.

## Understanding Incremental Loads
The Python ingestion script checks the target PostgreSQL `raw_earthquakes` table for the maximum `updated` timestamp. When pulling the weekly USGS GeoJSON feed, only records with an `updated` timestamp strictly greater than the database max are loaded, preventing massive duplication and acting as a robust incremental load strategy. Further deduplication acts as a safety net in the `stg_earthquakes` dbt model.

 locally in Docker, the architecture seamlessly ports to AWS/GCP (e.g., swapping Postgres for Snowflake/Redshift, and local Prefect for Airflow/Prefect Cloud)."
