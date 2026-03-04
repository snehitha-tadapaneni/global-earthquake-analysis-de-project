import subprocess
import logging
from prefect import flow, task

from src.ingestion.extract import fetch_earthquake_data, process_geojson, validate_data, save_raw_data
from src.database.postgres import load_to_postgres

# Setup basic logging for Prefect
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task(name="Extract Data from USGS", retries=3, retry_delay_seconds=10)
def extract_task():
    raw_json = fetch_earthquake_data()
    return raw_json

@task(name="Process and Validate geojson")
def transform_raw_task(raw_json):
    df = process_geojson(raw_json)
    validated_df = validate_data(df)
    return validated_df

@task(name="Save Raw to Parquet")
def save_local_task(df):
    filepath = save_raw_data(df)
    logger.info(f"Saved raw data to {filepath}")
    return filepath

@task(name="Load Raw Data to Postgres")
def load_postgres_task(df):
    load_to_postgres(df)

@task(name="Run dbt Models")
def run_dbt_task():
    # Run dbt using subprocess
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="dbt_project",
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"dbt run failed: {result.stdout}\n{result.stderr}")
        raise RuntimeError("dbt run failed")
    logger.info(f"dbt run success: {result.stdout}")
    return result.stdout

@flow(name="Earthquake Data Pipeline")
def earthquake_pipeline_flow():
    # 1. Ingestion
    raw_json = extract_task()
    df = transform_raw_task(raw_json)
    save_local_task(df)
    
    # 2. Storage
    load_postgres_task(df)
    
    # 3. Transformation
    run_dbt_task()

if __name__ == "__main__":
    earthquake_pipeline_flow()
