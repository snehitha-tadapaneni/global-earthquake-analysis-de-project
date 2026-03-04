import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from src.config import (POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD,
                        POSTGRES_PORT, POSTGRES_USER)

logger = logging.getLogger(__name__)

def get_engine():
    """Returns a SQLAlchemy engine connected to the Postgres database."""
    url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url)

def get_max_timestamp(table_name: str = "raw_earthquakes", column: str = "updated") -> Optional[int]:
    """Fetches the maximum timestamp from the table to support incremental loads."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            # Check if table exists first
            result = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table)"
            ), {"table": table_name})
            
            if not result.scalar():
                return None
                
            result = conn.execute(text(f"SELECT MAX({column}) FROM {table_name}"))
            max_val = result.scalar()
            return int(max_val) if max_val else None
    except Exception as e:
        logger.warning(f"Could not fetch max timestamp from {table_name}: {e}")
        return None

def load_to_postgres(df: pd.DataFrame, table_name: str = "raw_earthquakes") -> None:
    """Loads a Pandas DataFrame to PostgreSQL.
    
    If table doesn't exist, it creates it.
    If it exists, we append new records based on the incremental filtering.
    """
    if df.empty:
        logger.info(f"Empty dataframe, nothing to load into {table_name}")
        return
        
    engine = get_engine()
    
    # Simple incremental check
    max_ts = get_max_timestamp(table_name)
    if max_ts:
        initial_len = len(df)
        df = df[df["updated"] > max_ts]
        logger.info(f"Incremental load: Filtered out {initial_len - len(df)} already existing records")
        
    if df.empty:
        logger.info(f"No new records to load into {table_name} after incremental filtering")
        return

    try:
        logger.info(f"Loading {len(df)} rows into {table_name}")
        df.to_sql(table_name, engine, if_exists="append", index=False)
        logger.info("Successfully loaded data to Postgres")
    except Exception as e:
        logger.error(f"Failed to load data to Postgres: {e}")
        raise
