import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import requests

from src.config import USGS_EARTHQUAKE_URL, RAW_DATA_DIR

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_earthquake_data(url: str = USGS_EARTHQUAKE_URL) -> Dict[str, Any]:
    """Fetches earthquake data from the USGS GeoJSON feed."""
    logger.info(f"Fetching data from {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def process_geojson(data: Dict[str, Any]) -> pd.DataFrame:
    """Processes the nested GeoJSON data into a flat Pandas DataFrame."""
    logger.info("Processing GeoJSON data")
    features = data.get("features", [])
    
    records = []
    for feature in features:
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates", [None, None, None])
        
        record = {
            "id": feature.get("id"),
            "mag": props.get("mag"),
            "place": props.get("place"),
            "time": props.get("time"), # ms since epoch
            "updated": props.get("updated"),
            "tz": props.get("tz"),
            "url": props.get("url"),
            "detail": props.get("detail"),
            "felt": props.get("felt"),
            "cdi": props.get("cdi"),
            "mmi": props.get("mmi"),
            "alert": props.get("alert"),
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "sig": props.get("sig"),
            "net": props.get("net"),
            "code": props.get("code"),
            "ids": props.get("ids"),
            "sources": props.get("sources"),
            "types": props.get("types"),
            "nst": props.get("nst"),
            "dmin": props.get("dmin"),
            "rms": props.get("rms"),
            "gap": props.get("gap"),
            "magType": props.get("magType"),
            "type": props.get("type"),
            "title": props.get("title"),
            "longitude": coords[0] if len(coords) > 0 else None,
            "latitude": coords[1] if len(coords) > 1 else None,
            "depth": coords[2] if len(coords) > 2 else None,
        }
        records.append(record)
        
    df = pd.DataFrame(records)
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Performs data quality checks."""
    logger.info("Validating data")
    
    if df.empty:
        logger.warning("DataFrame is empty. Skipping validation.")
        return df
        
    # 1. Schema Validation (ensure columns exist)
    required_columns = ["id", "mag", "place", "time", "updated", "longitude", "latitude"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in data: {missing_cols}")
        
    # 2. Null Checks (drop rows missing crucial identifiers)
    initial_len = len(df)
    df = df.dropna(subset=["id", "time", "updated"])
    if len(df) < initial_len:
        logger.warning(f"Dropped {initial_len - len(df)} rows due to missing essential fields")
        
    # 3. Anomaly Checks (magnitude limits)
    # Magnitudes rarely exceed 10.0, and time shouldn't be negative.
    df = df[(df["mag"] >= -2.0) & (df["mag"] <= 10.0) | df["mag"].isna()]
    
    return df

def save_raw_data(df: pd.DataFrame, file_prefix: str = "earthquakes") -> str:
    """Saves the dataframe to the raw data directory as Parquet."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = RAW_DATA_DIR / f"{file_prefix}_{timestamp}.parquet"
    
    logger.info(f"Saving {len(df)} rows to {filename}")
    df.to_parquet(filename, index=False)
    return str(filename)

def run_extraction() -> str:
    """Main function to run the extraction pipeline."""
    try:
        raw_json = fetch_earthquake_data()
        df = process_geojson(raw_json)
        validated_df = validate_data(df)
        filepath = save_raw_data(validated_df)
        logger.info("Extraction completed successfully")
        return filepath
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    run_extraction()
