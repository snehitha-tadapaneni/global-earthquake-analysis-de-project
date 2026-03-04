import pytest
import pandas as pd

from src.ingestion.extract import process_geojson, validate_data

@pytest.fixture
def mock_geojson_data():
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": 1679000000000,
            "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson",
            "title": "USGS All Earthquakes, Past Week",
            "status": 200,
            "api": "1.10.3",
            "count": 2
        },
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "mag": 1.2,
                    "place": "10 km SSW of Volcano, Hawaii",
                    "time": 1679000000000,
                    "updated": 1679000000100,
                    "tz": None,
                    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/hv73350027",
                    "detail": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/hv73350027.geojson",
                    "felt": None,
                    "cdi": None,
                    "mmi": None,
                    "alert": None,
                    "status": "reviewed",
                    "tsunami": 0,
                    "sig": 22,
                    "net": "hv",
                    "code": "73350027",
                    "ids": ",hv73350027,",
                    "sources": ",hv,",
                    "types": ",origin,phase-data,",
                    "nst": 10,
                    "dmin": 0.05,
                    "rms": 0.1,
                    "gap": 100,
                    "magType": "ml",
                    "type": "earthquake",
                    "title": "M 1.2 - 10 km SSW of Volcano, Hawaii"
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-155.25, 19.3, 5.0]
                },
                "id": "hv73350027"
            },
            {
                "type": "Feature",
                "properties": {
                    "mag": -3.0, # Invalid anomaly to test filtering
                    "place": "Unknown",
                    "time": 1679000001000,
                    "updated": 1679000001100,
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [0, 0, 0]
                },
                "id": "bad123"
            }
        ]
    }

def test_process_geojson(mock_geojson_data):
    df = process_geojson(mock_geojson_data)
    assert len(df) == 2
    assert "id" in df.columns
    assert "longitude" in df.columns
    assert df.iloc[0]["longitude"] == -155.25

def test_validate_data(mock_geojson_data):
    df = process_geojson(mock_geojson_data)
    validated_df = validate_data(df)
    
    # Should drop the second row because mag is -3.0 (anomaly)
    assert len(validated_df) == 1
    assert "hv73350027" in validated_df["id"].values
