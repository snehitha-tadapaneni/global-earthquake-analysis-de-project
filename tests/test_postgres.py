from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

from src.database.postgres import load_to_postgres

@pytest.fixture
def mock_df():
    return pd.DataFrame({
        "id": ["1", "2"],
        "updated": [1679000000000, 1679000001000]
    })

@patch("src.database.postgres.get_engine")
@patch("src.database.postgres.get_max_timestamp")
def test_load_to_postgres_incremental(mock_get_max, mock_get_engine, mock_df):
    mock_get_max.return_value = 1679000000500 # Will filter out the first row
    
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    
    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        load_to_postgres(mock_df)
        mock_to_sql.assert_called_once()
        
@patch("src.database.postgres.get_engine")
@patch("src.database.postgres.get_max_timestamp")
def test_load_to_postgres_empty_after_filter(mock_get_max, mock_get_engine, mock_df):
    mock_get_max.return_value = 1679000002000 # Keeps 0 rows
    
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    
    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        load_to_postgres(mock_df)
        mock_to_sql.assert_not_called()
