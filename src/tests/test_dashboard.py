import pytest
import pandas as pd
from src.analytics.dashboard import generate_demo_data

def test_generate_demo_data():
    """
    Test that the demo data generator produces a valid pandas DataFrame
    with the required schema for the dashboard.
    """
    df = generate_demo_data()
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    
    # Check that required columns are present
    expected_columns = {
        "date", "merchant_category", "total_volume", 
        "transaction_count", "average_transaction_size", "fraud_count"
    }
    assert expected_columns.issubset(df.columns)
    
    # Check that there are 30 days of data multiplied by 7 categories = 210 rows
    assert len(df) == 210
    
    # Ensure numerical columns don't have negative values
    assert (df["total_volume"] >= 0).all()
    assert (df["transaction_count"] >= 0).all()
    assert (df["fraud_count"] >= 0).all()
