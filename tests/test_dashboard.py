import pandas as pd
from fintech_platform.analytics.data import generate_demo_data


def test_generate_demo_data():
    df = generate_demo_data()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    expected_columns = {
        "date",
        "merchant_category",
        "total_volume",
        "transaction_count",
        "average_transaction_size",
        "fraud_count",
    }
    assert expected_columns.issubset(df.columns)

    assert len(df) == 210
    assert (df["total_volume"] >= 0).all()
    assert (df["transaction_count"] >= 0).all()
    assert (df["fraud_count"] >= 0).all()
