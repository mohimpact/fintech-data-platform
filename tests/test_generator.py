from fintech_platform.config import VALID_CURRENCIES, VALID_MERCHANT_CATEGORIES
from fintech_platform.producer import generate_transaction


def test_generate_transaction_schema():
    txn = generate_transaction()

    expected_keys = {
        "transaction_id",
        "user_id",
        "amount",
        "currency",
        "timestamp",
        "merchant_name",
        "merchant_category",
        "location",
    }
    assert expected_keys.issubset(txn.keys())

    assert isinstance(txn["transaction_id"], str)
    assert isinstance(txn["user_id"], int)
    assert isinstance(txn["amount"], float)
    assert isinstance(txn["currency"], str)
    assert isinstance(txn["timestamp"], str)
    assert isinstance(txn["merchant_name"], str)
    assert isinstance(txn["merchant_category"], str)
    assert isinstance(txn["location"], str)

    assert txn["amount"] > 0
    assert txn["currency"] in VALID_CURRENCIES
    assert txn["merchant_category"] in VALID_MERCHANT_CATEGORIES
    assert len(txn["location"]) == 2
