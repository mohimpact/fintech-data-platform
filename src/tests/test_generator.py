import pytest
from src.generator.generator import generate_transaction

def test_generate_transaction_schema():
    """
    Test that the generator produces a transaction with the required fields
    and correct data types.
    """
    txn = generate_transaction()
    
    # Check that required keys are present
    expected_keys = {
        "transaction_id", "user_id", "amount", 
        "currency", "timestamp", "merchant_name", 
        "merchant_category", "location"
    }
    assert expected_keys.issubset(txn.keys())
    
    # Check data types
    assert isinstance(txn["transaction_id"], str)
    assert isinstance(txn["user_id"], int)
    assert isinstance(txn["amount"], float)
    assert isinstance(txn["currency"], str)
    assert isinstance(txn["timestamp"], str)
    assert isinstance(txn["merchant_name"], str)
    assert isinstance(txn["merchant_category"], str)
    assert isinstance(txn["location"], str)
    
    # Ensure amount is positive
    assert txn["amount"] > 0
