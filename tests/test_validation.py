import pytest
from producer.producer import validate_order

def test_negative_amount_is_rejected():
    with pytest.raises(ValueError):
        validate_order({"order_id":"O", "customer":"A", "product":"P", "amount":-1, "status":"PLACED"})

def test_missing_field_is_rejected():
    with pytest.raises(ValueError):
        validate_order({"order_id":"O", "customer":"A", "amount":10, "status":"PLACED"})
