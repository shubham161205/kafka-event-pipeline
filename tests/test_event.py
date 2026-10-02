from producer.producer import create_order, validate_order

def test_order_creation_has_expected_fields():
    order = create_order("O1", "Sam", "Book", 250)
    assert order == {"order_id": "O1", "customer": "Sam", "product": "Book", "amount": 250, "status": "PLACED"}

def test_valid_order_is_accepted():
    assert validate_order(create_order("O2", "Ari", "Pen", 10)) is True
