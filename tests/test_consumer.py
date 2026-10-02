from consumer.consumer import process_order, save_order
import json

def test_processing_changes_status_without_mutating_input():
    order = {"order_id":"O1", "customer":"A", "product":"P", "amount":10, "status":"PLACED"}
    result = process_order(order)
    assert result["status"] == "PROCESSED"
    assert order["status"] == "PLACED"

def test_save_order_writes_jsonl(tmp_path):
    path = tmp_path / "out.jsonl"
    order = {"order_id":"O1", "status":"PROCESSED"}
    save_order(order, str(path))
    assert json.loads(path.read_text().strip()) == order
