"""Publish sample order events to a Kafka topic."""
import json
import os
import time


def create_order(order_id: str, customer: str, product: str, amount: float) -> dict:
    order = {"order_id": order_id, "customer": customer, "product": product,
             "amount": amount, "status": "PLACED"}
    validate_order(order)
    return order


def validate_order(order: dict) -> bool:
    required = ("order_id", "customer", "product", "amount", "status")
    if not isinstance(order, dict) or any(k not in order for k in required):
        raise ValueError("Order is missing required fields")
    if any(not isinstance(order[k], str) or not order[k].strip()
           for k in ("order_id", "customer", "product", "status")):
        raise ValueError("Text fields must be non-empty strings")
    amount = order["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, float)) or amount <= 0:
        raise ValueError("Amount must be a positive number")
    return True


def publish_orders():
    # Import here so unit tests do not require a running Kafka broker.
    from confluent_kafka import Producer
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = Producer({"bootstrap.servers": broker})
    topic = os.getenv("KAFKA_TOPIC", "orders")
    for i in range(1, 6):
        order = create_order(f"ORD{i}", f"Customer{i}", "Laptop", 50000 + i * 1000)
        producer.produce(topic, key=order["order_id"], value=json.dumps(order))
        producer.poll(0)
        print(f"Queued order {order['order_id']}", flush=True)
        time.sleep(1)
    producer.flush(15)
    print("Finished publishing orders", flush=True)


if __name__ == "__main__":
    publish_orders()
