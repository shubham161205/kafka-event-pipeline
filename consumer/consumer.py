"""Consume order events and append processed results as JSON Lines."""
import json
import os
from pathlib import Path


def process_order(order: dict) -> dict:
    from producer.producer import validate_order
    validate_order(order)
    result = order.copy()
    result["status"] = "PROCESSED"
    return result


def save_order(order: dict, output_file: str = "data/processed_orders.jsonl") -> None:
    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(order) + "\n")


def consume_orders():
    from confluent_kafka import Consumer
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "order-processors"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.getenv("KAFKA_TOPIC", "orders")])
    output = os.getenv("OUTPUT_FILE", "data/processed_orders.jsonl")
    print("Consumer listening for order events...", flush=True)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Kafka error: {message.error()}", flush=True)
                continue
            try:
                order = json.loads(message.value().decode("utf-8"))
                processed = process_order(order)
                save_order(processed, output)
                consumer.commit(message=message, asynchronous=False)
                print(f"Processed {processed['order_id']}", flush=True)
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                print(f"Rejected invalid event: {exc}", flush=True)
                consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping consumer", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_orders()
