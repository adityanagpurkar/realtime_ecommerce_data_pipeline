import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce_events"

EVENT_TYPES = [
    "user_registered",
    "product_viewed",
    "cart_added",
    "checkout_started",
    "payment_completed",
    "order_cancelled",
]

LOCATIONS = [
    "Mumbai",
    "Pune",
    "Nagpur",
    "Bangalore",
    "Hyderabad",
    "Delhi",
]

DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]


def generate_event():
    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"USER_{random.randint(1, 1000)}",
        "product_id": f"PROD_{random.randint(1, 500)}",
        "session_id": f"SESSION_{random.randint(1, 5000)}",
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
    }

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print("Starting e-commerce event generator...")

    while True:
        event = generate_event()

        producer.send(TOPIC, value=event)
        producer.flush()

        print(event)

        time.sleep(1)


if __name__ == "__main__":
    main()