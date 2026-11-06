import json
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer


KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce_events"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce",
    "user": "ecommerce",
    "password": "ecommerce_password",
}


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ecommerce-postgres-consumer",
)

connection = psycopg2.connect(**DB_CONFIG)
cursor = connection.cursor()

print("Kafka → PostgreSQL consumer started.")

for message in consumer:
    event = message.value

    try:
        cursor.execute(
            """
            INSERT INTO ecommerce_events (
                event_id,
                event_type,
                event_timestamp,
                user_id,
                product_id,
                session_id,
                device,
                location
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event["event_id"],
                event["event_type"],
                datetime.fromisoformat(event["timestamp"]),
                event["user_id"],
                event["product_id"],
                event["session_id"],
                event["device"],
                event["location"],
            ),
        )

        connection.commit()

        print(
            f"Inserted: {event['event_type']} | "
            f"{event['event_id']}"
        )

    except Exception as error:
        connection.rollback()
        print(f"Error: {error}")