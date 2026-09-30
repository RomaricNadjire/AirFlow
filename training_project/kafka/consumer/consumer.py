import json
import os
from datetime import datetime
from decimal import Decimal

from confluent_kafka import Consumer
from dotenv import load_dotenv
import psycopg
import csv
from confluent_kafka import Consumer, KafkaException

load_dotenv()

conf = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "group.id": "ecommerce-postgres-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

conn = psycopg.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)

consumer = Consumer(conf)

consumer.subscribe(["transactions"])

        # data = csv.DictReader(value.splitlines())  # header line handled automatically, columns can be reordered

def insert_transaction(conn, data):
    sql = """
        INSERT INTO transactions (
            transaction_id,
            customer_id,
            product_id,
            amount,
            country,
            timestamp,
            source_file
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (transaction_id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            amount = EXCLUDED.amount,
            country = EXCLUDED.country,
            timestamp = EXCLUDED.timestamp,
            source_file = EXCLUDED.source_file;
    """

    timestamp = datetime.fromisoformat(data["timestamp"])

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                int(data["transaction_id"]),
                int(data["customer_id"]),
                int(data["product_id"]),
                Decimal(str(data["amount"])),
                data["country"],
                timestamp,
                data.get("source_file"),
            ),
        )


try:
    with conn:

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                data = json.loads(
                    msg.value().decode("utf-8")
                )

                print(
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()} "
                    f"key={msg.key()} "
                    f"transaction_id={data['transaction_id']}"
                )

                # 1. PostgreSQL
                insert_transaction(conn, data)
                conn.commit()
                
                # 2. Kafka offset
                consumer.commit(
                    message=msg,
                    asynchronous=False
                )

                print("Processed successfully.")

            except Exception as exc:

                conn.rollback()

                print(
                    f"Processing failed: {exc}"
                )

                # Pas de commit Kafka.
                # Le message pourra être relu.

finally:
    consumer.close()