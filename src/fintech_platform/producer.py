"""Synthetic transaction producer for the raw Kafka topic."""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer
from faker import Faker

from fintech_platform.config import (
    KafkaConfig,
    RAW_TRANSACTIONS_TOPIC,
    VALID_CURRENCIES,
    VALID_MERCHANT_CATEGORIES,
)


log = logging.getLogger("fintech_platform.producer")
fake = Faker()


def generate_transaction() -> dict:
    """Generate a realistic mock card transaction."""
    high_value = random.random() < 0.05
    amount = random.uniform(5_000.0, 20_000.0) if high_value else random.uniform(1.0, 100.0)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "amount": round(amount, 2),
        "currency": random.choice(VALID_CURRENCIES),
        "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "merchant_name": fake.company(),
        "merchant_category": random.choice(VALID_MERCHANT_CATEGORIES),
        "location": fake.country_code(),
    }


def delivery_report(error, message) -> None:
    if error is not None:
        log.error("Failed to deliver message: %s", error)
        return

    log.info(
        "Produced %s[%s]@%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def run(interval_min_seconds: float = 0.01, interval_max_seconds: float = 0.5) -> None:
    kafka = KafkaConfig()
    producer = Producer({"bootstrap.servers": kafka.bootstrap_servers})

    log.info("Producing transactions to %s via %s", RAW_TRANSACTIONS_TOPIC, kafka.bootstrap_servers)
    try:
        while True:
            producer.produce(
                topic=RAW_TRANSACTIONS_TOPIC,
                value=json.dumps(generate_transaction()).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(interval_min_seconds, interval_max_seconds))
    except KeyboardInterrupt:
        log.info("Stopping transaction producer")
    finally:
        producer.flush()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    run()


if __name__ == "__main__":
    main()
