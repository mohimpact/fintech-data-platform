import json
import time
import random
import uuid
from faker import Faker
from confluent_kafka import Producer
from datetime import datetime

# Initialize Faker
fake = Faker()

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'raw_transactions'

def receipt(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Produced message to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

def generate_transaction():
    """Generates a realistic mock fintech transaction."""
    
    # 5% chance of simulating a fraudulent transaction (e.g., extremely high amount)
    is_fraudulent = random.random() < 0.05
    
    amount = round(random.uniform(1.0, 100.0), 2)
    if is_fraudulent:
        amount = round(random.uniform(5000.0, 20000.0), 2)
        
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "merchant_name": fake.company(),
        "merchant_category": random.choice(["Retail", "Food", "Travel", "Electronics", "Entertainment", "Healthcare", "Utilities"]),
        "location": fake.country_code(),
        # We don't send the is_fraud flag, the pipeline must deduce it!
    }
    
    return transaction

def main():
    # Configure the Confluent Kafka Producer
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf)
    
    print(f"Starting transaction generation to topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            transaction = generate_transaction()
            # Serialize to JSON and encode to bytes
            record_value = json.dumps(transaction).encode('utf-8')
            
            # Produce the message
            producer.produce(
                topic=KAFKA_TOPIC,
                value=record_value,
                callback=receipt
            )
            
            # Serve delivery callback queue.
            producer.poll(0)
            
            # Random sleep to simulate realistic incoming traffic (between 10ms and 500ms)
            time.sleep(random.uniform(0.01, 0.5))
            
    except KeyboardInterrupt:
        print("\nStopping transaction generation...")
    finally:
        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
        producer.flush()
        print("Flushed all messages and exited cleanly.")

if __name__ == '__main__':
    main()
