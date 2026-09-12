"""
Order Events Producer for Apache Kafka
Simulates real-time e-commerce order transactions:
- ORDER_CREATED
- ORDER_PAID
- ORDER_CANCELLED
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'order_events'

USERS = [f"usr-{i:03d}" for i in range(1, 21)]
PRODUCTS = [
    {"product_id": "prod-A", "name": "Wireless Headphones", "price": 149.99},
    {"product_id": "prod-B", "name": "Ergonomic Mouse", "price": 44.75},
    {"product_id": "prod-C", "name": "Mechanical Keyboard", "price": 299.00},
    {"product_id": "prod-D", "name": "USB-C Cable", "price": 45.00},
    {"product_id": "prod-E", "name": "Laptop Stand", "price": 59.99},
]
CITIES = ['Mumbai', 'Bengaluru', 'Delhi', 'Hyderabad', 'Pune', 'Chennai']
PAYMENT_METHODS = ['CREDIT_CARD', 'UPI', 'DEBIT_CARD', 'NET_BANKING']


def generate_order_event():
    order_id = f"ord-{uuid.uuid4().hex[:8]}"
    user_id = random.choice(USERS)
    city = random.choice(CITIES)
    selected_products = random.sample(PRODUCTS, k=random.randint(1, 3))
    
    order_items = []
    total_amount = 0.0
    for p in selected_products:
        qty = random.randint(1, 2)
        total_amount += p["price"] * qty
        order_items.append({
            "product_id": p["product_id"],
            "product_name": p["name"],
            "quantity": qty,
            "unit_price": p["price"]
        })

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "user_id": user_id,
        "event_type": "ORDER_CREATED",
        "total_amount": round(total_amount, 2),
        "shipping_city": city,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": "COMPLETED",
        "items": order_items,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def start_order_producer(events_count=30, delay_seconds=0.8):
    print(f"🚀 Connecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"📡 Publishing {events_count} order events to topic '{TOPIC_NAME}'...")
    for i in range(events_count):
        event = generate_order_event()
        producer.send(TOPIC_NAME, value=event)
        print(f"[{i+1}/{events_count}] Created Order {event['order_id']} | User: {event['user_id']} | Total: ₹{event['total_amount']} | City: {event['shipping_city']}")
        time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    print("✅ Finished publishing order events.")


if __name__ == "__main__":
    start_order_producer(events_count=20, delay_seconds=0.5)
