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
from pathlib import Path

try:
    from kafka import KafkaProducer
except ImportError:
    try:
        from kafka_python_ng import KafkaProducer
    except ImportError:
        KafkaProducer = None

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'order_events'
BRONZE_ORDERS_DIR = Path(__file__).resolve().parents[2] / "datalake" / "bronze" / "kafka" / "order_events"

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


def start_order_producer(events_count=20, delay_seconds=0.05):
    producer = None
    if KafkaProducer is not None:
        try:
            print(f"🚀 Connecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=3000
            )
        except Exception as e:
            print(f"⚠️ Kafka connection note: {e}")

    print(f"📡 Generating {events_count} order events...")
    events = []
    for i in range(events_count):
        event = generate_order_event()
        events.append(event)
        if producer:
            try:
                producer.send(TOPIC_NAME, value=event)
            except Exception:
                pass
        print(f"  [{i+1}/{events_count}] Order {event['order_id']} | User: {event['user_id']} | ₹{event['total_amount']} | {event['shipping_city']}")
        time.sleep(delay_seconds)

    if producer:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

    # Ensure events land in Bronze Data Lake
    now = datetime.now(timezone.utc)
    out_dir = BRONZE_ORDERS_DIR / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"orders_{now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"💾 Bronze Order Events Lake verified at:\n   👉 {out_file}")


if __name__ == "__main__":
    start_order_producer(events_count=20, delay_seconds=0.02)
