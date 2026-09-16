"""
Clickstream Event Producer for Apache Kafka
Simulates real-time e-commerce user activity:
- PAGE_VIEW
- SEARCH
- PRODUCT_VIEW
- ADD_TO_CART
- REMOVE_FROM_CART
- CHECKOUT_START
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
TOPIC_NAME = 'clickstream_events'
BRONZE_FALLBACK_DIR = Path(__file__).resolve().parents[2] / "datalake" / "bronze" / "kafka" / "clickstream"

# Mock Data Pools
USERS = [f"usr-{i:03d}" for i in range(1, 21)]
PRODUCTS = [f"prod-{c}" for c in ['A', 'B', 'C', 'D', 'E', 'F', 'G']]
CITIES = ['Mumbai', 'Bengaluru', 'Delhi', 'Hyderabad', 'Pune', 'Chennai']
EVENT_TYPES = [
    ('PAGE_VIEW', 0.40),
    ('SEARCH', 0.20),
    ('PRODUCT_VIEW', 0.20),
    ('ADD_TO_CART', 0.12),
    ('REMOVE_FROM_CART', 0.05),
    ('CHECKOUT_START', 0.03),
]
DEVICE_TYPES = ['mobile_android', 'mobile_ios', 'desktop_chrome', 'desktop_safari']
SEARCH_TERMS = ['wireless headphones', 'gaming mouse', 'mechanical keyboard', 'usb-c cable', 'laptop stand', 'monitor']


def get_weighted_event_type():
    events, weights = zip(*EVENT_TYPES)
    return random.choices(events, weights=weights)[0]


def generate_clickstream_event():
    event_type = get_weighted_event_type()
    user_id = random.choice(USERS)
    product_id = random.choice(PRODUCTS) if event_type in ['PRODUCT_VIEW', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'CHECKOUT_START'] else None
    search_query = random.choice(SEARCH_TERMS) if event_type == 'SEARCH' else None

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product_id,
        "search_query": search_query,
        "device_type": random.choice(DEVICE_TYPES),
        "city": random.choice(CITIES),
        "ip_address": f"192.168.1.{random.randint(2, 254)}",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def start_producer(events_count=50, delay_seconds=0.05):
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

    print(f"📡 Generating {events_count} clickstream events...")
    events = []
    for i in range(events_count):
        event = generate_clickstream_event()
        events.append(event)
        if producer:
            try:
                producer.send(TOPIC_NAME, value=event)
            except Exception:
                pass
        print(f"  [{i+1}/{events_count}] {event['event_type']} by {event['user_id']} ({event['city']})")
        time.sleep(delay_seconds)

    if producer:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

    # Ensure events land in Bronze Data Lake
    now = datetime.now(timezone.utc)
    out_dir = BRONZE_FALLBACK_DIR / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"clickstream_{now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"💾 Bronze Clickstream Lake verified at:\n   👉 {out_file}")


if __name__ == "__main__":
    start_producer(events_count=50, delay_seconds=0.02)
