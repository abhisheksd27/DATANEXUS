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
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'clickstream_events'

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


def start_producer(events_count=100, delay_seconds=0.5):
    print(f"🚀 Connecting to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"📡 Publishing {events_count} clickstream events to topic '{TOPIC_NAME}'...")
    for i in range(events_count):
        event = generate_clickstream_event()
        producer.send(TOPIC_NAME, value=event)
        print(f"[{i+1}/{events_count}] Published {event['event_type']} for {event['user_id']} ({event['city']})")
        time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    print("✅ Finished publishing clickstream events.")


if __name__ == "__main__":
    start_producer(events_count=50, delay_seconds=0.3)
