# 📍 Step 3: Multi-Source Data Ingestion (Debezium CDC + Kafka Event Producers + API Ingestor)

---

## 🎯 1. What Step 3 Accomplishes
In Step 3, we build the complete **Multi-Source Ingestion Layer** for DataNexus. We collect data from 3 distinct sources across 3 different ingestion paradigms:

1. **Change Data Capture (CDC):** Uses **Debezium** to capture live row-level database changes (INSERT, UPDATE, DELETE) from MySQL and PostgreSQL without querying or locking tables, streaming them into Kafka topics (`cdc.mysql.orders_db.orders`, `cdc.postgres.users_db.users`).
2. **Real-Time Clickstream & Order Events:** Python Kafka producers publish continuous user browsing actions (page views, search, product clicks, add-to-cart) to `clickstream_events` and live transactions to `order_events`.
3. **External REST API Ingestor:** Python scripts fetch live weather conditions (Open-Meteo) and currency exchange rates, storing raw JSON files directly into the **Bronze Data Lake** partitioned by date (`datalake/bronze/api/weather/` and `datalake/bronze/api/exchange_rates/`).

---

## 🧠 2. Deep Explanation of Technologies in Step 3

### 1. 🔄 Debezium & Kafka Connect (Port 8083)
* **What it is:** Debezium is a distributed platform for Change Data Capture built on top of Kafka Connect.
* **How it works:** It acts as a replica of MySQL and PostgreSQL. It tails the MySQL binary log and PostgreSQL Write-Ahead Log (WAL), instantly converting every database transaction into a structured JSON event and publishing it to dedicated Kafka topics.
* **Why this is critical:** Eliminates batch SQL polling (`SELECT * FROM orders WHERE updated_at > ...`). Polling puts heavy load on production databases and misses deleted rows. CDC is real-time, non-intrusive, and captures 100% of state transitions.

### 2. 📨 Python Kafka Producers (`kafka-python`)
* **What it is:** High-throughput streaming publishers.
* **How it works:** Serializes Python dictionaries into UTF-8 JSON bytes and pushes them asynchronously to Kafka brokers with partition-level buffering.

### 3. 🌐 REST API Ingestors
* **What it is:** Batch ingestion workers fetching external environmental signals.
* **Data Ingested:** 
  * City weather (temperature, rain, precipitation, wind speed) from Open-Meteo API.
  * Currency exchange rates against USD and INR.
* **Storage Pattern:** Writes raw immutable payloads into the Medallion Bronze Zone partitioned as `datalake/bronze/api/<source>/year=YYYY/month=MM/day=DD/`.

---

## 📁 3. Folder Structure for Step 3

```
DataNexus/
├── ingestion/
│   ├── debezium/
│   │   ├── mysql-connector.json         <-- Debezium CDC config for MySQL orders_db
│   │   └── postgres-connector.json      <-- Debezium CDC config for PostgreSQL users_db
│   ├── kafka_producers/
│   │   ├── clickstream_producer.py      <-- Simulates user browsing & cart activity
│   │   └── order_events_producer.py     <-- Simulates live order creation transactions
│   └── api_ingestor/
│       ├── config.py                    <-- API URLs, coordinates & Bronze paths
│       ├── weather_ingestor.py          <-- Fetches city weather & writes to Bronze
│       └── exchange_rate_ingestor.py    <-- Fetches FX rates & writes to Bronze
```

---

## 📝 4. Complete Code Files & Line-by-Line Explanations

### File 1: `ingestion/kafka_producers/clickstream_producer.py`
Simulates real-time e-commerce user activity: `PAGE_VIEW`, `SEARCH`, `PRODUCT_VIEW`, `ADD_TO_CART`, `REMOVE_FROM_CART`, `CHECKOUT_START`.

```python
import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'clickstream_events'

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
```

---

### File 2: `ingestion/api_ingestor/weather_ingestor.py`
Calls Open-Meteo API for top Indian e-commerce hubs and writes JSON payloads directly to `datalake/bronze/api/weather/`.

```python
import json
from datetime import datetime, timezone
import requests
from config import BRONZE_DIR, CITIES_COORDINATES, WEATHER_API_URL

def fetch_city_weather(city_name, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "weather_code", "wind_speed_10m"],
        "timezone": "auto"
    }
    response = requests.get(WEATHER_API_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    
    current = data.get("current", {})
    return {
        "city": city_name,
        "latitude": lat,
        "longitude": lon,
        "temperature_celsius": current.get("temperature_2m"),
        "apparent_temperature_celsius": current.get("apparent_temperature"),
        "humidity_percentage": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "rain_mm": current.get("rain"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "weather_code": current.get("weather_code"),
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }

def ingest_all_cities():
    print("🌦️ Starting Weather Data Ingestion for top e-commerce hubs...")
    all_weather_data = []

    for city, coords in CITIES_COORDINATES.items():
        try:
            weather_record = fetch_city_weather(city, coords["lat"], coords["lon"])
            all_weather_data.append(weather_record)
            print(f"  ✅ {city}: {weather_record['temperature_celsius']}°C | Rain: {weather_record['rain_mm']}mm | Wind: {weather_record['wind_speed_kmh']} km/h")
        except Exception as e:
            print(f"  ❌ Failed to fetch weather for {city}: {e}")

    now = datetime.now(timezone.utc)
    partition_path = BRONZE_DIR / "api" / "weather" / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    partition_path.mkdir(parents=True, exist_ok=True)

    output_file = partition_path / f"weather_{now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_weather_data, f, indent=2)

    print(f"💾 Raw weather data successfully written to Bronze Data Lake:\n   👉 {output_file}")
    return output_file

if __name__ == "__main__":
    ingest_all_cities()
```

---

### File 3: `ingestion/debezium/mysql-connector.json`
Debezium configuration registering CDC on MySQL `orders_db`.

```json
{
  "name": "mysql-orders-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "rootpassword",
    "database.server.id": "184054",
    "topic.prefix": "cdc.mysql",
    "database.include.list": "orders_db",
    "table.include.list": "orders_db.orders,orders_db.order_items,orders_db.payments",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schema-changes.mysql.orders",
    "include.schema.changes": "true",
    "decimal.handling.mode": "double"
  }
}
```

---

### File 4: `ingestion/debezium/postgres-connector.json`
Debezium configuration registering CDC on PostgreSQL `users_db`.

```json
{
  "name": "postgres-users-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgrespassword",
    "database.dbname": "users_db",
    "topic.prefix": "cdc.postgres",
    "table.include.list": "public.users,public.inventory",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_users_slot",
    "publication.autocreate.mode": "filtered",
    "decimal.handling.mode": "double"
  }
}
```

---

## 💻 5. Commands to Run & Verify Step 3

### Step 1: Install Python dependencies for Ingestion
```bash
pip install kafka-python requests
```

### Step 2: Restart Docker Stack with Debezium Kafka Connect
```bash
cd /Users/abhishekshankar/PROJECTS/DataNexus/docker
docker-compose up -d
```
*(Wait ~15 seconds for Debezium to initialize on port 8083)*

### Step 3: Register Debezium CDC Connectors
```bash
# Register MySQL Orders CDC Connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @/Users/abhishekshankar/PROJECTS/DataNexus/ingestion/debezium/mysql-connector.json

# Register PostgreSQL Users CDC Connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @/Users/abhishekshankar/PROJECTS/DataNexus/ingestion/debezium/postgres-connector.json
```

### Step 4: Verify CDC Connectors Status
```bash
curl -s http://localhost:8083/connectors/mysql-orders-cdc-connector/status
curl -s http://localhost:8083/connectors/postgres-users-cdc-connector/status
```
*(Both should report `"state": "RUNNING"`)*

### Step 5: Run Clickstream & Order Events Producers
```bash
python /Users/abhishekshankar/PROJECTS/DataNexus/ingestion/kafka_producers/clickstream_producer.py
python /Users/abhishekshankar/PROJECTS/DataNexus/ingestion/kafka_producers/order_events_producer.py
```

### Step 6: Run External API Ingestors (Weather & Exchange Rates)
```bash
python /Users/abhishekshankar/PROJECTS/DataNexus/ingestion/api_ingestor/weather_ingestor.py
python /Users/abhishekshankar/PROJECTS/DataNexus/ingestion/api_ingestor/exchange_rate_ingestor.py
```

### Step 7: Verify Kafka Topics & Ingested Bronze Data
```bash
# List all active Kafka topics (you should see cdc.mysql.orders_db.orders, clickstream_events, order_events)
docker exec -it datanexus-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Verify files created in Bronze Data Lake
ls -la /Users/abhishekshankar/PROJECTS/DataNexus/datalake/bronze/api/weather/*/*/*
ls -la /Users/abhishekshankar/PROJECTS/DataNexus/datalake/bronze/api/exchange_rates/*/*/*
```
