# Step 1 — Local Development Infrastructure Setup

## What We Are Doing
Setting up the entire local development environment using Docker and Docker Compose.
We will run 6 services as containers on a shared virtual network called `datanexus-net`.

---

## Why Docker?
- You do NOT install MySQL, Kafka, Spark etc directly on your laptop
- Each service runs in an isolated container — no conflicts between tools
- One command brings everything up: `docker-compose up -d`
- One command brings everything down: `docker-compose down`
- Everyone on the team gets the same environment every single time

---

## Services We Are Starting

| Container Name            | Image                          | Port  | Role                                           |
|---------------------------|--------------------------------|-------|------------------------------------------------|
| datanexus-mysql           | mysql:8.0                      | 3306  | Orders & Payments OLTP database                |
| datanexus-postgres        | postgres:15                    | 5432  | Users & Inventory OLTP database                |
| datanexus-zookeeper       | confluentinc/cp-zookeeper:7.5  | 2181  | Kafka cluster coordinator                      |
| datanexus-kafka           | confluentinc/cp-kafka:7.5      | 9092  | Event streaming / message broker               |
| datanexus-spark-master    | bitnami/spark:3.5.0            | 8080  | Spark cluster manager + Web UI                 |
| datanexus-spark-worker    | bitnami/spark:3.5.0            | -     | Spark compute executor (2 cores, 2GB RAM)      |

---

## Technology Explanations

### Docker
- Packages an application + all its dependencies into a container
- Container is lightweight and portable — runs the same on any machine
- `docker-compose.yml` = blueprint that defines all containers together

### Docker Bridge Network (`datanexus-net`)
- All 6 containers are connected on this virtual network
- Containers talk to each other by name (e.g., Spark connects to Kafka using hostname `kafka`)
- Nothing is exposed to the internet unless you explicitly map a port

### MySQL 8.0
- Relational database — stores data in tables with rows and columns
- Simulates the Orders Microservice database (`orders_db`)
- Tables: `orders`, `order_items`, `payments`
- IMPORTANT: We configure Binary Logging (`binlog_format = ROW`) so Debezium CDC can read changes in Step 3
- Binary Log = a file where MySQL records every row change (INSERT, UPDATE, DELETE)

### PostgreSQL 15
- Relational database — more advanced than MySQL, supports JSON, arrays, complex types
- Simulates the User & Inventory Microservice database (`users_db`)
- Tables: `users`, `inventory`
- IMPORTANT: We configure `wal_level = logical` so Debezium CDC can stream changes in Step 3
- WAL = Write-Ahead Log — every change is recorded here before it hits the actual table

### Zookeeper
- Coordination service for distributed systems
- Kafka uses it to track which broker is the leader, which partitions are where
- Think of it as the "manager" of the Kafka cluster
- In newer Kafka versions it is being replaced by KRaft mode, but we use it here for stability

### Apache Kafka
- Distributed event streaming platform
- Think of it as a post office — producers drop messages, consumers pick them up
- Data is organized into Topics (like folders)
- Topics we will use: `orders_topic`, `clickstream_topic`, `cdc.mysql.orders`, `cdc.postgres.users`
- Messages are NOT deleted after reading — they stay for a configured period (default 7 days)
- Multiple consumers can read the same message independently

### Apache Spark Master
- The brain of the Spark cluster
- Receives PySpark jobs and distributes tasks to workers
- Hosts a Web UI at `http://localhost:8080` — you can see jobs, workers, memory
- Port 7077 = the RPC port workers use to register with the master

### Apache Spark Worker
- The muscle of the Spark cluster
- Actually executes the data processing tasks
- We give it 2 CPU cores and 2GB of RAM
- In production, you have many workers across many machines

---

## Key Configuration Details

### MySQL CDC Configuration (my.cnf)
```
server-id        = 223344    → Unique ID for this MySQL server in replication
log_bin          = mysql-bin → Enable binary logging (CDC needs this)
binlog_format    = ROW       → Log actual row changes (not SQL statements)
binlog_row_image = FULL      → Log both before and after state of every row
expire_logs_days = 7         → Keep binary logs for 7 days only
```

### PostgreSQL CDC Configuration (postgresql.conf)
```
wal_level            = logical → Enables logical decoding for CDC
max_wal_senders      = 4       → Up to 4 concurrent WAL streaming connections
max_replication_slots = 4      → Up to 4 replication slots (one per CDC connector)
```

### Kafka Configuration (docker-compose.yml)
```
KAFKA_BROKER_ID                          = 1
→ Unique ID for this Kafka broker in the cluster

KAFKA_ZOOKEEPER_CONNECT                  = zookeeper:2181
→ Tells Kafka where Zookeeper is running (by container name)

KAFKA_ADVERTISED_LISTENERS               = PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
→ Two listeners:
  - PLAINTEXT://kafka:29092   → Used by other containers inside Docker network
  - PLAINTEXT_HOST://localhost:9092 → Used by apps on your laptop (outside Docker)

KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR   = 1
→ We have only 1 broker, so replication factor must be 1
```

---

## Folder Structure for Step 1

```
DataNexus/
└── docker/
    ├── docker-compose.yml           ← Master file that starts all 6 containers
    ├── mysql/
    │   ├── my.cnf                   ← MySQL config: enables binary log for CDC
    │   └── init.sql                 ← Creates orders_db schema + inserts seed data
    └── postgres/
        ├── postgresql.conf          ← PostgreSQL config: enables WAL logical replication
        └── init.sql                 ← Creates users_db schema + inserts seed data
```

---

## Files to Create

### 1. `docker/mysql/my.cnf`
```ini
[mysqld]
server-id                  = 223344
log_bin                    = mysql-bin
binlog_format              = ROW
binlog_row_image           = FULL
binlog_expire_logs_seconds = 604800
```

### 2. `docker/mysql/init.sql`
```sql
CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

CREATE TABLE IF NOT EXISTS orders (
    order_id     VARCHAR(36) PRIMARY KEY,
    user_id      VARCHAR(36) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status       VARCHAR(20) NOT NULL,
    city         VARCHAR(50) NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id    VARCHAR(36) PRIMARY KEY,
    order_id   VARCHAR(36) NOT NULL,
    product_id VARCHAR(36) NOT NULL,
    quantity   INT NOT NULL,
    price      DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id     VARCHAR(36) PRIMARY KEY,
    order_id       VARCHAR(36) NOT NULL,
    method         VARCHAR(30) NOT NULL,
    status         VARCHAR(20) NOT NULL,
    amount         DECIMAL(10, 2) NOT NULL,
    paid_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

INSERT INTO orders VALUES
('ord-101','usr-001',149.99,'COMPLETED','Mumbai',NOW()),
('ord-102','usr-002',89.50,'PENDING','Bengaluru',NOW()),
('ord-103','usr-003',299.00,'COMPLETED','Delhi',NOW());

INSERT INTO order_items VALUES
('itm-01','ord-101','prod-A',1,149.99),
('itm-02','ord-102','prod-B',2,44.75),
('itm-03','ord-103','prod-C',1,299.00);

INSERT INTO payments VALUES
('pay-501','ord-101','CREDIT_CARD','SUCCESS',149.99,NOW()),
('pay-502','ord-102','UPI','PENDING',89.50,NOW()),
('pay-503','ord-103','DEBIT_CARD','SUCCESS',299.00,NOW());
```

### 3. `docker/postgres/postgresql.conf`
```ini
wal_level             = logical
max_wal_senders       = 4
max_replication_slots = 4
```

### 4. `docker/postgres/init.sql`
```sql
CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR(36) PRIMARY KEY,
    full_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(100) UNIQUE NOT NULL,
    city        VARCHAR(50) NOT NULL,
    signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory (
    product_id     VARCHAR(36) PRIMARY KEY,
    product_name   VARCHAR(100) NOT NULL,
    category       VARCHAR(50) NOT NULL,
    stock_quantity INT NOT NULL,
    unit_price     DECIMAL(10, 2) NOT NULL
);

INSERT INTO users VALUES
('usr-001','Aarav Sharma','aarav@example.com','Mumbai',NOW()),
('usr-002','Priya Patel','priya@example.com','Bengaluru',NOW()),
('usr-003','Rohan Verma','rohan@example.com','Delhi',NOW());

INSERT INTO inventory VALUES
('prod-A','Wireless Headphones','Electronics',150,149.99),
('prod-B','Ergonomic Mouse','Electronics',300,44.75),
('prod-C','Mechanical Keyboard','Electronics',80,299.00);
```

### 5. `docker/docker-compose.yml`
```yaml
version: '3.8'

networks:
  datanexus-net:
    driver: bridge

services:

  mysql:
    image: mysql:8.0
    container_name: datanexus-mysql
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: orders_db
    ports:
      - "3306:3306"
    volumes:
      - ./mysql/my.cnf:/etc/mysql/conf.d/my.cnf
      - ./mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - datanexus-net

  postgres:
    image: postgres:15
    container_name: datanexus-postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgrespassword
      POSTGRES_DB: users_db
    ports:
      - "5432:5432"
    volumes:
      - ./postgres/postgresql.conf:/etc/postgresql/postgresql.conf
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    command: postgres -c config_file=/etc/postgresql/postgresql.conf
    networks:
      - datanexus-net

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: datanexus-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - datanexus-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: datanexus-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - datanexus-net

  spark-master:
    image: apache/spark:3.5.0
    container_name: datanexus-spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"
      - "7077:7077"
    networks:
      - datanexus-net

  spark-worker:
    image: apache/spark:3.5.0
    container_name: datanexus-spark-worker
    depends_on:
      - spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    networks:
      - datanexus-net
```

---

## Commands to Execute Step 1

```bash
# Step 1: Go to docker folder
cd DataNexus/docker

# Step 2: Start all 6 containers in background
docker-compose up -d

# Step 3: Check all containers are running
docker-compose ps

# Step 4: Verify MySQL seed data
docker exec -it datanexus-mysql mysql -uroot -prootpassword -e "USE orders_db; SELECT * FROM orders;"

# Step 5: Verify PostgreSQL seed data
docker exec -it datanexus-postgres psql -U postgres -d users_db -c "SELECT * FROM users;"

# Step 6: Open Spark Master Web UI
# Open browser → http://localhost:8080
# You should see 1 Worker registered with 2 Cores and 2 GB Memory
```

---

## What You Should See After Running

- `docker-compose ps` shows 6 containers with status `Up`
- MySQL query returns 3 orders (Mumbai, Bengaluru, Delhi)
- PostgreSQL query returns 3 users (Aarav, Priya, Rohan)
- Spark UI at `http://localhost:8080` shows 1 alive Worker node

---

## Why We Are Setting This Up
- MySQL & PostgreSQL = our fake production databases (like a real e-commerce company's backend)
- Kafka = the messaging system that will carry real-time events in Step 3
- Zookeeper = needed to coordinate Kafka
- Spark = the compute engine that will process millions of records in Steps 4 & 5
- Everything runs locally in Docker so you can develop without a cloud account until Step 2 (Terraform + GCP)
