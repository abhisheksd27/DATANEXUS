# 📍 Step 1: Setting Up the Local Development Environment

---

## 🎯 1. What Step 1 Accomplishes
Step 1 establishes the local containerized environment using **Docker** and **Docker Compose**. Instead of installing multiple heavy databases, message brokers, and compute engines directly on your host operating system, Docker runs them in isolated, reproducible containers.

---

## 🧠 2. Deep Explanation of Technologies in Step 1

### 1. 🐳 Docker & Docker Compose
* **What it is:** Docker packages applications and their dependencies into self-contained containers. `docker-compose` allows multi-container setup via a single YAML file (`docker-compose.yml`).
* **Why we use it:** Ensures zero environment mismatch between developer machines and production servers.
* **What it is doing:** Spins up and networks MySQL, PostgreSQL, Zookeeper, Kafka, Spark Master, and Spark Worker on a shared virtual bridge network (`datanexus-net`).

### 2. 🐬 MySQL (Port 3306)
* **What it is:** A relational OLTP database.
* **Role in Step 1:** Simulates the **Orders & Payments Microservice**, storing transactional tables (`orders`, `order_items`).
* **CDC Configuration:** Configured with Binary Logging (`log_bin = mysql-bin`, `binlog_format = ROW`) so **Debezium CDC** can read database row modifications in real time without locking tables.

### 3. 🐘 PostgreSQL (Port 5432)
* **What it is:** An enterprise object-relational database.
* **Role in Step 1:** Simulates the **User & Inventory Microservice**, storing customer profiles (`users`) and stock levels (`inventory`).
* **CDC Configuration:** Configured with `wal_level = logical` to enable PostgreSQL's Logical Replication stream for Debezium CDC.

### 4. 🐘 Zookeeper (Port 2181)
* **What it is:** A centralized cluster coordination service.
* **Role in Step 1:** Manages Kafka cluster state, topic partition assignments, and leader election.

### 5. 📨 Apache Kafka (Port 9092)
* **What it is:** A distributed event-streaming platform.
* **Role in Step 1:** Acts as the high-throughput message broker receiving real-time events from CDC connectors and clickstream producers.

### 6. ⚡ Apache Spark Master & Worker (Ports 8080, 7077)
* **What it is:** A distributed in-memory compute engine.
* **Role in Step 1:** 
  * **Spark Master (Port 8080 UI):** Coordinates jobs, schedules stages, and allocates cluster resources.
  * **Spark Worker (Port 7077 RPC):** Executes PySpark tasks across 2 allocated CPU cores and 2GB RAM.

---

## 🗂️ 3. Directory Structure to Prepare

Create the following files inside your project directory:

```
DataNexus/
└── docker/
    ├── docker-compose.yml
    ├── mysql/
    │   ├── my.cnf
    │   └── init.sql
    └── postgres/
        ├── postgresql.conf
        └── init.sql
```

---

## 📝 4. Complete Configuration Files with Code Explanations

### File 1: `docker/mysql/my.cnf`
*This configuration enables MySQL's binary log required by Debezium CDC.*

```ini
[mysqld]
# Unique server ID required for replication
server-id        = 223344

# Enable binary log output file prefix
log_bin          = mysql-bin

# Row-based logging captures exact row changes (required for CDC)
binlog_format    = ROW

# Captures complete row state before and after change
binlog_row_image = FULL

# Retain binary logs for 7 days
expire_logs_days = 7
```

---

### File 2: `docker/mysql/init.sql`
*Initializes `orders_db` and seeds initial transactional records.*

```sql
CREATE DATABASE IF NOT EXISTS orders_db;
USE orders_db;

-- Orders Table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    shipping_city VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items Table
CREATE TABLE IF NOT EXISTS order_items (
    item_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(36) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Seed Data
INSERT INTO orders (order_id, user_id, total_amount, status, shipping_city) VALUES
('ord-101', 'usr-001', 149.99, 'COMPLETED', 'Mumbai'),
('ord-102', 'usr-002', 89.50, 'PENDING', 'Bengaluru'),
('ord-103', 'usr-003', 299.00, 'COMPLETED', 'Delhi');

INSERT INTO order_items (item_id, order_id, product_id, quantity, price) VALUES
('itm-01', 'ord-101', 'prod-A', 1, 149.99),
('itm-02', 'ord-102', 'prod-B', 2, 44.75),
('itm-03', 'ord-103', 'prod-C', 1, 299.00);
```

---

### File 3: `docker/postgres/postgresql.conf`
*Enables logical replication for Debezium on PostgreSQL.*

```ini
# Logical replication level allows CDC tools to stream WAL logs
wal_level = logical
max_wal_senders = 4
max_replication_slots = 4
```

---

### File 4: `docker/postgres/init.sql`
*Initializes `users_db` and seeds initial user and inventory records.*

```sql
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(36) PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    city VARCHAR(50) NOT NULL,
    signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(36) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    stock_quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL
);

INSERT INTO users (user_id, full_name, email, city) VALUES
('usr-001', 'Aarav Sharma', 'aarav@example.com', 'Mumbai'),
('usr-002', 'Priya Patel', 'priya@example.com', 'Bengaluru'),
('usr-003', 'Rohan Verma', 'rohan@example.com', 'Delhi');

INSERT INTO inventory (product_id, product_name, category, stock_quantity, unit_price) VALUES
('prod-A', 'Wireless Headphones', 'Electronics', 150, 149.99),
('prod-B', 'Ergonomic Mouse', 'Electronics', 300, 44.75),
('prod-C', 'Mechanical Keyboard', 'Electronics', 80, 299.00);
```

---

### File 5: `docker/docker-compose.yml`
*Master orchestration file linking all services together.*

```yaml
version: '3.8'

networks:
  datanexus-net:
    driver: bridge

services:
  # 1. MySQL (Orders Service DB)
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

  # 2. PostgreSQL (User & Inventory Service DB)
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

  # 3. Zookeeper (Kafka Coordination)
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: datanexus-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - datanexus-net

  # 4. Apache Kafka (Message Broker)
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

  # 5. Apache Spark Master
  spark-master:
    image: bitnami/spark:3.5.0
    container_name: datanexus-spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
    ports:
      - "8080:8080"
      - "7077:7077"
    networks:
      - datanexus-net

  # 6. Apache Spark Worker
  spark-worker:
    image: bitnami/spark:3.5.0
    container_name: datanexus-spark-worker
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
    networks:
      - datanexus-net
```

---

## 💻 5. Execution & Verification Commands

1. **Navigate to the docker directory:**
   ```bash
   cd /Users/abhishekshankar/PROJECTS/DataNexus/docker
   ```

2. **Launch all services in background:**
   ```bash
   docker-compose up -d
   ```

3. **Check container status:**
   ```bash
   docker-compose ps
   ```

4. **Verify database seed data:**
   * **MySQL:** `docker exec -it datanexus-mysql mysql -uroot -prootpassword -e "USE orders_db; SELECT * FROM orders;"`
   * **PostgreSQL:** `docker exec -it datanexus-postgres psql -U postgres -d users_db -c "SELECT * FROM users;"`

5. **Verify Spark Web UI:**
   Access `http://localhost:8080` in your web browser. You will see the Spark Master interface with 1 active Worker node.
