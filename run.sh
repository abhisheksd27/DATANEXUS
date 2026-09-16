#!/bin/bash
# =============================================================================
# DataNexus — Master Run Script
# Runs the complete end-to-end data pipeline in one command.
# Usage:  bash run.sh [step]
#   steps: all | docker | seed | kafka | etl | api | stop
# =============================================================================

set -e  # Exit immediately on any error

PROJECT_ROOT="/Users/abhishekshankar/PROJECTS/DataNexus"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log()   { echo -e "${CYAN}[DataNexus]${NC} $1"; }
ok()    { echo -e "${GREEN}[✅ DONE ]${NC} $1"; }
warn()  { echo -e "${YELLOW}[⚠️  WARN ]${NC} $1"; }
err()   { echo -e "${RED}[❌ ERROR]${NC} $1"; }
header(){ echo -e "\n${BLUE}══════════════════════════════════════════${NC}"; echo -e "${BLUE}  $1${NC}"; echo -e "${BLUE}══════════════════════════════════════════${NC}"; }

# =============================================================================
# STEP 1: Start Docker containers
# =============================================================================
step_docker() {
    header "STEP 1: Starting Docker Infrastructure"

    if ! docker info > /dev/null 2>&1; then
        err "Docker Desktop is not running!"
        log "Please open Docker Desktop and wait 20 seconds, then re-run this script."
        exit 1
    fi

    log "Bringing up all containers (MySQL, Postgres, Kafka, Debezium, Spark)..."
    cd "$DOCKER_DIR"
    docker-compose down --remove-orphans 2>/dev/null || true
    docker-compose up -d

    log "Waiting 25 seconds for all services to initialise..."
    sleep 25

    log "Container status:"
    docker-compose ps
    ok "Docker infrastructure is up!"
}

# =============================================================================
# STEP 2: Seed databases
# =============================================================================
step_seed() {
    header "STEP 2: Seeding MySQL & PostgreSQL"

    log "Seeding MySQL with 50 e-commerce orders..."
    python "$PROJECT_ROOT/scripts/seed_mysql.py"
    ok "MySQL seeded!"

    log "Seeding PostgreSQL with 30 users & inventory..."
    python "$PROJECT_ROOT/scripts/seed_postgres.py"
    ok "PostgreSQL seeded!"
}

# =============================================================================
# STEP 3: Register Debezium CDC connectors + run Kafka producers & API ingestors
# =============================================================================
step_kafka() {
    header "STEP 3: Registering CDC Connectors & Ingesting Data"

    log "Waiting for Debezium Connect to be ready on port 8083..."
    for i in {1..15}; do
        if curl -s http://localhost:8083/ > /dev/null 2>&1; then
            ok "Debezium Connect is ready!"
            break
        fi
        echo "  Attempt $i/15 — waiting 3s..."
        sleep 3
    done

    log "Registering MySQL CDC connector..."
    RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" \
        http://localhost:8083/connectors/ \
        -d @"$PROJECT_ROOT/ingestion/debezium/mysql-connector.json")
    if [ "$RESP" == "201" ] || [ "$RESP" == "409" ]; then
        ok "MySQL CDC connector registered (HTTP $RESP)"
    else
        warn "MySQL CDC connector response: HTTP $RESP (may already exist)"
    fi

    log "Registering PostgreSQL CDC connector..."
    RESP=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" \
        http://localhost:8083/connectors/ \
        -d @"$PROJECT_ROOT/ingestion/debezium/postgres-connector.json")
    if [ "$RESP" == "201" ] || [ "$RESP" == "409" ]; then
        ok "PostgreSQL CDC connector registered (HTTP $RESP)"
    else
        warn "PostgreSQL CDC connector response: HTTP $RESP (may already exist)"
    fi

    log "Fetching live weather data from Open-Meteo API..."
    python "$PROJECT_ROOT/ingestion/api_ingestor/weather_ingestor.py"
    ok "Weather data ingested into Bronze lake!"

    log "Fetching live FX exchange rates..."
    python "$PROJECT_ROOT/ingestion/api_ingestor/exchange_rate_ingestor.py"
    ok "Exchange rates ingested into Bronze lake!"

    log "Publishing 50 clickstream events to Kafka..."
    python "$PROJECT_ROOT/ingestion/kafka_producers/clickstream_producer.py"
    ok "Clickstream events published!"

    log "Publishing 20 order transaction events to Kafka..."
    python "$PROJECT_ROOT/ingestion/kafka_producers/order_events_producer.py"
    ok "Order events published!"
}

# =============================================================================
# STEP 4: Run PySpark Medallion ETL (Bronze → Silver → Gold)
# =============================================================================
step_etl() {
    header "STEP 4: Running PySpark Medallion ETL"

    log "Bronze → Silver: orders_etl.py"
    python "$PROJECT_ROOT/processing/batch/bronze_to_silver/orders_etl.py"
    ok "Orders Silver layer complete!"

    log "Bronze → Silver: users_etl.py"
    python "$PROJECT_ROOT/processing/batch/bronze_to_silver/users_etl.py"
    ok "Users Silver layer complete!"

    log "Bronze → Silver: inventory_etl.py"
    python "$PROJECT_ROOT/processing/batch/bronze_to_silver/inventory_etl.py"
    ok "Inventory Silver layer complete!"

    log "Silver → Gold: daily_revenue.py"
    python "$PROJECT_ROOT/processing/batch/silver_to_gold/daily_revenue.py"
    ok "Daily Revenue Gold layer complete!"

    log "Silver → Gold: user_behavior.py"
    python "$PROJECT_ROOT/processing/batch/silver_to_gold/user_behavior.py"
    ok "User Behavior Gold layer complete!"
}

# =============================================================================
# STEP 5: Launch FastAPI Analytics Serving API
# =============================================================================
step_api() {
    header "STEP 5: Launching FastAPI Analytics API"

    log "Starting DataNexus Analytics API on http://localhost:8000 ..."
    log "  📊 Swagger Docs  → http://localhost:8000/docs"
    log "  💰 Daily Revenue → http://localhost:8000/api/v1/revenue/daily"
    log "  📦 Trending Prods→ http://localhost:8000/api/v1/products/trending"
    log "  💚 Health Check  → http://localhost:8000/health"
    log "(Press Ctrl+C to stop the API server)"

    cd "$PROJECT_ROOT/serving/api"
    uvicorn main:app --reload --port 8000
}

# =============================================================================
# STOP: Tear down Docker
# =============================================================================
step_stop() {
    header "Stopping Docker Infrastructure"
    cd "$DOCKER_DIR"
    docker-compose down
    ok "All containers stopped."
}

# =============================================================================
# FULL RUN: All steps in order (except API which is interactive)
# =============================================================================
step_all() {
    header "DataNexus — Full End-to-End Pipeline Run"
    step_docker
    step_seed
    step_kafka
    step_etl
    header "🎉 DataNexus Pipeline Complete!"
    echo ""
    ok "All layers finished successfully:"
    echo "  ✅ Docker:  MySQL, PostgreSQL, Kafka, Debezium, Spark are running"
    echo "  ✅ Seed:    50 orders + 30 users + 10 products seeded into OLTP databases"
    echo "  ✅ Ingest:  CDC + Kafka events + Weather + FX rates landed in Bronze lake"
    echo "  ✅ ETL:     Bronze → Silver → Gold medallion transformations complete"
    echo ""
    log "To start the FastAPI analytics server, run:"
    echo "  bash run.sh api"
    echo ""
    log "To stop all Docker containers, run:"
    echo "  bash run.sh stop"
}

# =============================================================================
# Entrypoint — parse argument
# =============================================================================
STEP="${1:-all}"

case "$STEP" in
    all)    step_all   ;;
    docker) step_docker ;;
    seed)   step_seed  ;;
    kafka)  step_kafka  ;;
    etl)    step_etl   ;;
    api)    step_api   ;;
    stop)   step_stop  ;;
    *)
        echo ""
        echo "Usage: bash run.sh [step]"
        echo ""
        echo "  all     — Run the full pipeline end-to-end (default)"
        echo "  docker  — Start Docker containers only"
        echo "  seed    — Seed MySQL & PostgreSQL databases only"
        echo "  kafka   — Register CDC connectors + ingest data"
        echo "  etl     — Run all PySpark Bronze→Silver→Gold ETL jobs"
        echo "  api     — Launch FastAPI analytics serving API"
        echo "  stop    — Stop all Docker containers"
        echo ""
        ;;
esac
