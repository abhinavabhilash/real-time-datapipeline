# Real-Time E-Commerce Data Processing Pipeline

An end-to-end, fault-tolerant **real-time streaming analytics pipeline** built with **Python, Apache Kafka (KRaft), Apache Spark Structured Streaming (PySpark), PostgreSQL, Streamlit, and Docker**.

The pipeline simulates a live e-commerce platform (`product_view`, `cart_update`, `order_placed`, `payment_success`, `order_cancelled`), validates and deduplicates incoming events in real time, routes malformed records to a Dead-Letter Queue (DLQ), computes event-time windowed KPIs with watermarking, persists results idempotently to PostgreSQL, and visualizes live business metrics in an auto-refreshing Streamlit dashboard.

---

## System Architecture

```text
Python Data Generator ──► Kafka Producer (Idempotent, Snappy) ──► Apache Kafka (KRaft)
                                                                    ├── ecommerce-events (3 partitions)
                                                                    └── ecommerce-events-dlq (invalid records)
                                                                          │
                                                                          ▼
                                                            Spark Structured Streaming (PySpark 3.5)
                                                            • Explicit StructType Schema
                                                            • Validation & DLQ Routing
                                                            • Watermarked Event-ID Deduplication
                                                            • 1-Min Event-Time Window Aggregations
                                                            • Checkpoint Recovery
                                                                          │
                                                                          ▼
                                                                  PostgreSQL 16
                                                            (Idempotent ON CONFLICT Upserts)
                                                                          │
                                                                          ▼
                                                            Streamlit + Plotly Dashboard
                                                            (KPI Cards, Trends, Product Sales, DLQ)
```

### Technology Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Event Simulation** | Python 3.11, Faker | Generates realistic e-commerce JSON streams plus invalid, duplicate, and late-arriving test events |
| **Message Broker** | Apache Kafka 3.8 (KRaft mode) | Partitioned streaming ingestion without ZooKeeper |
| **Stream Processing** | Apache Spark 3.5.3 (PySpark) | Schema enforcement, validation, deduplication, watermarking, and event-time window aggregations |
| **Database** | PostgreSQL 16 + `psycopg2` | ACID storage with idempotent `ON CONFLICT` inserts/upserts |
| **Dashboard** | Streamlit + Plotly | Interactive live KPI dashboard with auto-refresh |
| **Infrastructure** | Docker & Docker Compose | Containerized Kafka, Kafka UI (`localhost:8080`), and PostgreSQL (`localhost:5432`) |
| **Testing** | Pytest | Unit tests for event generation, validation, deduplication, and window aggregations |

---

## Project Structure

```text
realtime-data-pipeline/
├── config/
│   ├── __init__.py
│   └── settings.py             # Typed dataclass settings loaded from .env
├── producer/
│   ├── __init__.py
│   ├── data_generator.py       # Realistic e-commerce event generator + chaos faults
│   └── kafka_producer.py       # Idempotent confluent-kafka producer with callbacks
├── kafka/
│   ├── __init__.py
│   ├── topic_config.py         # AdminClient topic creation (ecommerce-events & DLQ)
│   └── consumer.py             # Debug Kafka consumer with manual offset commits
├── spark/
│   ├── __init__.py
│   ├── schemas.py              # Explicit PySpark StructType schema
│   ├── transformations.py      # Parsing, validation, DLQ split, watermark dedup
│   ├── aggregations.py         # Event-time tumbling window KPIs & product metrics
│   └── spark_consumer.py       # Multi-sink Structured Streaming runner + checkpoints
├── database/
│   ├── __init__.py
│   ├── schema.sql              # PostgreSQL tables & indexes
│   ├── db_connection.py        # Context-managed connections & schema initializer
│   └── db_operations.py        # Batched execute_values inserts & ON CONFLICT upserts
├── dashboard/
│   ├── __init__.py
│   └── app.py                  # Streamlit + Plotly live analytics dashboard
├── tests/
│   ├── __init__.py
│   ├── test_producer.py        # Generator unit tests
│   └── test_transformations.py # PySpark transformation & aggregation tests
├── checkpoints/                # Spark Structured Streaming checkpoints (gitignored)
├── data/                       # Local scratch data directory (gitignored)
├── docker-compose.yml          # Kafka (KRaft), Kafka UI, PostgreSQL
├── requirements.txt            # Pinned Python dependencies
├── .env.example                # Environment variable template
├── .gitignore
└── README.md
```

---

## Business Logic & Engineering Guarantees

- **Zero Double-Counting Revenue**: Recognized revenue (`recognized_revenue`) is counted **only** on `payment_success` events where `payment_status == 'SUCCESS'`. `order_placed` (`PENDING`), `product_view`, `cart_update`, and `order_cancelled` contribute `0.00` to revenue.
- **Dead-Letter Queue (DLQ)**: Corrupted JSON, missing `user_id`/`product_id`, invalid timestamps, unknown event types, and negative amounts are tagged with a `validation_error` and routed to both the `ecommerce-events-dlq` Kafka topic and `dead_letter_events` PostgreSQL table.
- **Duplicate & Late-Event Handling**: Spark uses `withWatermark` + `dropDuplicatesWithinWatermark(["event_id"])`, backed by `PRIMARY KEY (event_id) ... ON CONFLICT DO NOTHING` in PostgreSQL for end-to-end idempotency.
- **Fault Recovery**: Every Spark streaming query persists offsets and state under `./checkpoints/ecommerce_stream/`.

---

## Prerequisites & Setup

### Required Software
- **Docker & Docker Compose** (Docker Desktop on macOS/Windows, or Docker Engine on Linux)
- **Python 3.11**
- **Java JDK 17** (required by PySpark 3.5.3)

### 1. Clone & Configure Environment

```bash
git clone <your-repo-url>
cd realtime-data-pipeline
cp .env.example .env
```

### 2. Create Virtual Environment & Install Dependencies

**macOS / Linux:**
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Start Infrastructure (Kafka, Kafka UI, PostgreSQL)

```bash
docker compose up -d
docker compose ps
```

- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)
- **PostgreSQL**: `localhost:5432` (`ecommerce_analytics`)

### 4. Initialize Kafka Topics & PostgreSQL Schema

```bash
python -m kafka.topic_config
python -m database.db_connection
```

---

## Running the Live Pipeline

Open **3 terminal windows** (with `.venv` activated in each):

### Terminal 1 — Start Continuous Kafka Producer
```bash
source .venv/bin/activate
python -m producer.kafka_producer --count 0 --interval 0.5
```

### Terminal 2 — Start Spark Structured Streaming Consumer
```bash
source .venv/bin/activate
python -m spark.spark_consumer --starting-offsets earliest --trigger-interval "5 seconds"
```
*(Use `--once` instead of continuous mode to process all currently buffered Kafka messages and exit.)*

### Terminal 3 — Launch Streamlit Live Dashboard
```bash
source .venv/bin/activate
streamlit run dashboard/app.py
```
Open **[http://localhost:8501](http://localhost:8501)** in your browser.

---

## Running Tests

```bash
source .venv/bin/activate
pytest -v
```

---

## Screenshots & Demo

> Add screenshots of your running pipeline before publishing to GitHub:
> - **Kafka UI (`http://localhost:8080`)**: Topics `ecommerce-events` and `ecommerce-events-dlq`
> - **Streamlit Dashboard (`http://localhost:8501`)**: KPI cards, revenue trends, product-wise sales, and DLQ tab

---

## Troubleshooting

| Issue | Resolution |
| :--- | :--- |
| `Unable to locate a Java Runtime` | Install JDK 17 and set `JAVA_HOME` (e.g. `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`). |
| `Failed to connect to broker at localhost:9092` | Ensure Docker Desktop is running and `docker compose ps` shows `ecommerce-kafka` as `healthy`. |
| `Permission denied` or stale Spark state after code edits | Delete `./checkpoints/ecommerce_stream/*` and restart `spark.spark_consumer`. |
| Streamlit shows empty charts | Confirm both `producer.kafka_producer` and `spark.spark_consumer` are running and writing rows (`python -m database.db_connection`). |
# real-time-datapipeline
