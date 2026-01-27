# Data Pipeline Exploration - Complete Data Engineering Stack

A comprehensive hands-on learning project demonstrating modern data engineering infrastructure through practical, runnable examples. This project showcases containerization, database design, real-time streaming, batch orchestration, and data quality validation patterns.

## 🎯 Project Overview

This project demonstrates fundamental data engineering concepts through four progressive layers:

1. **Layer 1: Docker Containerization** - Running databases and services in isolated containers
2. **Layer 2: Database Fundamentals** - PostgreSQL for batch data, TimescaleDB for time-series
3. **Layer 3: Streaming with Kafka** - Real-time data processing and validation
4. **Layer 4: Batch Orchestration with Airflow** - Scheduled workflows and pipeline automation

**What This Demonstrates:**
- Infrastructure as code with Docker Compose
- Batch and streaming data architectures working together
- Data quality validation and quarantine patterns
- Producer/consumer messaging patterns
- Workflow orchestration and scheduling
- Time-series data optimization
- Real-world handling of messy, incomplete data

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  DOCKER ENVIRONMENT (7 Services Running Locally)                 │
│                                                                  │
│  ┌──────────┐  ┌───────────┐  ┌───────┐  ┌────────────────┐      │
│  │PostgreSQL│  │TimescaleDB│  │ Kafka │  │    Airflow     │      │
│  │(Port 5432│  │(Port 5433)│  │ +ZK   │  │  (Port 8080)   │      │
│  │          │  │           │  │       │  │ Webserver +    │      │
│  │Batch Data│  │Time-Series│  │Message│  │  Scheduler     │      │
│  │3-Zone    │  │Sensor Data│  │ Queue │  │                │      │
│  └──────────┘  └───────────┘  └───────┘  └────────────────┘      │
│       ↑              ↑             ↑              ↑              │
│       └──────────────┴─────────────┴──────────────┘              │
│         Python Scripts, DAGs, and Pipeline Code                  │
└──────────────────────────────────────────────────────────────────┘
```

### Batch Pipeline (Airflow → PostgreSQL) ✅ Fully Implemented
```
Scheduler (Airflow) triggers at 2 AM daily
    ↓
Scraper generates product data (simulates web scraping)
    ↓
Load to Raw Zone (PostgreSQL - unvalidated)
    ↓
Validation Layer (Python - quality checks)
    ↓ (splits)
    ├─→ Clean Zone (valid products)
    └─→ Quarantine (invalid with reasons)
    ↓
Generate quality report

Demonstrates: Orchestration, scheduled workflows, data quality pipeline
```

### Streaming Pipeline (Kafka → TimescaleDB) ✅ Fully Implemented
```
Sensor Simulator → Kafka Topic → Stream Validator → TimescaleDB
   (Producer)    sensor_readings_raw   (Consumer)    (Valid/Invalid Tables)
                                                      
Demonstrates: Real-time validation, deduplication, data quality
```

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Containerization** | Docker Compose | Run all services locally without complex installation |
| **Batch Database** | PostgreSQL 15 | Structured product data with ACID guarantees |
| **Time-Series DB** | TimescaleDB | IoT sensor data optimized for time-based queries |
| **Message Queue** | Apache Kafka | Decoupled real-time data streaming |
| **Orchestration** | Apache Airflow 2.8 | Workflow scheduling and monitoring |
| **Coordination** | Zookeeper | Kafka cluster management |
| **Language** | Python 3.11 | Data processing, validation, exploration |
| **DB Connector** | psycopg2 | Python PostgreSQL interface |
| **Kafka Client** | kafka-python | Python Kafka producer/consumer |

## 🚀 Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- Python 3.9 or higher
- 8GB RAM minimum (for all 7 containers)
- ~5GB free disk space

### Installation

```bash
# 1. Clone this repository
git clone https://github.com/BKettler-DE/DE-Portfolio.git
cd data-pipeline-exploration

# 2. Create required directories
mkdir -p dags logs plugins batch_pipeline

# 3. Start all services (first time takes 5-10 minutes)
docker-compose up -d

# 4. Wait for services to initialize (important!)
# Windows:
timeout /t 90
# Mac/Linux:
sleep 90

# 5. Verify all containers are running
docker-compose ps
# Should show 7 containers: postgres, timescaledb, zookeeper, kafka, 
# airflow-init (exited), airflow-webserver, airflow-scheduler

# 6. Set up Python environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 7. Install Python dependencies
pip install -r requirements.txt
```

### Verify Setup

```bash
# Check PostgreSQL
docker exec -it my_postgres psql -U pipeline_user -d pipeline_db -c "SELECT COUNT(*) FROM raw_products;"
# Expected: 4

# Check TimescaleDB
docker exec -it my_timescaledb psql -U timeseries_user -d timeseries_db -c "SELECT COUNT(*) FROM sensor_readings;"
# Expected: 1000

# Check Kafka
docker logs my_kafka | grep "started"
# Should see: "[KafkaServer id=1] started"

# Check Airflow (most important!)
docker logs airflow_webserver | grep "Uvicorn running"
# Should see: "Uvicorn running on http://0.0.0.0:8080"
```

If all checks pass, you're ready! ✅

## 📚 Exploring the Project

### Layer 1 & 2: Database Exploration

#### Explore PostgreSQL

```bash
python python_scripts/explore_postgres.py
```

**What you'll see:**
- Database tables and their sizes
- Raw product data (with quality issues)
- Clean validated product data
- Analytics queries (inventory value, price distribution)
- Interactive SQL console

#### Explore TimescaleDB

```bash
python python_scripts/explore_timescale.py
```

**What you'll see:**
- TimescaleDB hypertables and chunking
- Recent sensor readings
- Time-series aggregations (5-minute windows)
- Data distribution across time

### Layer 3: Kafka Streaming Pipeline

#### Run the Streaming Pipeline

**Terminal 1: Start Producer**
```bash
python streaming_pipeline/sensor_simulator.py
```

**Terminal 2: Start Consumer**
```bash
python streaming_pipeline/stream_validator.py
```

**Terminal 3: Watch Results**
```bash
docker exec -it my_timescaledb psql -U timeseries_user -d timeseries_db \
  -c "SELECT COUNT(*) FROM sensor_readings;"
# Count increases in real-time!
```

### Layer 4: Batch Pipeline with Airflow ✨ NEW

#### Access Airflow Web UI

1. Open browser: **http://localhost:8080**
2. Login:
   - Username: `admin`
   - Password: `admin`
3. You'll see the Airflow dashboard

#### Configure Database Connection (First Time Only)

1. Go to **Admin** → **Connections**
2. Click **+** to add connection
3. Fill in:
   - **Connection Id**: `postgres_default`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Database**: `pipeline_db`
   - **Login**: `pipeline_user`
   - **Password**: `pipeline_pass`
   - **Port**: `5432`
4. Click **Save**

#### Run the Batch Pipeline

**Method 1: Manual Trigger (Immediate)**
1. In Airflow UI, find DAG: `product_batch_pipeline`
2. Toggle it **ON** (if paused)
3. Click **▶️ Play** button → **Trigger DAG**
4. Watch tasks execute in Graph view

**Method 2: Scheduled Runs**
- DAG runs automatically daily at 2 AM
- View schedule in Airflow UI

**Pipeline Flow:**
```
extract_products (scrape 45 products)
    ↓
load_to_raw_zone (save to PostgreSQL)
    ↓
validate_products (apply quality checks)
    ↓  ↓
    │  └→ load_to_quarantine (invalid data)
    ↓
load_to_clean_zone (valid data)
    ↓
mark_processed (update flags)
    ↓
generate_report (print statistics)
```

#### View Results

```bash
# Check clean products loaded
docker exec -it my_postgres psql -U pipeline_user -d pipeline_db -c "
SELECT COUNT(*) as total, 
       MAX(loaded_at) as last_load 
FROM clean_products 
WHERE loaded_at > NOW() - INTERVAL '1 hour';"

# Check quarantined products with reasons
docker exec -it my_postgres psql -U pipeline_user -d pipeline_db -c "
SELECT issues, COUNT(*) as count 
FROM quarantine_products 
WHERE quarantined_at > NOW() - INTERVAL '1 hour'
GROUP BY issues 
ORDER BY count DESC;"

# Check raw products processing status
docker exec -it my_postgres psql -U pipeline_user -d pipeline_db -c "
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN processed THEN 1 ELSE 0 END) as processed,
    SUM(CASE WHEN NOT processed THEN 1 ELSE 0 END) as pending
FROM raw_products;"
```

## 📁 Project Structure

```
data-pipeline-exploration/
├── README.md                        # This file
├── docker-compose.yml               # Infrastructure as code (7 services)
├── requirements.txt                 # Python dependencies
│
├── sql/
│   ├── init_postgres.sql           # Batch pipeline schema + sample data
│   ├── init_timescale.sql          # Streaming pipeline schema + sample data
│   └── init_airflow_db.sql         # Airflow database
│
├── python_scripts/
│   ├── explore_postgres.py         # Interactive PostgreSQL exploration
│   ├── explore_timescale.py        # Interactive TimescaleDB exploration
│   ├── explore_kafka.py            # Kafka fundamentals demo
│   └── insert_and_validate.py      # Data validation demo
│
├── streaming_pipeline/
│   ├── sensor_simulator.py         # Kafka producer (IoT data generator)
│   └── stream_validator.py         # Kafka consumer (validation + storage)
│
├── batch_pipeline/                  # ← NEW
│   ├── scraper.py                  # Product data scraper
│   └── validator.py                # Batch validation logic
│
├── dags/                            # ← NEW
│   └── product_batch_pipeline.py   # Airflow DAG
│
├── logs/                            # Airflow logs (auto-created)
└── plugins/                         # Airflow plugins (empty)
```

## 🔍 Key Concepts Demonstrated

### 1. Workflow Orchestration (NEW!)
- **Airflow DAGs**: Define task dependencies as code
- **Scheduling**: Automatic daily runs at specified time
- **Monitoring**: Visual task execution tracking
- **Retries**: Automatic retry logic on failures
- **Logging**: Detailed logs for each task
- **XCom**: Passing data between tasks

### 2. Data Quality Engineering
- **Three-Zone Pattern**: Raw → Validation → Clean/Quarantine
- **Comprehensive Validation**: 
  - Schema validation (required fields, data types)
  - Range validation (price > 0, stock >= 0)
  - Format standardization (price: "$19.99" → 19.99)
  - Deduplication (same product_id)
- **Error Tracking**: Quarantine with specific rejection reasons
- **Quality Metrics**: Validation pass/fail rates, issue breakdown

### 3. Batch vs Streaming Architectures

**What's Actually Implemented:**

| Component | Status | What You Can Do |
|-----------|--------|-----------------|
| **Batch Pipeline** | ✅ Working | Trigger DAG in Airflow, watch execution, query results |
| **Streaming Pipeline** | ✅ Working | Run producer/consumer, see real-time validation |
| **Database Exploration** | ✅ Working | Query sample data, run analytics |

**Conceptual Comparison:**
| Aspect | Batch (Airflow) | Streaming (Kafka) |
|--------|----------------|-------------------|
| **Frequency** | Scheduled (daily at 2 AM) ✅ | Continuous real-time ✅ |
| **Latency** | Minutes to hours | Seconds ✅ |
| **Volume** | 45 products/run ✅ | 10 events/second ✅ |
| **Use Case** | Inventory updates ✅ | Sensor monitoring ✅ |
| **Orchestration** | Airflow DAG ✅ | Event-driven ✅ |

### 4. Production Patterns
- **Idempotent Operations**: Safe to re-run pipelines
- **Error Quarantine**: Preserve invalid data for analysis
- **Monitoring**: Airflow UI + database metrics
- **Raw Data Preservation**: Enable reprocessing with updated logic
- **Graceful Degradation**: Continue processing valid data when errors occur

### 5. Database Selection
- **PostgreSQL**: Batch data with complex relationships
- **TimescaleDB**: Time-series sensor data
- **Design Principle**: Use the right tool for the job

## 📊 Sample Queries

### Batch Pipeline Analytics

```sql
-- Validation quality metrics from latest run
SELECT 
    COUNT(*) as total_raw,
    (SELECT COUNT(*) FROM clean_products WHERE loaded_at > NOW() - INTERVAL '1 hour') as clean,
    (SELECT COUNT(*) FROM quarantine_products WHERE quarantined_at > NOW() - INTERVAL '1 hour') as quarantined
FROM raw_products
WHERE batch_id LIKE 'manual%'
ORDER BY ingestion_time DESC
LIMIT 50;

-- Top rejection reasons
SELECT 
    issues,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM quarantine_products
WHERE quarantined_at > NOW() - INTERVAL '24 hours'
GROUP BY issues
ORDER BY count DESC;

-- Inventory value by category
SELECT 
    category,
    COUNT(*) as products,
    SUM(price * stock) as total_value,
    AVG(price) as avg_price
FROM clean_products
GROUP BY category
ORDER BY total_value DESC;
```

### Streaming Analytics

```sql
-- Average sensor readings (last hour)
SELECT 
    sensor_id,
    COUNT(*) as readings,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity
FROM sensor_readings
WHERE time > NOW() - INTERVAL '1 hour'
GROUP BY sensor_id;

-- 5-minute time-bucketed aggregations
SELECT 
    time_bucket('5 minutes', time) as bucket,
    sensor_id,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp,
    COUNT(*) as reading_count
FROM sensor_readings
WHERE time > NOW() - INTERVAL '30 minutes'
GROUP BY bucket, sensor_id
ORDER BY bucket DESC;
```

## 🛑 Shutdown & Cleanup

```bash
# Stop all services (preserves data)
docker-compose stop

# Stop and remove containers (data still preserved in volumes)
docker-compose down

# Nuclear option: Remove everything including data
docker-compose down -v

# Remove just Airflow logs (if they get large)
rm -rf logs/*
```

## 🎓 What I Learned

### Docker & Infrastructure ✅
- Multi-container orchestration (7 services)
- Volume management for data persistence
- Container networking and service discovery
- Health checks and dependency management
- Troubleshooting container startup issues

### Workflow Orchestration ✅ (NEW!)
- **Apache Airflow fundamentals**
- **DAG design and task dependencies**
- **XCom for passing data between tasks**
- **Scheduling with cron expressions**
- **Monitoring and logging in Airflow UI**
- **PostgresHook for database connections**
- **Error handling and retries**

### Database Design ✅
- Schema design for different workloads
- When to use PostgreSQL vs TimescaleDB
- Time-series optimization with hypertables
- Indexing for query performance
- JSONB for flexible schema storage

### Streaming Architecture ✅
- Producer/consumer decoupling with Kafka
- Message queues as buffers
- Real-time validation in streams
- Deduplication strategies
- Handling late-arriving data
- Consumer groups and offset management

### Data Engineering Patterns ✅
- **Three-zone architecture** (Raw → Clean → Quarantine)
- **Data validation at multiple stages** (batch and streaming)
- **Error quarantine** (preserve, don't drop invalid data)
- **Idempotent operations** (safe to re-run)
- **Data quality metrics** (validation pass rates, issue tracking)

### Python for Data Engineering ✅
- Database connectivity with psycopg2
- Kafka integration with kafka-python
- **Airflow task creation with PythonOperator**
- **XCom push/pull for task communication**
- Programmatic data validation
- Error handling and logging

*Last Updated: January 2026 

💡 **Note**: This is a learning project focused on understanding fundamentals. For production use, additional considerations like security, monitoring, backup strategies, and scalability would be needed.
