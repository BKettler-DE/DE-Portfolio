# Data Pipeline Exploration - Docker, Databases & Streaming

A hands-on learning project exploring core data engineering infrastructure through practical, runnable examples. This project demonstrates containerization, database design, real-time streaming, and data quality validation patterns.

## 🎯 Project Overview

This project showcases fundamental data engineering concepts through three progressive layers:

1. **Layer 1: Docker Containerization** - Running databases in isolated containers
2. **Layer 2: Database Fundamentals** - PostgreSQL for batch data, TimescaleDB for time-series
3. **Layer 3: Streaming with Kafka** - Real-time data processing and validation

**What This Demonstrates:**
- Infrastructure as code with Docker Compose
- Batch vs streaming data architectures
- Data quality validation and quarantine patterns
- Producer/consumer messaging patterns
- Time-series data optimization
- Real-world handling of messy, incomplete data

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  DOCKER ENVIRONMENT (Runs Locally on Your Laptop)           │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │  PostgreSQL  │  │ TimescaleDB  │  │  Kafka + ZK     │    │
│  │  (Port 5432) │  │ (Port 5433)  │  │  (Port 9092)    │    │
│  │              │  │              │  │                 │    │
│  │ Batch Data   │  │ Time-Series  │  │ Message Queue   │    │
│  │ 3-Zone Model │  │ Sensor Data  │  │ Event Streaming │    │
│  └──────────────┘  └──────────────┘  └─────────────────┘    │
│         ↑                 ↑                   ↑             │
│         └─────────────────┴───────────────────┘             │
│              Python Scripts & Exploration Tools             │
└─────────────────────────────────────────────────────────────┘
```

### Database Exploration (PostgreSQL)
```
Sample Data (pre-loaded) → Interactive Exploration Scripts
├── Raw products (4 sample records with quality issues)
├── Clean products (3 validated records)
└── Quarantine (for manual validation testing)
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
| **Coordination** | Zookeeper | Kafka cluster management |
| **Language** | Python 3.9+ | Data processing, validation, exploration |
| **DB Connector** | psycopg2 | Python PostgreSQL interface |
| **Kafka Client** | kafka-python | Python Kafka producer/consumer |

## 🚀 Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- Python 3.9 or higher
- 8GB RAM recommended (Docker needs resources)
- ~2GB free disk space

### Installation

```bash
# 1. Clone this repository
git clone https://github.com/BKettler-DE/DE-Portfolio.git
cd data-pipeline-exploration

# 2. Start all services (downloads images first time - may take 5-10 minutes)
docker-compose up -d

# 3. Wait for services to be ready (important!)
echo "Waiting for services to start..."
sleep 45

# 4. Verify all containers are running
docker-compose ps
# Should show: my_postgres, my_timescaledb, my_zookeeper, my_kafka all "Up"

# 5. Set up Python environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 6. Install Python dependencies
pip install -r requirements.txt
```

### Verify Setup

```bash
# Check PostgreSQL has sample data
docker exec -it my_postgres psql -U pipeline_user -d pipeline_db -c "SELECT COUNT(*) FROM raw_products;"
# Expected: 4

# Check TimescaleDB has sample data
docker exec -it my_timescaledb psql -U timeseries_user -d timeseries_db -c "SELECT COUNT(*) FROM sensor_readings;"
# Expected: 1000

# Check Kafka is ready
docker logs my_kafka | grep "started"
# Should see: "[KafkaServer id=1] started"
```

If all checks pass, you're ready! ✅

## 📚 Exploring the Project

### Layer 1 & 2: Databases

#### Explore PostgreSQL (Batch Pipeline)

```bash
python python_scripts/explore_postgres.py
```

**What you'll see:**
- Database tables and their sizes
- Raw messy product data (nulls, weird formats, duplicates)
- Clean validated product data
- Analytics queries (inventory value, price distribution)
- Interactive SQL console

**Key Learning:**
- Three-zone architecture (Raw → Clean → Quarantine)
- Data validation and standardization
- Handling invalid data without losing it

#### Explore TimescaleDB (Time-Series)

```bash
python python_scripts/explore_timescale.py
```

**What you'll see:**
- TimescaleDB hypertables and chunking
- Recent sensor readings (temperature, humidity, pressure)
- Time-series aggregations (5-minute windows)
- Data distribution across time
- Location-based comparisons

**Key Learning:**
- Time-series optimization with hypertables
- Time-bucket aggregations
- Efficient time-range queries

#### Test Data Validation

```bash
python python_scripts/insert_and_validate.py
```

**What you'll see:**
- 4 test cases with different quality issues
- Valid data flowing to clean zone
- Invalid data quarantined with reasons
- Real-world validation logic in action

### Layer 3: Kafka Streaming

#### Explore Kafka Basics

```bash
python python_scripts/explore_kafka.py
```

**What you'll see:**
- Creating topics and partitions
- Sending and receiving messages
- Understanding keys, offsets, and consumer groups
- Viewing the sensor data topic

**Key Learning:**
- Kafka topics and partitions
- Producer/consumer pattern
- Message ordering and delivery guarantees

#### Run the Streaming Pipeline

**Terminal 1: Start the Producer**

```bash
python streaming_pipeline/sensor_simulator.py
```

**Output:**
```
✅ Connected to Kafka broker at localhost:9092
🟢 Generating data...
  ✅ Sent 50 readings...
  🔴 Sending INVALID temperature: -45.2°C
  🟡 Sending DUPLICATE from sensor_002
  ✅ Sent 100 readings...
```

Let this run for 1-2 minutes.

**Terminal 2: Start the Consumer**

```bash
python streaming_pipeline/stream_validator.py
```

**Output:**
```
✅ Connected to Kafka consumer
✅ Connected to TimescaleDB
🟢 Processing messages...
  🔴 Invalid: Temperature out of range: -45.2°C
📊 Processed: 50 | Valid: 47 | Invalid: 2 | Duplicates: 1
```

**Terminal 3: Watch Data Flow**

```bash
# See valid readings count increase
docker exec -it my_timescaledb psql -U timeseries_user -d timeseries_db \
  -c "SELECT COUNT(*) FROM sensor_readings;"

# See invalid readings
docker exec -it my_timescaledb psql -U timeseries_user -d timeseries_db \
  -c "SELECT * FROM sensor_readings_invalid ORDER BY time DESC LIMIT 5;"
```

**Key Learning:**
- Decoupled producer/consumer architecture
- Real-time data validation
- Deduplication strategies
- Handling messy streaming data
- Kafka as a buffer between components

## 📁 Project Structure

```
data-pipeline-exploration/
├── README.md                        # This file
├── docker-compose.yml               # Infrastructure as code (4 services)
├── requirements.txt                 # Python dependencies
│
├── sql/
│   ├── init_postgres.sql           # Batch pipeline schema + sample data
│   └── init_timescale.sql          # Streaming pipeline schema + sample data
│
├── python_scripts/
│   ├── explore_postgres.py         # Interactive PostgreSQL exploration
│   ├── explore_timescale.py        # Interactive TimescaleDB exploration
│   ├── explore_kafka.py            # Kafka fundamentals demo
│   └── insert_and_validate.py      # Data validation demo
│
└── streaming_pipeline/
    ├── sensor_simulator.py         # Kafka producer (IoT data generator)
    └── stream_validator.py         # Kafka consumer (validation + storage)
```

## 🔍 Key Concepts Demonstrated

### 1. Infrastructure as Code
- **Docker Compose**: Define entire stack in one YAML file
- **Reproducibility**: Same environment on any machine
- **Isolation**: Services don't interfere with your system

### 2. Data Quality Engineering
- **Three-Zone Pattern**: Raw → Validation → Clean/Quarantine
- **Schema Validation**: Required fields, data types
- **Range Validation**: Out-of-bounds detection
- **Deduplication**: Handling duplicate records
- **Error Logging**: Tracking why data was rejected

### 3. Batch vs Streaming
| Aspect | Batch (PostgreSQL) | Streaming (Kafka) |
|--------|-------------------|-------------------|
| **Frequency** | Scheduled (hourly/daily) | Continuous real-time |
| **Latency** | Minutes to hours | Seconds |
| **Volume** | Large batches | Individual events |
| **Use Case** | Reports, analytics | Monitoring, alerts |

### 4. Database Selection
- **PostgreSQL**: Relational data with complex queries
- **TimescaleDB**: Time-series data with temporal queries
- **Design Principle**: Use the right tool for the job

### 5. Messaging Patterns
- **Producer/Consumer**: Decoupled data flow
- **Topics**: Organized message channels
- **Partitions**: Parallel processing
- **Consumer Groups**: Load balancing

## 📊 Sample Queries

### PostgreSQL Analytics

```sql
-- Data quality overview
SELECT 
    COUNT(*) as total_raw,
    (SELECT COUNT(*) FROM clean_products) as clean,
    (SELECT COUNT(*) FROM quarantine_products) as quarantined
FROM raw_products;

-- Inventory value by price range
SELECT 
    CASE 
        WHEN price < 20 THEN 'Budget'
        WHEN price < 60 THEN 'Mid-range'
        ELSE 'Premium'
    END as category,
    COUNT(*) as products,
    SUM(price * stock) as total_value
FROM clean_products
GROUP BY category;
```

### TimescaleDB Time-Series

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

-- 5-minute aggregations
SELECT 
    time_bucket('5 minutes', time) as bucket,
    sensor_id,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp
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
```

*Last Updated: January 2026 

💡 **Note**: This is a learning project focused on understanding fundamentals. For production use, additional considerations like security, monitoring, backup strategies, and scalability would be needed.
