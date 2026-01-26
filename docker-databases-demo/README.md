# Data Pipeline Exploration - Docker & Database Fundamentals

A hands-on learning project exploring core data engineering infrastructure: Docker containerization, PostgreSQL for batch processing, and TimescaleDB for time-series data.

## 🎯 Project Overview

This project demonstrates fundamental data engineering concepts through practical, runnable examples. It focuses on understanding how modern data pipelines use containers and databases to handle different types of data workloads.

**What This Project Covers:**
- Docker containerization for data infrastructure
- PostgreSQL database fundamentals and data validation
- TimescaleDB for time-series sensor data
- Data quality patterns (Raw → Clean → Quarantine zones)
- Hands-on exploratory scripts to interact with databases

**Learning Focus:**
- Understanding how Docker simplifies infrastructure setup
- Comparing batch data (products) vs time-series data (sensors)
- Implementing basic data validation logic
- Writing SQL queries for analytics

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────┐
│  DOCKER ENVIRONMENT (Runs on Your Laptop)            │
│                                                      │
│  ┌─────────────────┐      ┌──────────────────┐       │
│  │   PostgreSQL    │      │   TimescaleDB    │       │
│  │   (Port 5432)   │      │   (Port 5433)    │       │
│  │                 │      │                  │       │
│  │  Batch Pipeline │      │ Streaming Data   │       │
│  │  Product Data   │      │ Sensor Readings  │       │
│  └─────────────────┘      └──────────────────┘       │
│           ↑                        ↑                 │
│           └────────────────────────┘                 │
│              Python Scripts Access Both              │
└──────────────────────────────────────────────────────┘
```

**PostgreSQL Stores:**
- `raw_products` - Unvalidated product data (like it arrives)
- `clean_products` - Validated, cleaned product data
- `quarantine_products` - Invalid records with reasons

**TimescaleDB Stores:**
- `sensor_readings` - Valid IoT sensor data (temperature, humidity, pressure)
- `sensor_readings_invalid` - Invalid sensor readings

## 🛠️ Tech Stack

| Technology | Purpose | Why This Choice |
|-----------|---------|-----------------|
| **Docker** | Containerization | Run PostgreSQL and TimescaleDB without complex installation |
| **PostgreSQL 15** | Relational Database | Industry-standard for structured data with ACID guarantees |
| **TimescaleDB** | Time-Series Database | PostgreSQL extension optimized for sensor/metrics data |
| **Python** | Scripting & Analysis | Connect to databases, run queries, explore data |
| **psycopg2** | Database Driver | Python library to interact with PostgreSQL |

## 🚀 Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- Python 3.9 or higher
- A code editor (VS Code, PyCharm, etc.)

### Installation Steps

```bash
# 1. Clone this repository
git clone https://github.com/yourusername/DE-Portfolio.git
cd DE-Portfolio/docker-databases-demo

# 2. Start the databases (this downloads Docker images first time - takes a few minutes)
docker-compose up -d

# 3. Verify containers are running
docker-compose ps
# You should see my_postgres and my_timescaledb with status "Up"

# 4. Set up Python environment
python -m venv venv

# Activate virtual environment
# On Mac/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# 5. Install Python dependencies
pip install -r requirements.txt
```

### Verify Everything Works

```bash
# Check PostgreSQL has data
docker exec -it my_postgres psql -U pipeline_user -d pipeline_db -c "SELECT COUNT(*) FROM raw_products;"
# Expected output: count = 4

# Check TimescaleDB has data
docker exec -it my_timescaledb psql -U timeseries_user -d timeseries_db -c "SELECT COUNT(*) FROM sensor_readings;"
# Expected output: count = 1000
```

If you see these counts, you're ready to explore! 🎉

## 🎮 Exploring the Project

### 1. Explore PostgreSQL (Batch Data)

```bash
python python_scripts/explore_postgres.py
```

**What you'll see:**
- List of all database tables and their sizes
- Raw product data (messy, with issues like missing values and duplicates)
- Clean product data (validated and standardized)
- Analytics queries showing inventory value and price distributions
- Interactive SQL mode where you can type your own queries

**Try these queries in interactive mode:**
```sql
-- See all raw data
SELECT * FROM raw_products;

-- See only unprocessed records
SELECT * FROM raw_products WHERE processed = FALSE;

-- Find expensive products
SELECT * FROM clean_products WHERE price > 50;
```

### 2. Explore TimescaleDB (Time-Series Data)

```bash
python python_scripts/explore_timescale.py
```

**What you'll see:**
- TimescaleDB version and hypertable configuration
- Recent sensor readings (temperature, humidity, pressure)
- Time-series aggregations (5-minute averages)
- How data is distributed across different time windows
- Comparison of readings across different warehouse locations

**Key Concept Demonstrated:**
TimescaleDB automatically organizes data into time-based "chunks" for faster queries when you ask questions like "what happened in the last hour?"

### 3. Test Data Validation

```bash
python python_scripts/insert_and_validate.py
```

**What this does:**
Inserts 4 test product records with different quality issues:
1. ✅ **Valid product** → Goes to clean zone
2. ❌ **Missing product name** → Goes to quarantine
3. ❌ **Negative stock value** → Goes to quarantine
4. ❌ **Invalid price format** → Goes to quarantine

**Key Concept Demonstrated:**
Never lose data! Even invalid records are stored in quarantine so they can be reviewed and corrected later.

## 📁 Project Structure

```
data-pipeline-exploration/
│
├── README.md                        # This file
├── docker-compose.yml               # Defines PostgreSQL and TimescaleDB containers
├── requirements.txt                 # Python dependencies
│
├── sql/
│   ├── init_postgres.sql           # Creates tables and sample data for PostgreSQL
│   └── init_timescale.sql          # Creates tables and sample data for TimescaleDB
│
└── python_scripts/
    ├── explore_postgres.py         # Interactive exploration of batch database
    ├── explore_timescale.py        # Interactive exploration of time-series database
    └── insert_and_validate.py      # Demonstrates data validation logic
```

## 🔍 Key Concepts Demonstrated

### 1. Docker Containerization
- **Services as Containers**: Each database runs in its own isolated container
- **Port Mapping**: `localhost:5432` (PostgreSQL) and `localhost:5433` (TimescaleDB)
- **Volume Persistence**: Data survives container restarts
- **Automatic Initialization**: SQL scripts run automatically on first startup

### 2. Data Quality Patterns

**Three-Zone Architecture:**
```
Raw Zone → Validation Logic → Clean Zone
                  ↓
              Quarantine (for invalid data)
```

**Why this matters:**
- Raw zone preserves original data (can reprocess if validation logic changes)
- Clean zone only contains validated data (ready for analytics)
- Quarantine zone captures issues (nothing is lost, can be reviewed)

### 3. Batch vs Time-Series Data

**Batch Data (Products):**
- Infrequent updates
- Relationships between entities (products, categories, suppliers)
- Analytics focus: "What's our total inventory value?"

**Time-Series Data (Sensors):**
- Constant stream of timestamped readings
- Time-based queries: "What was average temperature in the last hour?"
- Optimized for aggregations over time windows

### 4. Data Validation
- **Schema validation**: Required fields exist
- **Type validation**: Prices are numbers, stock is integer
- **Range validation**: Stock can't be negative, temperature must be realistic
- **Format standardization**: "$19.99" → 19.99

## 📊 Example Queries to Try

### PostgreSQL (Batch)

```sql
-- Data quality overview
SELECT 
    COUNT(*) as total_raw,
    SUM(CASE WHEN processed THEN 1 ELSE 0 END) as processed_count,
    (SELECT COUNT(*) FROM clean_products) as clean_count,
    (SELECT COUNT(*) FROM quarantine_products) as quarantine_count
FROM raw_products;

-- Most valuable products
SELECT name, price, stock, (price * stock) as inventory_value
FROM clean_products
ORDER BY inventory_value DESC;

-- Price distribution
SELECT 
    CASE 
        WHEN price < 20 THEN 'Budget'
        WHEN price < 60 THEN 'Mid-range'
        ELSE 'Premium'
    END as category,
    COUNT(*) as product_count
FROM clean_products
GROUP BY category;
```

### TimescaleDB (Time-Series)

```sql
-- Average temperature per sensor (last hour)
SELECT 
    sensor_id,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp,
    COUNT(*) as reading_count
FROM sensor_readings
WHERE time > NOW() - INTERVAL '1 hour'
GROUP BY sensor_id;

-- 5-minute aggregated data
SELECT 
    time_bucket('5 minutes', time) as bucket,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp
FROM sensor_readings
WHERE time > NOW() - INTERVAL '30 minutes'
GROUP BY bucket
ORDER BY bucket DESC;

-- Compare warehouses
SELECT 
    location,
    COUNT(*) as reading_count,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp
FROM sensor_readings
GROUP BY location;
```

## 🛑 Stopping the Project

```bash
# Stop containers (data is preserved)
docker-compose stop

# Stop and remove containers (data still preserved in Docker volumes)
docker-compose down

# Remove everything including data (fresh start)
docker-compose down -v
```

## 🎓 What I Learned

### Docker & Infrastructure
- How to define multi-container applications with docker-compose
- Port mapping and container networking
- Volume management for data persistence
- Using healthchecks to verify services are ready

### Database Fundamentals
- Difference between relational and time-series databases
- Schema design for different data types
- Writing SQL queries for analytics
- Using indexes for performance

### Data Engineering Patterns
- Raw → Clean → Quarantine architecture
- Data validation strategies
- Handling messy real-world data
- Preserving data lineage

### Python for Data Engineering
- Connecting to databases with psycopg2
- Programmatic data insertion and validation
- Building interactive exploration scripts
- Error handling for production-like code

## 🚧 Future Enhancements

This is a learning project with room to grow:

- [ ] Add Apache Airflow for orchestration
- [ ] Implement Kafka for streaming sensor data
- [ ] Build a simple dashboard (Streamlit or Grafana)
- [ ] Add Great Expectations for automated data quality checks
- [ ] Create automated tests (pytest)
- [ ] Add more complex validation rules
- [ ] Implement data lineage tracking

## 🔗 Resources Used

- [Docker Documentation](https://docs.docker.com/)
- [PostgreSQL Tutorial](https://www.postgresql.org/docs/)
- [TimescaleDB Docs](https://docs.timescale.com/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)

💡 **Note**: This is a learning project focused on understanding fundamentals. For production use, additional considerations like security, monitoring, backup strategies, and scalability would be needed.
