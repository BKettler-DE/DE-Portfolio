# Data Engineering Portfolio

Building production-ready data engineering skills through hands-on projects. This portfolio documents my understanding from fundamentals to advanced concepts, with each project demonstrating real-world data engineering patterns.

## 🚀 Featured Project

### [Data Pipeline Exploration - Docker, Databases & Streaming](./data-pipeline-exploration) ✅ Complete

**A comprehensive exploration of modern data engineering infrastructure**

**Tech Stack:** Docker, PostgreSQL, TimescaleDB, Apache Kafka, Zookeeper, Python

Built a complete local data engineering environment with three progressive layers:
- **Layer 1**: Docker containerization for reproducible infrastructure
- **Layer 2**: Database fundamentals - PostgreSQL + TimescaleDB comparison
- **Layer 3**: Real-time streaming pipeline with Kafka (fully implemented producer/consumer)

**What's Actually Built:**
- ✅ **Complete streaming pipeline**: IoT sensors → Kafka → Validation → TimescaleDB
- ✅ **Real-time data quality**: Validation, deduplication, quarantine pattern
- ✅ **Interactive exploration**: Scripts to query and understand both databases
- ⚠️ **Batch pipeline**: Sample data and validation concept (automated pipeline not yet built)

**Key Features:**
- ✅ Three-zone data quality architecture (Raw → Clean → Quarantine)
- ✅ Real-time streaming pipeline with validation and deduplication
- ✅ Time-series optimization for IoT sensor data
- ✅ Interactive exploration scripts for hands-on learning
- ✅ Handles messy, real-world data (duplicates, missing values, out-of-range)
- ✅ Producer/consumer decoupling with Kafka message queue
- ✅ Complete Docker Compose setup (4 services, 1 command to start)

**What Makes This Different:**
Unlike tutorial projects that gloss over data quality, this demonstrates production patterns:
- Invalid data isn't dropped - it's quarantined with reasons
- Deduplication logic prevents processing the same event twice
- Idempotent design allows safe pipeline re-runs
- Observable with statistics and validation metrics
- Raw data preservation enables reprocessing with updated logic

**Metrics:**
- 3 distinct layers (containerization, databases, streaming)
- 4 Docker services orchestrated together
- 7 Python scripts (3 exploratory, 2 pipeline, 2 validation)
- 2 database schemas with sample data
- 1000+ lines of working, documented code

[View Project Details →](./data-pipeline-exploration)

**Live Demo Capabilities:**
```bash
# Start everything
docker-compose up -d

# Terminal 1: Producer sends 10 readings/second
python streaming_pipeline/sensor_simulator.py

# Terminal 2: Consumer validates in real-time
python streaming_pipeline/stream_validator.py

# Terminal 3: Watch data flowing into database
# Can literally see counts increasing!
```

## 🛠️ Technical Skills

### Languages & Frameworks
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat-square&logo=gnu-bash&logoColor=white)

### Data Engineering Tools

**Infrastructure:**
- Docker & Docker Compose (containerization)
- Apache Kafka (message streaming)
- Apache Zookeeper (coordination)

**Databases:**
- PostgreSQL (relational, ACID)
- TimescaleDB (time-series optimization)
- Understanding of: NoSQL, data warehouses, data lakes

**Processing:**
- Python (pandas, psycopg2, kafka-python)
- SQL (complex queries, window functions, CTEs)
- Batch processing patterns
- Stream processing patterns

**Coming Soon:**
- Apache Airflow (workflow orchestration)
- Great Expectations (data quality framework)
- dbt (data transformation)
- Apache Spark (large-scale processing)

### Data Engineering Concepts

**Architecture Patterns:**
- Batch vs Streaming trade-offs
- Producer/Consumer decoupling
- Three-zone data quality (Raw/Clean/Quarantine)
- Idempotent pipeline design
- Data lineage and observability

**Database Design:**
- Schema design for different workloads
- Indexing strategies
- Time-series optimization
- Partitioning approaches

**Data Quality:**
- Validation at ingestion
- Schema enforcement
- Range checking and anomaly detection
- Deduplication strategies
- Error quarantine (preserve, don't drop)
  
*Last Updated: January 2026*
