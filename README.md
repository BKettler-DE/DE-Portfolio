# Data Engineering Portfolio

Demonstrating production-ready data engineering skills through hands-on projects. This portfolio documents my unnderstanding of fundamentals and advanced concepts, with each project demonstrating real-world data engineering patterns.


## 🚀 Featured Projects

### [Data Pipeline Exploration - Complete Stack](./data-pipeline-exploration) ✅ Complete

**A comprehensive, production-ready data engineering environment**

**Tech Stack:** Docker, PostgreSQL, TimescaleDB, Apache Kafka, Apache Airflow, Zookeeper, Python

Built a complete local data engineering environment demonstrating four progressive layers:
- **Layer 1**: Docker containerization for reproducible infrastructure
- **Layer 2**: Database fundamentals - PostgreSQL + TimescaleDB comparison
- **Layer 3**: Real-time streaming with Kafka producer/consumer
- **Layer 4**: Batch orchestration with Apache Airflow

**What's Actually Built:**

✅ **Complete Batch Pipeline (Airflow)**
- Scheduled DAG running daily at 2 AM
- 7-task workflow: Extract → Load Raw → Validate → Load Clean/Quarantine → Report
- Product data scraper generating messy data
- Comprehensive validation with detailed error tracking
- Three-zone architecture (Raw → Clean → Quarantine)
- Full orchestration visible in Airflow web UI

✅ **Complete Streaming Pipeline (Kafka)**
- Real-time IoT sensor data: Producer → Kafka → Consumer → TimescaleDB
- Live validation, deduplication, and error quarantine
- Handles 10+ events/second continuously
- Time-series optimization with hypertables

✅ **Interactive Database Exploration**
- Scripts to query and analyze both databases
- Sample data pre-loaded for immediate exploration
- Analytics queries demonstrating SQL skills

**Key Features:**
- ✅ **4 complete layers** - Docker → Databases → Streaming → Orchestration
- ✅ **7 Docker services** orchestrated together
- ✅ **Both batch and streaming** architectures working simultaneously
- ✅ **Production patterns**: Idempotent, observable, error-handling
- ✅ **Data quality obsession**: Validation, quarantine, metrics tracking
- ✅ **Real-time demo capable**: All pipelines can run live
- ✅ **Fully documented**: Setup guides, troubleshooting, architecture diagrams

**What Makes This Different:**

Unlike tutorial projects, this demonstrates production-aware thinking:
- **Streaming is fully implemented** - working producer/consumer with Kafka
- **Batch is fully orchestrated** - Airflow DAG with task dependencies
- Invalid data is quarantined with reasons, not dropped
- Deduplication prevents processing the same event twice
- Observable with real-time statistics and validation metrics
- Handles realistic data quality issues (late arrivals, missing fields, duplicates)
- Can run end-to-end and watch both pipelines simultaneously
- Survived real debugging - fixed 5+ issues during development

**Metrics:**
- 4 progressive layers (Docker → Databases → Streaming → Orchestration)
- 7 Docker services orchestrated together
- 10 Python files:
  - 2 streaming pipeline scripts (producer/consumer)
  - 2 batch pipeline scripts (scraper/validator)
  - 1 Airflow DAG (7 tasks)
  - 4 exploration scripts
  - 1 Kafka exploration script
- 3 database schemas (PostgreSQL, TimescaleDB, Airflow metadata)
- 1200+ lines of documented, working code

[View Project Details →](./data-pipeline-exploration)

### [Cloud Data Warehouse & Analytics](./cloud-data-warehouse) ✅ Complete

**Modern analytics platform demonstrating transformation and dimensional modeling**

**Tech Stack:** DuckDB, dbt Core, Streamlit, Plotly, Python, Parquet

Built the analytics consumption layer that transforms raw data into business insights:
- **Data Lake Pattern**: Parquet files simulating S3 intermediate storage
- **dbt Transformations**: SQL-based modeling with testing and documentation
- **Dimensional Warehouse**: Star schema with fact and dimension tables
- **Interactive Dashboard**: Self-service analytics with Streamlit

**What's Actually Built:**

✅ **Data Export Bridge**
- Python script connecting pipeline project to warehouse
- Exports to parquet files (data lake pattern)
- Handles 1,650+ rows across 4 source tables
- Daily export workflow ready for scheduling

✅ **dbt Transformation Pipeline**
- 2 staging models (views for standardization)
- 2 marts models (tables for analytics)
- Surrogate key generation with dbt_utils
- Comprehensive data quality tests
- Auto-generated documentation with lineage graph

✅ **DuckDB Analytical Warehouse**
- Embedded warehouse (production-grade, used by Figma/Notion)
- Star schema: 1 dimension + 1 fact table
- Business categorizations (price tiers, temperature ranges)
- Anomaly detection and data quality flags
- Fast queries on pre-computed tables

✅ **Streamlit Analytics Dashboard**
- 4 interactive pages (Overview, Sensors, Products, Quality)
- Real-time metrics and visualizations
- Plotly charts (time-series, distributions, pie charts)
- Drill-down filtering by sensor
- Data quality monitoring

**Key Features:**
- ✅ **Complete data stack** - Lake → Transform → Warehouse → Dashboard
- ✅ **Modern data stack** - dbt-centric transformation workflow
- ✅ **Dimensional modeling** - Kimball star schema methodology
- ✅ **Production patterns** - Staging/marts layers, surrogate keys, testing
- ✅ **End-to-end data flow** - Ingestion → Transformation → Visualization
- ✅ **Self-service analytics** - Business users can explore without SQL

**What Makes This Different:**

Demonstrates the consumption side of data engineering:
- **Builds on pipeline project** - Shows complete data lifecycle
- **Modern tooling** - dbt is industry standard for transformation
- **Analytics engineering** - SQL-first, version-controlled transformations
- **Dimensional design** - Proper fact/dimension separation
- **Data quality focus** - Testing at every layer
- **Business value** - Dashboard shows actual insights
- **Cloud-ready** - DuckDB → Snowflake migration is trivial

**Metrics:**
- 2-layer transformation architecture (Staging → Marts)
- 4 dbt models (2 views + 2 tables)
- 1 DuckDB warehouse (~1,650 rows across 2 schemas)
- 1 interactive dashboard (4 pages, 10+ visualizations)
- 6 data quality tests
- Full lineage documentation
- 600+ lines of SQL and Python

**Integration with Pipeline Project:**
```
Pipeline Project (Ingestion)
    ↓
Data Lake (Parquet Files)
    ↓
dbt Transformations
    ↓
DuckDB Warehouse
    ↓
Streamlit Dashboard
```

**Together, the projects demonstrate:**
- Raw data → Ingestion (Kafka/Airflow) → Storage (PostgreSQL/TimescaleDB)
- Export → Data Lake (Parquet) → Transform (dbt) → Warehouse (DuckDB)
- Analytics → Visualization (Streamlit) → Insights

[View Project Details →](./cloud-data-warehouse)


## 🛠️ Technical Skills

### Languages & Frameworks
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat-square&logo=gnu-bash&logoColor=white)

### Data Engineering Tools

**Infrastructure:**
- Docker & Docker Compose (multi-container orchestration)
- Apache Kafka (message streaming)
- Apache Airflow (workflow orchestration)
- Apache Zookeeper (coordination)

**Databases:**
- PostgreSQL (relational, ACID)
- TimescaleDB (time-series optimization)
- Understanding of: NoSQL, data warehouses, data lakes

**Processing:**
- Python (pandas, psycopg2, kafka-python)
- SQL (complex queries, window functions, CTEs, JSONB)
- Batch processing with Airflow
- Stream processing with Kafka

**Coming Soon:**
- Great Expectations (data quality framework)
- dbt (data transformation)
- Apache Spark (large-scale processing)
- Cloud deployment (AWS/Azure/GCP)

### Data Engineering Concepts

**Architecture Patterns:**
- Batch and Streaming architectures (both implemented)
- Producer/Consumer decoupling
- Three-zone data quality (Raw/Clean/Quarantine)
- Idempotent pipeline design
- Data lineage and observability
- Workflow orchestration with DAGs

**Workflow Orchestration:**
- Airflow DAG design and task dependencies
- XCom for inter-task communication
- Scheduling with cron expressions
- Error handling and retries
- Monitoring and logging

**Database Design:**
- Schema design for different workloads
- Indexing strategies
- Time-series optimization with hypertables
- JSONB for flexible schemas
- Partitioning approaches

**Data Quality:**
- Validation at ingestion (batch and streaming)
- Schema enforcement
- Range checking and anomaly detection
- Deduplication strategies
- Error quarantine (preserve, don't drop)
- Quality metrics tracking

## 📈 Project Evolution

### Completed Milestones ✅

**Phase 1: Foundations** (Complete)
- ✅ Docker containerization
- ✅ PostgreSQL and TimescaleDB setup
- ✅ Database exploration scripts

**Phase 2: Streaming** (Complete)
- ✅ Kafka producer/consumer implementation
- ✅ Real-time validation logic
- ✅ Deduplication and error handling
- ✅ TimescaleDB integration

**Phase 3: Orchestration** (Complete)
- ✅ Apache Airflow setup
- ✅ Batch pipeline DAG implementation
- ✅ Product scraper and validator
- ✅ Three-zone data quality architecture
- ✅ Workflow monitoring and logging

### Current Status
- **4 layers fully functional** - Can demo end-to-end
- **Both architectures working** - Batch and streaming running simultaneously
- **Production patterns implemented** - Validation, quarantine, idempotency
- **Real debugging experience** - Solved 5+ real issues
 
*Last Updated: January 2026*
