# Cloud Data Warehouse & Analytics

A production-ready analytics platform built on modern data stack principles. This project demonstrates data transformation, dimensional modeling, and self-service analytics - the consumption side of data engineering that turns raw data into business insights.

## 🎯 Project Overview

This project builds the **analytics layer** on top of the [data pipeline project](../data-pipeline-exploration), demonstrating how raw data flows from ingestion through transformation to visualization. It showcases:

1. **Data Lake Pattern** - Parquet files as intermediate storage (simulating S3)
2. **dbt Transformations** - SQL-based data modeling with testing and documentation
3. **Dimensional Modeling** - Star schema with fact and dimension tables
4. **Analytics Dashboard** - Interactive Streamlit app for business intelligence

**What This Demonstrates:**
- Modern data transformation with dbt
- Dimensional warehouse design (Kimball methodology)
- Data quality testing and validation
- Self-service analytics with interactive dashboards
- End-to-end data flow from source to insights

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  PIPELINE PROJECT (Upstream)                                    │
│  PostgreSQL + TimescaleDB                                       │
│  (Batch & Streaming Data)                                       │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Python Export Script
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  DATA LAKE (Local Parquet Files - Simulates S3)                 │
│                                                                 │
│  data_lake/                                                     │
│  ├── raw/                                                       │
│  │   ├── batch/           (Products from PostgreSQL)            │
│  │   │   ├── clean_products_20260128.parquet                    │
│  │   │   └── quarantine_products_20260128.parquet               │
│  │   └── streaming/       (Sensors from TimescaleDB)            │
│  │       ├── sensor_readings_20260128.parquet                   │
│  │       └── sensor_readings_invalid_20260128.parquet           │
│  └── metadata/                                                  │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ dbt reads parquet files
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  DBT TRANSFORMATION LAYER                                       │
│                                                                 │
│  Staging (Views)    → Standardize raw data                      │
│  ├── stg_products                                               │
│  └── stg_sensor_readings                                        │
│                                                                 │
│  Marts (Tables)     → Analytics-ready data                      │
│  ├── dim_products   (Product dimension)                         │
│  └── fct_sensor_readings (Sensor fact table)                    │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ Stored in DuckDB
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  DUCKDB WAREHOUSE (warehouse.duckdb)                            │
│                                                                 │
│  Schemas:                                                       │
│  ├── main_staging  (2 views)                                    │
│  └── main_marts    (2 tables - ~1,650 rows)                     │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ SQL queries
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│  STREAMLIT DASHBOARD (Port 8501)                                │
│                                                                 │
│  Pages:                                                         │
│  ├── Overview      (Key metrics, recent activity)               │
│  ├── Sensor Analytics (Time-series, patterns)                   │
│  ├── Product Analytics (Inventory, pricing)                     │
│  └── Data Quality  (Validation metrics, anomalies)              │
└─────────────────────────────────────────────────────────────────┘
```

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Lake** | Parquet files | Intermediate storage (simulates S3) |
| **Warehouse** | DuckDB | Embedded analytical database |
| **Transformation** | dbt Core 1.7+ | SQL-based data modeling |
| **Dashboard** | Streamlit | Interactive web analytics |
| **Visualization** | Plotly | Interactive charts |
| **Language** | Python 3.11 | Orchestration and dashboards |
| **Export** | pandas, psycopg2 | Extract data from pipeline DBs |

**Why DuckDB?**
- Production-grade analytics (used by Figma, Notion)
- Free and embedded (no server needed)
- Reads parquet natively (perfect for data lake pattern)
- Same SQL as Snowflake/Redshift (easy cloud migration)
- Handles millions of rows efficiently

## 🚀 Quick Start

### Prerequisites

- Completed [data pipeline project](../data-pipeline-exploration) setup
- Pipeline containers running (PostgreSQL, TimescaleDB)
- Python 3.9 or higher
- Data in your pipeline databases

### Installation

```bash
# 1. Navigate to warehouse project
cd cloud-data-warehouse

# 2. Create virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Export data from pipeline project
cd data_ingestion
python export_from_pipeline.py
cd ..

# Should see:
# ✅ Exported 40 clean products
# ✅ Exported 11 quarantined products  
# ✅ Exported 1,612 sensor readings
# ✅ Exported 24 invalid sensor readings

# 5. Run dbt transformations
cd dbt_warehouse
dbt deps  # Install dbt packages
dbt run   # Build all models

# Should see:
# Completed successfully
# Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4

# 6. Launch dashboard
cd ../dashboard
streamlit run app.py

# Opens in browser at http://localhost:8501
```

### Verify Setup

```bash
# Check data lake files
ls data_lake/raw/batch/
ls data_lake/raw/streaming/
# Should see .parquet files with today's date

# Check warehouse tables
python
>>> import duckdb
>>> conn = duckdb.connect('dbt_warehouse/warehouse.duckdb')
>>> conn.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema LIKE '%staging' OR table_schema LIKE '%marts'").df()
# Should show 2 views (staging) and 2 tables (marts)
>>> exit()

# Check dashboard is running
# Browser should open to http://localhost:8501
# Navigate between pages to verify data displays
```

## 📚 Understanding the Layers

### Layer 1: Data Export Bridge

**File:** `data_ingestion/export_from_pipeline.py`

Connects the pipeline project to this warehouse project:

```python
# Reads from pipeline databases
PostgreSQL (products) → clean_products_YYYYMMDD.parquet
TimescaleDB (sensors) → sensor_readings_YYYYMMDD.parquet

# Creates data lake structure
data_lake/
├── raw/batch/       (batch pipeline data)
├── raw/streaming/   (streaming pipeline data)  
└── metadata/        (export tracking)
```

**Why parquet?**
- Industry standard for data lakes (AWS S3, Azure Data Lake, GCS)
- Columnar format (fast analytics)
- Built-in compression (smaller files)
- Used by Spark, Snowflake, BigQuery

**Run it:**
```bash
python data_ingestion/export_from_pipeline.py
```

### Layer 2: dbt Transformations

**Three-layer architecture:**

#### Staging Layer (Views)
**Purpose:** Standardize raw data, minimal transformation

```sql
-- stg_products.sql
SELECT 
    product_id,
    name AS product_name,           -- Rename for clarity
    price,
    stock AS stock_quantity,        -- Standardize names
    source AS source_system,
    loaded_at AS source_loaded_at   -- Track lineage
FROM read_parquet('../data_lake/raw/batch/clean_products_*.parquet')
```

**Why views?**
- Fast to build (just saves SQL)
- Always fresh (reads latest parquet files)
- No storage overhead

#### Marts Layer (Tables)
**Purpose:** Business-ready analytics tables

**Dimension Table - `dim_products`:**
```sql
SELECT 
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    price,
    -- Business categorizations
    CASE 
        WHEN price < 50 THEN 'budget'
        WHEN price < 200 THEN 'mid-range'
        WHEN price < 500 THEN 'premium'
        ELSE 'luxury'
    END as price_tier,
    ...
```

**Fact Table - `fct_sensor_readings`:**
```sql
SELECT
    reading_timestamp,
    sensor_id,
    temperature,
    humidity,
    -- Derived metrics
    CASE 
        WHEN temperature < 0 THEN 'freezing'
        WHEN temperature < 15 THEN 'cold'
        WHEN temperature < 25 THEN 'comfortable'
        ...
    END as temperature_category,
    -- Anomaly detection
    CASE 
        WHEN temperature < -50 OR temperature > 60 THEN TRUE
        ELSE FALSE
    END as is_anomaly
```

**Why tables?**
- Pre-computed transformations
- Fast queries (no parquet reading)
- Perfect for dashboards

**Run it:**
```bash
cd dbt_warehouse
dbt run              # Build all models
dbt test             # Run data quality tests
dbt docs generate    # Generate documentation
dbt docs serve       # View docs at http://localhost:8080
```

### Layer 3: Analytics Dashboard

**File:** `dashboard/app.py`

Interactive Streamlit app with 4 pages:

1. **Overview**
   - Key metrics cards
   - Latest sensor readings table
   - Temperature distribution chart

2. **Sensor Analytics**
   - Sensor selector dropdown
   - Temperature time-series line chart
   - Hourly pattern analysis
   - Location distribution pie chart

3. **Product Analytics**
   - Price tier breakdown
   - Stock status visualization
   - Category analysis table

4. **Data Quality**
   - Quality percentage over time
   - Anomaly detection by sensor
   - Completeness metrics

**Run it:**
```bash
streamlit run dashboard/app.py
# Opens at http://localhost:8501
```

## 📁 Project Structure

```
cloud-data-warehouse/
├── README.md                       # This file
├── requirements.txt                # Python dependencies
│
├── data_ingestion/                 # Bridge to pipeline project
│   ├── export_from_pipeline.py    # Exports to parquet
│   └── config.yaml                # Database credentials
│
├── data_lake/                     # Simulated S3 data lake
│   ├── raw/
│   │   ├── batch/                 # clean_products_*.parquet
│   │   ├── streaming/             # sensor_readings_*.parquet
│   │   └── metadata/              # Export tracking
│   └── .gitkeep
│
├── dbt_warehouse/                 # dbt transformation project
│   ├── dbt_project.yml            # dbt configuration
│   ├── profiles.yml               # DuckDB connection
│   ├── packages.yml               # dbt dependencies
│   │
│   └── models/
│       ├── sources.yml            # Define data sources
│       │
│       ├── staging/               # Layer 1: Standardization
│       │   ├── stg_products.sql
│       │   └── stg_sensor_readings.sql
│       │
│       └── marts/                 # Layer 2: Analytics tables
│           ├── analytics/
│           │   ├── dim_products.sql
│           │   └── fct_sensor_readings.sql
│           └── schema.yml         # Tests and docs
│
├── dashboard/                     # Streamlit analytics app
│   └── app.py                     # Interactive dashboard
│
└── warehouse.duckdb               # DuckDB warehouse file
```

## 🔍 Key Concepts Demonstrated

### 1. Modern Data Stack

**dbt-centric transformation:**
- SQL for transformations (not Python)
- Version control for analytics code
- Automated testing and documentation
- Modular, reusable models

### 2. Dimensional Modeling (Kimball)

**Star schema design:**
```
        dim_products (40 rows)
               |
               |
        fct_sensor_readings (1,612 rows)
```

**Why star schema?**
- Simple for end users to understand
- Fast aggregation queries
- Flexible for different analyses
- Industry standard for data warehouses

### 3. Data Lake Pattern

**Three zones:**
- **Raw:** Immutable source data (parquet files)
- **Staging:** Standardized views (in warehouse)
- **Marts:** Business-ready tables (in warehouse)

**Why this pattern?**
- Separation of concerns
- Can reprocess if logic changes
- Mirrors production (S3 → Snowflake)

### 4. Views vs Tables Strategy

| Layer | Materialization | Why |
|-------|----------------|-----|
| Staging | View | Always fresh, fast to rebuild |
| Marts | Table | Pre-computed, fast queries |

### 5. Data Quality Throughout

**Source level:**
```yaml
# sources.yml
columns:
  - name: product_id
    tests:
      - not_null
      - unique
```

**Model level:**
```yaml
# schema.yml
columns:
  - name: price_tier
    tests:
      - accepted_values:
          values: ['budget', 'mid-range', 'premium', 'luxury']
```

### 6. Self-Service Analytics

**Dashboard empowers users to:**
- Explore data without SQL knowledge
- Filter and drill down interactively
- Monitor data quality
- Track business metrics

## 📊 Sample Queries

### Dimensional Model Queries

```sql
-- Products by price tier with stock levels
SELECT 
    price_tier,
    COUNT(*) as product_count,
    SUM(stock_quantity) as total_stock,
    AVG(price) as avg_price
FROM main_marts.dim_products
GROUP BY price_tier
ORDER BY avg_price;

-- Temperature analysis with categorization
SELECT 
    temperature_category,
    COUNT(*) as reading_count,
    ROUND(AVG(temperature), 2) as avg_temp,
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp
FROM main_marts.fct_sensor_readings
WHERE is_valid_reading = TRUE
GROUP BY temperature_category
ORDER BY avg_temp;

-- Hourly sensor patterns
SELECT 
    reading_hour,
    COUNT(DISTINCT sensor_id) as active_sensors,
    ROUND(AVG(temperature), 2) as avg_temp,
    ROUND(AVG(humidity), 2) as avg_humidity
FROM main_marts.fct_sensor_readings
WHERE is_valid_reading = TRUE
GROUP BY reading_hour
ORDER BY reading_hour;

-- Data quality metrics
SELECT 
    reading_date,
    COUNT(*) as total_readings,
    SUM(CASE WHEN is_valid_reading THEN 1 ELSE 0 END) as valid_count,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN is_valid_reading THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_pct
FROM main_marts.fct_sensor_readings
GROUP BY reading_date
ORDER BY reading_date DESC;
```

### dbt Lineage Queries

```sql
-- See model dependencies
SELECT 
    ref('stg_products')  -- Staging view
    ↓
    ref('dim_products')  -- Dimension table

-- dbt compiles this to actual table names
-- and ensures stg_products runs first
```

## 🔄 Daily Workflow

```bash
# 1. Export fresh data from pipeline
cd data_ingestion
python export_from_pipeline.py

# 2. Run dbt transformations
cd ../dbt_warehouse
dbt run

# 3. (Optional) Run tests
dbt test

# 4. Dashboard auto-refreshes on page reload
# No restart needed - it reads from warehouse
```

## 🛑 Shutdown & Cleanup

```bash
# Stop dashboard
# Ctrl+C in terminal running streamlit

# Close dbt docs
# Ctrl+C in terminal running dbt docs serve

# Clean dbt artifacts
cd dbt_warehouse
dbt clean

# Remove warehouse and start fresh
rm warehouse.duckdb
dbt run  # Rebuilds from parquet files
```

## 🎓 What I Learned

### Modern Data Transformation ✅
- **dbt fundamentals** - Models, tests, docs, packages
- **Jinja templating** - `{{ ref() }}`, `{{ source() }}`, `{{ config() }}`
- **Materializations** - Views vs tables vs incremental
- **Data lineage** - Understanding dependencies
- **Testing as code** - Automated data quality validation

### Dimensional Modeling ✅
- **Star schema design** - Facts and dimensions
- **Surrogate keys** - Using `dbt_utils.generate_surrogate_key()`
- **Slowly Changing Dimensions** - Type 1 (overwrite)
- **Grain definition** - What does one row represent?
- **Derived attributes** - Adding business value

### Analytics Engineering ✅
- **Separation of concerns** - Staging vs marts layers
- **SQL-first approach** - Transformations in SQL, not Python
- **Documentation generation** - Auto-generated from YAML
- **Version control** - Git for analytics code
- **Modular design** - Reusable, tested components

### Data Lake Patterns ✅
- **Parquet format** - Columnar, compressed, efficient
- **Zone architecture** - Raw → Staging → Marts
- **Schema evolution** - Handling changes over time
- **Partitioning** - Wildcard patterns for file matching

### DuckDB for Analytics ✅
- **Embedded database** - No server needed
- **Parquet native** - Direct file reading
- **SQL compatibility** - Works like PostgreSQL/Snowflake
- **Performance** - Columnar storage, vectorized execution
- **Cloud migration path** - Same SQL works on Snowflake

### Dashboard Development ✅
- **Streamlit framework** - Python to web app
- **Interactive visualizations** - Plotly charts
- **State management** - Caching, session state
- **User experience** - Navigation, filtering, drill-down
- **Database connectivity** - Efficient querying

## 🔗 Relationship to Pipeline Project

This warehouse project **consumes data from** the [pipeline project](../data-pipeline-exploration):

| Pipeline Project | Warehouse Project |
|-----------------|-------------------|
| Data Ingestion | Data Transformation |
| Kafka/Airflow | dbt |
| Quality Validation | Quality Analytics |
| PostgreSQL/TimescaleDB | DuckDB |
| Operational Focus | Analytical Focus |
| Real-time + Batch | Star Schema Modeling |

**Together, they demonstrate:**
```
Raw Data → Ingestion → Storage → Transformation → Analytics → Visualization
(Sources) → (Pipeline) → (Lake) → (dbt) → (Warehouse) → (Dashboard)
```
*Last Updated: January 2026*

