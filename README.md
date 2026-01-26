# Data Engineering Portfolio

Welcome! I'm building this portfolio to document my understanding of engineering concepts through hands-on projects. Each project focuses on understanding fundamental tools and patterns used in production data systems.

## 🚀 Projects

### 1. [Data Pipeline Exploration - Docker & Databases](./docker-databases-demo) ✅ Complete

**Tech Stack:** Docker, PostgreSQL, TimescaleDB, Python

My first hands-on project exploring how Docker simplifies data infrastructure and how different databases handle different types of workloads.

**What I Built:**
- Multi-container Docker environment with PostgreSQL and TimescaleDB
- Interactive Python scripts to explore batch and time-series data
- Data validation logic with three-zone architecture (Raw → Clean → Quarantine)
- Comparison of relational vs time-series database patterns

**What I Learned:**
- Docker basics: containers, volumes, port mapping, docker-compose
- PostgreSQL fundamentals and SQL analytics queries
- TimescaleDB for time-series data optimization
- Data quality patterns and validation strategies
- Python database connectivity with psycopg2

**Try It Yourself:**
```bash
git clone https://github.com/BKettler-DE/DE-Portfolio.git
cd DE-Portfolio/docker-databases-demo
docker-compose up -d
python python_scripts/explore_postgres.py
```

[View Project Details →](./docker-databases-demo)

---

### 2. Upcoming: Batch Pipeline with Airflow ⏳ In Progress

**Planned Tech Stack:** Apache Airflow, Great Expectations, Python

Building on the foundation from Project #1, I'll add workflow orchestration and automated data quality checks.

**Learning Goals:**
- Airflow DAG design and task dependencies
- Implementing Great Expectations for data validation
- Error handling and retries
- Monitoring pipeline health

---

### 3. Upcoming: Streaming Data with Kafka 🔜 Planned

**Planned Tech Stack:** Apache Kafka, Python Kafka consumers/producers

Exploring real-time data streaming patterns with Kafka message queues.

**Learning Goals:**
- Kafka producer/consumer patterns
- Handling late-arriving data
- Deduplication strategies
- Real-time validation

---

## 🛠️ Skills I'm Building

### Currently Comfortable With:
- **Docker**: Containers, docker-compose, volumes, networking
- **Python**: Scripts, data processing, database connectivity
- **SQL**: SELECT queries, aggregations, JOINs, window functions
- **PostgreSQL**: Schema design, indexing, basic optimization
- **Git**: Version control, branching, commits

### Actively Learning:
- **Apache Airflow**: DAGs, operators, scheduling, monitoring
- **Kafka**: Message queues, streaming patterns, consumers
- **Data Quality**: Great Expectations, validation frameworks
- **Cloud Platforms**: AWS (S3, Redshift, Glue)
- **dbt**: Data transformations and testing

### On My Roadmap:
- Apache Spark for large-scale processing
- Terraform for infrastructure as code
- Kubernetes for container orchestration
- Data warehousing (Snowflake, BigQuery)
- CI/CD for data pipelines

## 📚 Learning Path

### Completed ✅
- [x] Docker fundamentals and containerization
- [x] PostgreSQL basics and schema design
- [x] TimescaleDB for time-series data
- [x] Python database connectivity
- [x] Basic data validation patterns
- [x] SQL analytics queries

### In Progress 🔄
- [ ] Apache Airflow orchestration
- [ ] Great Expectations framework
- [ ] Kafka streaming basics
- [ ] Error handling and retries
- [ ] Data quality metrics

### Next Up ⏳
- [ ] Cloud deployment (AWS/Docker on EC2)
- [ ] dbt for transformations
- [ ] Data modeling and dimensional design
- [ ] CI/CD for pipelines

## 🎯 Learning Approach

**1. Build Small, Working Projects**
Each project is:
- Runnable on a laptop (no cloud costs while learning)
- Well-documented with clear explanations
- Focused on specific concepts
- Something I can demo to others

**2. Learn by Doing**
- Read documentation and tutorials
- Build hands-on projects
- Break things and fix them
- Document what I learned

**3. Focus on Fundamentals**
Before moving to advanced topics, I'm making sure I understand:
- Why tools exist (what problem do they solve?)
- When to use them (batch vs streaming, SQL vs NoSQL)
- How they work (not just how to use them)

## 🗺️ Portfolio Roadmap

### Phase 1: Foundations (Current) ✅
**Goal:** Understand core data infrastructure
- ✅ Docker and containerization
- ✅ Relational databases (PostgreSQL)
- ✅ Time-series databases (TimescaleDB)
- ✅ Basic data validation patterns

### Phase 2: Orchestration (In Progress) 🔄
**Goal:** Learn workflow management
- 🔄 Apache Airflow DAGs
- 🔄 Great Expectations for data quality
- ⏳ Error handling and monitoring
- ⏳ Scheduled batch processing

### Phase 3: Streaming (Next Up) ⏳
**Goal:** Real-time data processing
- ⏳ Apache Kafka basics
- ⏳ Stream processing patterns
- ⏳ Real-time validation
- ⏳ Handling late data

### Phase 4: Cloud & Scale (Future) 🔜
**Goal:** Production deployment
- 🔜 AWS deployment (EC2, S3, Redshift)
- 🔜 Infrastructure as Code (Terraform)
- 🔜 Data warehouse design

## 💡 Project Ideas Queue

Projects I want to build as I learn:

1. **Batch ETL with Airflow** (Next up)
   - Orchestrate the existing pipeline with Airflow
   - Add Great Expectations validation
   - Implement retry logic and monitoring

2. **Real-Time Sensor Pipeline** (After Airflow)
   - Kafka producers generating IoT data
   - Python consumers validating in real-time
   - TimescaleDB for storage

3. **Cloud Data Warehouse** (After Kafka)
   - Deploy to AWS (S3 + Redshift)
   - dbt for transformations
   - Dashboard with visualizations

4. **Change Data Capture (CDC)** (Advanced)
   - Debezium for CDC from PostgreSQL
   - Kafka for event streaming
   - Multiple downstream consumers

*Last Updated: January 2026*

*Project Count: 1 complete, 2 in progress*
