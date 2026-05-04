# Glamira Real-Time Streaming Pipeline

## Overview

Real-time data pipeline that reads user behavior events from Kafka, processes them with Spark Structured Streaming, stores results in PostgreSQL, and visualizes with a Streamlit dashboard.
Kafka (remote) → Spark Structured Streaming → PostgreSQL → Streamlit Dashboard


## Architecture

### Data Flow
```
Kafka topic: product_view
↓ readStream (every 30 seconds)
Spark on YARN (Hadoop cluster)
↓ foreachBatch
├── dim_device   (browser, OS, device type)
├── dim_date     (year, month, day, hour)
├── dim_product  (product_id)
├── dim_store    (store_id)
├── dim_referrer (referrer domain)
├── dim_location (IP, country, domain)
└── fact_product_view (main fact table)
↓
PostgreSQL
↓
Streamlit Dashboard (auto-refresh every 30s)
```

### Database Schema

**Fact Table:**
- `fact_product_view` — one row per user behavior event

**Dimension Tables:**
- `dim_date` — date/time breakdown (year, month, day, hour)
- `dim_product` — unique products
- `dim_store` — unique stores
- `dim_location` — IP address, country code, domain
- `dim_device` — browser, OS, device type (parsed from user_agent)
- `dim_referrer` — referrer domain (extracted from referrer_url)

## Reports

The dashboard provides 6 real-time reports:

1. **Top 10 products** by views today
2. **Top 10 countries** by views today (extracted from current_url domain)
3. **Top 5 referrer domains** by views today
4. **Stores by views** for any selected country
5. **Views by hour** for any product
6. **Views by browser & OS** breakdown

## Project Structure
```
99-project/
├── kafka_streaming.py   # Main Spark streaming pipeline
├── dashboard.py         # Streamlit real-time dashboard
├── browser/
│   ├── init.py
│   └── browser.py       # User-agent parsing UDF
└── README.md
```

## Prerequisites

### Infrastructure (Docker)
- Hadoop cluster (namenode, 2 datanodes, resourcemanager, 2 nodemanagers)
- Kafka (3 brokers)
- PostgreSQL

### Python dependencies
pyspark==3.5.1
psycopg2-binary
user-agents
streamlit
pandas
## Setup

### 1. Create PostgreSQL tables

Connect to PostgreSQL and run:

```sql
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    year INT, month INT, day INT, hour INT
);
CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    product_id VARCHAR UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_store (
    store_key BIGSERIAL PRIMARY KEY,
    store_id VARCHAR UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_location (
    location_key BIGSERIAL PRIMARY KEY,
    ip_address VARCHAR UNIQUE,
    country_code VARCHAR,
    country_name VARCHAR,
    domain VARCHAR
);
CREATE TABLE IF NOT EXISTS dim_device (
    device_key BIGSERIAL PRIMARY KEY,
    browser VARCHAR NOT NULL,
    os VARCHAR NOT NULL,
    device_type VARCHAR NOT NULL,
    UNIQUE (browser, os, device_type)
);
CREATE TABLE IF NOT EXISTS dim_referrer (
    referrer_key BIGSERIAL PRIMARY KEY,
    referrer_domain VARCHAR UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_product_view (
    view_id VARCHAR PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    product_key BIGINT REFERENCES dim_product(product_key),
    store_key BIGINT REFERENCES dim_store(store_key),
    location_key BIGINT REFERENCES dim_location(location_key),
    device_key BIGINT REFERENCES dim_device(device_key),
    referrer_key BIGINT REFERENCES dim_referrer(referrer_key),
    event_ts TIMESTAMP,
    event_hour SMALLINT,
    time_stamp BIGINT,
    current_url VARCHAR,
    referrer_url VARCHAR,
    ip_address VARCHAR,
    collection VARCHAR,
    api_version VARCHAR,
    ingested_at TIMESTAMP NOT NULL
);
```

### 2. Fix HDFS permissions

```bash
docker exec -it hadoop-namenode-1 hdfs dfs -chmod 777 /
```

### 3. Run the Spark streaming pipeline

```bash
docker container stop glamira-streaming 2>/dev/null || true
docker container rm glamira-streaming 2>/dev/null || true

docker run --rm -ti --name glamira-streaming \
--network=streaming-network \
-v ./:/spark \
-v spark_lib:/home/spark/.ivy2 \
-v spark_data:/data \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--deploy-mode client \
--master yarn \
/spark/99-project/kafka_streaming.py"
```

### 4. Run the Streamlit dashboard

```bash
cd 99-project
streamlit run dashboard.py
```

Open browser at `http://localhost:8501`

## Key Technical Decisions

- **foreachBatch** — used instead of direct JDBC write to support multiple sinks and deduplication
- **cache/unpersist** — prevents re-reading Kafka for each dim/fact operation within a batch
- **ON CONFLICT DO NOTHING** — ensures idempotent writes, safe for retries
- **Checkpoint on HDFS** — enables fault-tolerant offset tracking without Kafka consumer groups
- **startingOffsets: latest** — only processes new events, not historical backlog
- **country_code from URL domain** — extracted from current_url (e.g. glamira.fr → FR)
- **referrer_domain** — extracted domain only (e.g. google.com) for clean grouping

## Kafka Configuration
Bootstrap servers: 46.202.167.130:9094,9194,9294
Security:          SASL_PLAINTEXT / PLAIN
Topic:             product_view
