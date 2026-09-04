# 🧬 PGVD - Genomic Data Processing with Apache Spark & Kafka

**Academic Project for Real-Time Genomic Data Processing using Apache Spark, Kafka, and HDFS**

## 📋 Project Description

PGVD is a platform for **real-time distributed processing** of genomic data using:

- **Apache Spark**: Distributed processing of genetic data
- **Apache Kafka**: Real-time data ingestion from producers
- **HDFS**: Distributed dataset storage
- **Flask Dashboard**: Advanced visualization with genetic streaming metrics
- **Docker Compose**: Complete service orchestration

### Key Features

✅ **Genomic Data Processing**
- Family analysis (Father, Mother, Children)
- Genetic variant detection
- Genetic orientation calculation (Dominant/Recessive/Heterozygous)

✅ **Real-Time Streaming**
- Kafka ingestion
- Time windows for analysis
- Genetic anomaly detection

✅ **Advanced Metrics**
- Real-time mutation rate
- Genotype distribution
- Top genes and detected variants
- Genetic diversity
- Mutation trends

✅ **Cluster Monitoring**
- Spark metrics (Masters, Workers, Jobs)
- HDFS status (DataNodes, Storage)
- Executor performance

---

## 🏗️ Project Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     PRODUCER (producer/)                     │
│  - family_generator.py: Generates genetic families          │
│  - producer.py: Sends data to Kafka                         │
│  - streaming_manager.py: Manages data flow                  │
└────────────────┬────────────────────────────────────────────┘
                 │ [KAFKA TOPICS]
┌────────────────▼────────────────────────────────────────────┐
│                  CONSUMER (cosumer/)                         │
│  - spark_consumer.py: Consumes from Kafka                   │
│  - Processes genomic data in Spark                          │
│  - Stores in HDFS                                           │
│  - Sends metrics to Dashboard                               │
└────────────────┬────────────────────────────────────────────┘
                 │ [REST API]
┌────────────────▼────────────────────────────────────────────┐
│           DASHBOARD (cosumer/dashboard/)                     │
│  - dashboard_advanced.py: Flask Backend                     │
│  - dashboard.js: Interactive Charts                         │
│  - index.html: Modern UI                                    │
│  - Real-time Metrics Visualization                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start Guide

### Prerequisites

- **Docker** and **Docker Compose** installed
- Minimum **8GB RAM** for Spark cluster
- Genomic data in `producer/data/archive-2/` (5 CSV files)

### 1️⃣ Prepare Data

Place genomic files in `producer/data/archive-2/`:
```
producer/data/archive-2/
├── Father Genome.csv
├── Mother Genome.csv
├── Child 1 Genome.csv
├── Child 2 Genome.csv
└── Child 3 Genome.csv
```

**Dataset**: [Family Genome Dataset - Kaggle](https://www.kaggle.com/datasets/zusmani/family-genome-dataset)

### 2️⃣ Deploy with Docker

```bash
git clone https://github.com/ALbertE03/PGVD.git

```bash
# Start all services
./start.sh

# Monitor logs in real-time
docker-compose logs -f

# View service status
docker-compose ps
```

### 3️⃣ Access Dashboard

```
http://localhost:5000
```

**Dashboard Navigation:**
- 🖥️ **Cluster Metrics**: Spark, HDFS, and Jobs status
- 🧬 **Genetic Streaming**: Advanced genomic data metrics
- 📊 **Data Analysis**: Processing statistics

### 4️⃣ Stop Services

```bash
./stop.sh
```

---

## 📁 Project Structure

```
PGVD/
├── README.md                          # This file
├── start.sh                           # Startup script
├── stop.sh                            # Shutdown script
│
├── producer/                          # Data ingestion
│   ├── producer.py                    # Sends data to Kafka
│   ├── family_generator.py            # Genomic data generator
│   ├── streaming_manager.py           # Flow manager
│   ├── config.py                      # Configuration
│   ├── requirements.txt               # Python dependencies
│   ├── Dockerfile                     # Docker image
│   ├── docker-compose.yml             # Orchestration
│   └── data/
│       └── archive-2/                 # Genomic data (CSV)
│
├── cosumer/                           # Processing and dashboard
│   ├── spark_consumer.py              # Spark Consumer
│   ├── requirements.txt               # Python dependencies
│   ├── requirements_dashboard.txt     # Dashboard dependencies
│   ├── entrypoint.sh                  # Startup script
│   ├── docker-compose.yml             # Orchestration
│   ├── dockerfile                     # Base image
│   ├── Dockerfile.driver              # Spark Driver
│   ├── Dockerfile.master              # Spark Master
│   ├── Dockerfile.worker              # Spark Worker
│   ├── Dockerfile.dashboard           # Dashboard Flask
│   ├── driver-entrypoint.sh           # Driver startup
│   ├── worker-entrypoint.sh           # Worker startup
│   │
│   ├── dashboard/
│   │   ├── dashboard_advanced.py      # Flask Backend (NEW)
│   │   ├── static/
│   │   │   └── dashboard.js           # Advanced Charts (UPDATED)
│   │   └── templates/
│   │       └── index.html             # Modern UI (UPDATED)
│   │
│   └── models/
│       ├── __init__.py
│       └── data_models.py             # Data models
│
├── informe/
│   └── informe_pgvd.tex               # Academic documentation
│
└── photos/                            # Screenshots
```

---

## 🛠️ Control Scripts

### `start.sh` - Starts all services

```bash
./start.sh
```

**What it does:**
1. Builds Docker images
2. Starts Producer (Kafka, Zookeeper)
3. Starts Consumer (Spark, HDFS, Dashboard)
4. Starts data generator
5. Monitors logs in real-time

### `stop.sh` - Stops all services

```bash
./stop.sh
```

**What it does:**
1. Stops and removes containers
2. Removes volumes (optional)
3. Cleans up Docker resources

---

## 🔌 Services and Ports

### Producer (Port 9092)
- **Kafka Broker**: `kafka:9092`
- **Zookeeper**: `zookeeper:2181`

### Consumer - Spark Cluster
- **Spark Master**: `http://spark-master-1:8080`
- **Spark Worker 1**: `http://spark-worker-1:8081`
- **Spark Worker 2**: `http://spark-worker-2:8082`
- **Spark Worker 3**: `http://spark-worker-3:8083`
- **Spark Driver UI**: `http://localhost:4040`

### Consumer - HDFS
- **NameNode**: `http://namenode:9870`
- **DataNode 1**: `http://datanode-1:9864`
- **DataNode 2**: `http://datanode-2:9864`
- **DataNode 3**: `http://datanode-3:9864`

### Dashboard
- **Flask Dashboard**: `http://localhost:5000`
- **REST API**: `http://localhost:5000/api/*`

---

## 📊 API Endpoints

### Processing Metrics
```
GET /api/stats                    # Count of processed families
GET /api/cluster_stats            # Spark/HDFS cluster status
GET /api/processing_history       # Processing history
GET /api/task_times               # Task completion times
```

### Genetic Metrics (NEW ✨)
```
GET /api/genetic_analysis         # Genetic streaming metrics and analysis
GET /api/families                 # List of processed families
```

### Spark Metrics
```
GET /api/spark_jobs               # Active jobs and executors
```

---

## 📝 How Genetic Streaming Works

### 1. Producer sends data to Kafka

```python
# producer.py
# Sends family data with SNP information
kafka_message = {
    "family_id": "FAM_ABC123",
    "member_type": "father",
    "person_id": "FAM_ABC123_F",
    "gender": "Male",
    "total_snps": 500,
    "snp_data": {
        "chromosome": "17",
        "position": 41196312,
        "genotype": "0/1"
    }
}
```

### 2. Consumer receives and processes

```python
# spark_consumer.py
# 1. Consumes messages from Kafka
# 2. Analyzes genomic data
# 3. Calculates metrics in time windows
# 4. Sends metrics to Dashboard via REST API
# 5. Stores data in HDFS (Parquet format)
```

### 3. Dashboard displays in real-time

- **Mutation Rate**: Variants per second in 60s window
- **Genotype Distribution**: Dominant/Recessive/Heterozygous
- **Top Genes**: 5 most frequent genes
- **Top Variants**: 5 most common variants
- **Anomalies**: Deviations from expected rate

---

## 🔍 Advanced Metrics Explained

### 1️⃣ Mutation Rate
**Formula**: Number of genetic variants / seconds in window

```
Window: 60 seconds
If 45 variants received in 60s → Rate = 0.75 variants/sec
```

### 2️⃣ Genetic Orientation

| Genotype | Orientation | Description |
|----------|-------------|------------|
| 0/0 | Recessive | Two recessive alleles |
| 0/1 | Heterozygous | One allele of each type |
| 1/1 | Dominant | Two dominant alleles |

### 3️⃣ Anomaly Detection

```
Anomaly detected if:
|Current Rate - Expected Rate| > 2.5σ (standard deviations)
```

### 4️⃣ Genetic Diversity

```
Diversity = Number of unique genes / Total variants
Range: 0-1 (1 = maximum diversity)
```

---

## 🐛 Troubleshooting

### ❌ "Connection refused" on Kafka

```bash
# Verify Kafka is running
docker-compose -f producer/docker-compose.yml ps

# Restart
docker-compose -f producer/docker-compose.yml restart kafka zookeeper
```

### ❌ "No space left on device"

```bash
# Clean images and volumes
docker system prune -a --volumes
./start.sh
```

### ❌ Dashboard not loading charts

```bash
# Verify Flask is running
docker-compose -f cosumer/docker-compose.yml ps

# View logs
docker-compose -f cosumer/docker-compose.yml logs dashboard
```

### ❌ Spark without connected workers

```bash
# Restart Spark cluster
docker-compose -f cosumer/docker-compose.yml restart spark-master-1
docker-compose -f cosumer/docker-compose.yml restart spark-worker-1 spark-worker-2 spark-worker-3
```

---

## 📈 Query Examples

### Get all genetic analysis data

```bash
curl http://localhost:5000/api/genetic_analysis
```

**Response:**
```json
{
  "genetic_data": {
    "fathers": [...],
    "mothers": [...],
    "children": [...]
  },
  "genetic_metrics": {
    "mutation_rate_history": [0.023, 0.025, ...],
    "variant_types": {"SNP": 156},
    "gene_frequency": {
      "BRCA1": 23,
      "TP53": 18
    },
    "genotype_distribution": {
      "dominant": 42,
      "recessive": 35,
      "heterozygous": 79
    }
  }
}
```

### Get list of processed families

```bash
curl http://localhost:5000/api/families
```

**Response:**
```json
{
  "families": ["FAM_ABC123", "FAM_DEF456", ...],
  "total": 42
}
```

### Get cluster status

```bash
curl http://localhost:5000/api/cluster_stats
```

---

## 🎯 Justification for Using Streaming

✅ **Why Apache Kafka?**
- Real-time ingestion of genomic data (not batch processing)
- Fault tolerance with replication
- Horizontal scalability

✅ **Why Apache Spark?**
- Distributed processing of time windows
- Efficient aggregation calculation
- Integration with HDFS and Kafka

✅ **Why HDFS?**
- Distributed storage of genomic datasets
- High availability (3x replication)
- Parallel access from Spark

✅ **Why Real-Time Dashboard?**
- Instant monitoring of mutation rate
- Detection of genetic anomalies
- Real-time analysis of trends

---

## 📚 References

- [Apache Spark Documentation](https://spark.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache HDFS Architecture](https://hadoop.apache.org/docs/r3.3.0/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Family Genome Dataset - Kaggle](https://www.kaggle.com/datasets/zusmani/family-genome-dataset)

---
