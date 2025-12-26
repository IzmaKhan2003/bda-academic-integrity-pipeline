# Real-Time Academic Integrity Detection System

## BDA Project - Complete Implementation

A comprehensive Big Data Analytics system for detecting academic integrity violations in real-time, featuring AI usage detection, collusion analysis, and behavioral monitoring.

## 🏗️ Architecture Overview

### Infrastructure (13 Docker Containers)
- **Zookeeper & Kafka**: Message streaming with 3 topics (exam_attempts, session_logs, student_events)
- **MongoDB**: Hot data storage with 6 collections (students, courses, exams, questions, exam_attempts, session_logs)
- **Redis**: High-performance caching layer for KPIs
- **Spark**: Distributed processing engine for analytics
- **Airflow**: Workflow orchestration with 4 automated DAGs
- **HDFS**: Long-term archival storage
- **Grafana**: Real-time dashboards with 4 comprehensive views
- **PostgreSQL**: Airflow metadata backend

### Data Pipeline
```
Real-Time Generation → Kafka → MongoDB → Spark Analytics → Redis Cache → Grafana
                                      ↓
                                   HDFS Archival (48h+ old data)
```

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (8GB+ RAM, 4+ CPUs, 50GB disk)
- Git
- Python 3.11+

### Installation & Setup

```bash
# Clone repository
git clone <repository-url>
cd bda-academic-integrity-pipeline

# Start all services (13 containers)
docker-compose up -d

# Wait 5-7 minutes for services to initialize
# Check status
docker ps

# Create Kafka topics
./scripts/create_kafka_topics.sh

# Initialize MongoDB collections
docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
db.createCollection('students');
db.createCollection('courses');
db.createCollection('exams');
db.createCollection('questions');
db.createCollection('exam_attempts');
db.createCollection('session_logs');
"
```

### Test the Pipeline

```bash
# Test data generation (run in background)
docker exec -d spark-master python3 /spark/jobs/generators/simple_data_generator.py

# Test data consumption
docker exec -d spark-master python3 /spark/jobs/kafka_to_mongo_consumer.py

# Wait 2-3 minutes, then check MongoDB
docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
print('exam_attempts:', db.exam_attempts.count());
print('session_logs:', db.session_logs.count());
"
```

## 📊 System Components

### 1. Data Generation Layer
- **Student Profiles**: Normal students, AI cheaters, colluding students
- **Statistical Models**: Log-normal response times, Poisson event distributions
- **Real-time Rate**: 5000+ events/minute across concurrent exams

### 2. Detection Algorithms
- **AI Usage Detection**: Machine learning models scoring 0.0-1.0
- **Collusion Detection**: Answer hash similarity analysis
- **Behavioral Analysis**: Session monitoring (tab switches, idle time, focus loss)

### 3. Analytics Engine
- **7 Key Performance Indicators**:
  - Average response time
  - Accuracy rate
  - Response variance
  - AI usage score
  - Answer similarity index
  - Suspicion confidence
  - Session irregularity score

### 4. Storage Architecture
- **Hot Data**: MongoDB (real-time queries, last 48 hours)
- **Archive**: HDFS Parquet files (partitioned by date)
- **Cache**: Redis (60-second TTL KPIs)

## 🎯 Access Points

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Airflow** | http://localhost:8081 | admin/admin | Workflow orchestration |
| **Grafana** | http://localhost:3000 | admin/admin123 | Real-time dashboards |
| **Spark Master** | http://localhost:8080 | - | Processing cluster UI |
| **HDFS NameNode** | http://localhost:9870 | - | File system browser |
| **Superset** | http://localhost:8088 | admin/admin123 | BI dashboards |
| **MongoDB** | localhost:27017 | admin/admin123 | Database access |
| **Redis** | localhost:6379 | - | Cache inspection |

## 📈 Grafana Dashboards

### 1. Academic Integrity Overview
- Total events processed (real-time counter)
- Average response time trends
- High-risk student counts
- Risk level distribution (pie chart)
- AI usage trends over time
- Top 10 suspicious students table

### 2. AI Usage Heat Map
- AI usage by program and risk level
- AI score distribution histogram
- Response time vs AI score scatter plot

### 3. Collusion Detection Network
- Total collusion cases
- High similarity pairs count
- Average matching questions
- Top 20 collusion pairs table
- Collusion by exam breakdown
- Time difference distribution

### 4. Session Behavior Analysis
- Average tab switches, focus loss, idle time
- Device type and browser distributions
- Tab switches over time trends
- High-risk sessions table

## 🔄 Automated Workflows (Airflow DAGs)

### 1. realtime_generation_dag.py
- **Schedule**: Every 60 seconds
- **Tasks**: Kafka health check → Data generation (50s) → Rate monitoring

### 2. analytics_processing_dag.py
- **Schedule**: Every 60 seconds
- **Tasks**: MongoDB check → KPI calculations → Redis cache update

### 3. data_archival_dag.py
- **Schedule**: Every hour
- **Tasks**: Eligibility check → HDFS archival → Metadata update

### 4. cache_refresh_dag.py
- **Schedule**: Every 60 seconds
- **Tasks**: Expire stale keys → Verify cache health

### 5. dashboard_refresh_dag.py
- **Schedule**: Every 60 seconds
- **Tasks**: Refresh aggregated collections → Update time-series → Detect collusion

## 📊 Data Schema

### Dimension Tables
- **students**: 5000 records (student_id, name, program, skill_level, region)
- **courses**: 50 records (course_id, name, difficulty, department)
- **exams**: 100 records (exam_id, course_id, date, duration, proctored)
- **questions**: 5000 records (question_id, exam_id, difficulty, type, marks)

### Fact Tables
- **exam_attempts**: Main fact table (response_time, ai_usage_score, answer_hash, keystrokes)
- **session_logs**: Behavioral data (tab_switches, idle_time, focus_loss, device_type)

### KPIs & Analytics
- Real-time aggregation every 60 seconds
- 48-hour rolling window for recent data
- Automatic archival to HDFS for older data
- Redis caching for dashboard performance

## 🛠️ Development & Testing

### Project Structure
```
bda-academic-integrity-pipeline/
├── docker-compose.yml          # 13-service infrastructure
├── dags/                       # Airflow workflows (5 DAGs)
├── spark_jobs/                 # PySpark processing jobs
│   ├── generators/            # Data generation
│   ├── analytics/             # KPI calculations
│   ├── models/                # Student behavior profiles
│   └── utils/                 # Statistical distributions
├── config/grafana/            # Dashboard configurations
├── scripts/                   # Setup and utility scripts
├── data/                      # Local data directories
└── docs/                      # Documentation and schemas
```

### Key Scripts
- `scripts/create_kafka_topics.sh`: Initialize Kafka topics
- `scripts/test_pipeline.sh`: End-to-end pipeline testing
- `scripts/test_airflow_dags.sh`: DAG execution verification
- `scripts/test_grafana.sh`: Dashboard functionality check
- `scripts/prepare_grafana_data.py`: Dashboard data preparation

### Testing Commands
```bash
# Full system health check
docker ps | grep -c "Up"  # Should be 13

# Data pipeline test
./scripts/test_pipeline.sh

# Airflow DAGs test
./scripts/test_airflow_dags.sh

# Dashboard test
./scripts/test_grafana.sh

# Performance benchmark
docker exec spark-master python3 /spark/jobs/performance_benchmark.py
```

## 📈 Performance Metrics

### Data Volume Targets
- **MongoDB**: 300MB+ (exam_attempts + session_logs)
- **Generation Rate**: 5000+ events/minute
- **Archival**: Automatic after 48 hours
- **Cache Hit Rate**: 95%+ (60s TTL)

### System Performance
- **End-to-End Latency**: <1000ms
- **MongoDB Read**: <100ms P95
- **Redis Cache**: <5ms average
- **Spark Processing**: <500ms P95 for aggregations

## 👥 Team Information

**Project Team:**
- Izma Khan - [Roll Number]

**Course:** Big Data Analytics (BDA)
**Semester:** [Current Semester]
**Submission Date:** [Date]

## 📚 Documentation

- `docs/data_dictionary.md`: Complete field definitions and relationships
- `docs/complete_schema.json`: Database schema specification
- `docs/superset_dashboard_setup.md`: BI dashboard configuration
- `mds/`: Phase-wise implementation logs (Phases 3-13)

## 🔧 Troubleshooting

### Common Issues

**Containers not starting:**
```bash
# Check Docker resources
docker system info

# Restart with clean state
docker-compose down -v
docker-compose up -d
```

**Kafka connection errors:**
```bash
# Recreate topics
./scripts/create_kafka_topics.sh

# Check Kafka logs
docker logs kafka
```

**MongoDB connection issues:**
```bash
# Reset MongoDB
docker-compose restart mongodb

# Check MongoDB logs
docker logs mongodb
```

**Airflow DAGs not running:**
```bash
# Unpause DAGs in UI
# Or via command:
docker exec airflow-webserver airflow dags unpause <dag_id>
```

### Logs and Monitoring
```bash
# Service logs
docker logs <container_name>

# Airflow logs
docker exec airflow-webserver airflow dags list-runs <dag_id>

# Spark jobs
docker exec spark-master ls /spark/logs/
```

## 🎯 Project Milestones Achieved

✅ **Phase 1-2**: Infrastructure setup (13 containers, Kafka topics, basic pipeline)  
✅ **Phase 3**: Database schema (6 collections, 82 fields, 43 indexes)  
✅ **Phase 4-5**: Statistical data generation (5000 students, 250MB+ data)  
✅ **Phase 6-7**: Real-time streaming (5000 events/min, Spark analytics)  
✅ **Phase 8**: Redis caching layer (60s TTL KPIs)  
✅ **Phase 9**: HDFS archival (48h+ data, partitioned Parquet)  
✅ **Phase 10**: Airflow orchestration (5 automated DAGs)  
✅ **Phase 11**: Grafana dashboards (4 comprehensive views)  
✅ **Phase 12**: Advanced analytics (collusion detection, session behavior)  
✅ **Phase 13**: Documentation & testing (complete system validation)

## 🚀 Future Enhancements

- Machine learning model integration for improved AI detection
- Real-time alerting system for high-risk students
- Advanced collusion network analysis
- Mobile app integration for proctored exams
- Multi-tenant architecture for multiple institutions

---

**Status**: ✅ FULLY IMPLEMENTED AND TESTED  
**Ready for**: Demo and evaluation


