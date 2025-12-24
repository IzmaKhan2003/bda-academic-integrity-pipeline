# Real-Time Academic Integrity & AI-Assisted Cheating Detection

## Big Data Analytics Project

### Izma Khan 

### Project Overview
Real-time streaming analytics pipeline for detecting academic misconduct during online exams using behavioral analysis and AI pattern detection.

### Quick Start

#### Prerequisites
- Docker Desktop (16GB RAM, 50GB disk)
- Python 3.11+
- Git

#### Installation

1. Clone repository:
```bash
git clone <your-repo-url>
cd academic-integrity-bda
```

2. Start all services:
```bash
docker-compose up -d
```

3. Wait 3-5 minutes for services to initialize

4. Access services:
- Airflow UI: http://localhost:8081 (admin/admin)
- Superset: http://localhost:8088 (admin/admin123)
- Spark Master: http://localhost:8080
- HDFS NameNode: http://localhost:9870

#### Verify Installation
```bash
# Check all containers running
docker ps

# Should show 13+ containers
```

### Project Structure
```
academic-integrity-bda/
├── dags/              # Airflow DAGs
├── spark_jobs/        # Spark processing scripts
├── config/            # Configuration files
├── scripts/           # Utility scripts
├── dashboards/        # BI dashboards
└── docs/              # Documentation
```

### Architecture
[Architecture diagram to be added]

### Documentation
- [Data Dictionary](docs/data_dictionary.md)
- [Archiving Policy](docs/archiving_policy.md)
- [Setup Guide](docs/SETUP.md)


