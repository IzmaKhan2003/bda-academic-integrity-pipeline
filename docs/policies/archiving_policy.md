# 📁 Data Archiving Policy
## Academic Integrity Detection System

**Version:** 1.0  
**Last Updated:** Week 3, Phase 9  
**Owner:** BDA Project Team

---

## 1. POLICY OVERVIEW

This document defines the data retention, archival, and lifecycle management policies for the Academic Integrity Detection System.

### 1.1 Objectives
- Maintain 48-hour rolling window in hot storage (MongoDB)
- Preserve historical data for compliance and trend analysis
- Optimize storage costs and query performance
- Ensure data integrity during archival process

---

## 2. STORAGE TIERS

### 2.1 Hot Storage (MongoDB)
**Purpose:** Real-time analytics and live dashboard queries

| Attribute | Value |
|-----------|-------|
| **Retention Period** | 48 hours (rolling window) |
| **Storage Format** | BSON documents |
| **Data Collections** | exam_attempts, session_logs |
| **Target Size** | 300-400 MB maintained |
| **Query Latency** | <500ms (with Redis cache) |

**Trigger Conditions for Archival:**
1. Data age exceeds 48 hours, OR
2. MongoDB size exceeds 500 MB, OR
3. Scheduled hourly archival job (via Airflow DAG)

### 2.2 Cold Storage (HDFS)
**Purpose:** Long-term storage for historical trend analysis

| Attribute | Value |
|-----------|-------|
| **Retention Period** | Permanent (10+ years) |
| **Storage Format** | Parquet with Snappy compression |
| **Partitioning** | year/month/day/course_id |
| **Compression Ratio** | ~70% (Snappy) |
| **Query Latency** | 3-10 seconds (acceptable for historical queries) |

---

## 3. ARCHIVAL PROCESS

### 3.1 Workflow
```
┌─────────────┐
│  MongoDB    │ (48-hour window)
│  Hot Data   │
└──────┬──────┘
       │
       │ Hourly Airflow DAG
       │
       ▼
┌─────────────┐
│ Spark Job   │ (Filter old data)
│ Archival    │
└──────┬──────┘
       │
       │ Write Parquet
       │
       ▼
┌─────────────┐
│   HDFS      │ (Permanent storage)
│  Parquet    │
└──────┬──────┘
       │
       │ Register metadata
       │
       ▼
┌─────────────┐
│ Hive        │ (Schema & partitions)
│ Metastore   │
└──────┬──────┘
       │
       │ Delete archived records
       │
       ▼
┌─────────────┐
│  MongoDB    │ (Cleaned, <400 MB)
│  Cleanup    │
└─────────────┘
```

### 3.2 Step-by-Step Process

**Step 1: Identify Old Data**
- Query MongoDB for records where `submission_timestamp < (NOW() - 48 hours)`
- Count records to archive

**Step 2: Extract to HDFS**
- Spark reads old records from MongoDB
- Adds partitioning columns: `year`, `month`, `day`, `partition_key`
- Writes to HDFS in Parquet format with Snappy compression
- Partitions organized as: `/archive/exam_data/{table}/year={YYYY}/month={MM}/day={DD}/partition_key={KEY}/`

**Step 3: Register Metadata**
- Create/update Hive external tables
- Run `MSCK REPAIR TABLE` to discover new partitions
- Log metadata: record count, file size, schema version, timestamps

**Step 4: Verify Archival**
- Query Hive tables to confirm data exists
- Validate record counts match MongoDB extraction

**Step 5: Clean MongoDB**
- Delete archived records using `attempt_id` and `log_id` lists
- Verify MongoDB size reduced

**Step 6: Log Completion**
- Update archival log with success/failure status
- Record metrics: records archived, time taken, final MongoDB size

---

## 4. METADATA MANAGEMENT

### 4.1 Hive Metastore Schema

**exam_attempts_archived**
```sql
CREATE EXTERNAL TABLE exam_attempts_archived (
    attempt_id STRING,
    student_id STRING,
    exam_id STRING,
    question_id STRING,
    response_time DOUBLE,
    is_correct BOOLEAN,
    ai_usage_score DOUBLE,
    answer_hash STRING,
    answer_text STRING,
    submission_timestamp TIMESTAMP,
    keystroke_count INT,
    paste_count INT,
    marks_obtained INT,
    flagged BOOLEAN
)
PARTITIONED BY (year INT, month INT, day INT, partition_key STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/archive/exam_data/exam_attempts';
```

### 4.2 Metadata Files

**Location:** `hdfs://namenode:9000/archive/exam_data/metadata/`

**Format:** JSON

**Example:**
```json
{
  "archival_timestamp": "2025-01-15T14:30:00Z",
  "tables": {
    "exam_attempts_archived": {
      "record_count": 125000,
      "location": "/archive/exam_data/exam_attempts",
      "format": "parquet",
      "compression": "snappy",
      "schema_version": "v1.0",
      "partitions": 42,
      "total_size_mb": 85.3
    },
    "session_logs_archived": {
      "record_count": 25000,
      "location": "/archive/exam_data/session_logs",
      "format": "parquet",
      "compression": "snappy",
      "schema_version": "v1.0",
      "partitions": 42,
      "total_size_mb": 12.1
    }
  },
  "data_quality": {
    "completeness": 0.998,
    "null_count": 125,
    "duplicate_count": 0
  }
}
```

---

## 5. ARCHIVAL SCHEDULE

### 5.1 Automated Schedule
- **Frequency:** Hourly
- **Airflow DAG:** `archival_dag`
- **Cron Expression:** `0 * * * *` (top of every hour)
- **Expected Duration:** 5-15 minutes

### 5.2 Manual Triggers
Archival can be manually triggered in these scenarios:
- MongoDB size exceeds 500 MB
- System maintenance window
- Data migration requirements

**Manual Command:**
```bash
docker exec spark-master spark-submit /spark/jobs/archival/archival_job.py
```

---

## 6. DATA RETRIEVAL FROM ARCHIVE

### 6.1 Query Archived Data via Hive

**Example 1: Historical Trend Analysis**
```sql
SELECT 
    year, month, 
    AVG(ai_usage_score) as avg_ai_score,
    COUNT(*) as total_attempts
FROM exam_attempts_archived
WHERE year = 2024 AND month = 12
GROUP BY year, month;
```

**Example 2: Student Historical Performance**
```sql
SELECT 
    student_id,
    exam_id,
    AVG(response_time) as avg_time,
    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as accuracy
FROM exam_attempts_archived
WHERE student_id = 'STU001234'
GROUP BY student_id, exam_id;
```

### 6.2 Partition Pruning
Always use partition columns in WHERE clause for performance:
```sql
-- Good (uses partition pruning)
SELECT * FROM exam_attempts_archived
WHERE year = 2024 AND month = 12 AND day = 15;

-- Bad (scans all partitions)
SELECT * FROM exam_attempts_archived
WHERE submission_timestamp >= '2024-12-15';
```

---

## 7. DISASTER RECOVERY

### 7.1 Backup Strategy
- **HDFS Replication Factor:** 3 (default)
- **Metadata Backup:** Daily backup of Hive Metastore to S3/Azure Blob
- **Recovery Time Objective (RTO):** 4 hours
- **Recovery Point Objective (RPO):** 1 hour (last successful archival)

### 7.2 Data Restoration Process
1. Identify required data range (year/month/day/partition_key)
2. Query Hive tables to locate Parquet files
3. Read Parquet files using Spark
4. Insert into MongoDB if needed for hot storage

---

## 8. MONITORING & ALERTS

### 8.1 Metrics to Monitor
- MongoDB size (alert if >500 MB)
- Archival job success rate (alert if <95%)
- HDFS available space (alert if <20%)
- Archival job duration (alert if >30 min)
- Data loss detection (compare MongoDB count before/after cleanup)

### 8.2 Alert Configuration
| Metric | Threshold | Action |
|--------|-----------|--------|
| MongoDB size | >500 MB | Trigger manual archival |
| Archival failure | 2 consecutive failures | Page on-call engineer |
| HDFS space | <20% free | Provision more storage |
| Data mismatch | >0.1% discrepancy | Halt cleanup, investigate |

---

## 9. COMPLIANCE & AUDIT

### 9.1 Data Retention Requirements
- **Academic Records:** 7 years (compliance with FERPA)
- **Audit Logs:** 10 years (institutional policy)
- **Research Data:** Indefinite (for academic studies)

### 9.2 Audit Trail
Every archival operation is logged with:
- Timestamp
- Records archived
- User/service initiating archival
- Success/failure status
- Data integrity checksums

**Audit Log Location:** `hdfs://namenode:9000/archive/audit_logs/`

---

## 10. CHANGE HISTORY

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-15 | BDA Team | Initial policy document |

---

## 11. APPENDIX

### 11.1 Parquet Format Benefits
- **Columnar Storage:** Only read required columns
- **Compression:** 70-80% size reduction with Snappy
- **Schema Evolution:** Add/remove columns without rewriting files
- **Splittable:** Parallel processing in Spark

### 11.2 Partition Strategy Rationale
- **year/month/day:** Natural time-based partitioning
- **partition_key (exam_id/course_id):** Enables queries filtering by course
- **Avoids small files:** Each partition contains multiple Parquet files

### 11.3 Alternative Archival Triggers
- **Size-based:** If MongoDB exceeds threshold
- **Time-based:** Daily/weekly/monthly
- **Event-based:** After semester ends

**Current Choice:** Time-based (hourly) for consistency and predictability

---

**END OF ARCHIVING POLICY DOCUMENT**
