"""
Hive Metastore Integration
Phase 9 - Step 33: Register metadata for archived data
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json

HDFS_BASE_PATH = 'hdfs://namenode:9000/archive/exam_data'
METASTORE_DB = 'academic_integrity'

print(f"""
{'=' * 70}
HIVE METASTORE WRITER - Phase 9
{'=' * 70}
""")

def create_hive_tables(spark):
    """
    Create external Hive tables pointing to HDFS Parquet files
    """
    print("\n1. Creating Hive database...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {METASTORE_DB}")
    spark.sql(f"USE {METASTORE_DB}")
    print(f"   ✓ Using database: {METASTORE_DB}")
    
    # Create exam_attempts table
    print("\n2. Creating exam_attempts_archived table...")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS exam_attempts_archived (
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
        LOCATION '{HDFS_BASE_PATH}/exam_attempts'
    """)
    print("   ✓ Table created")
    
    # Create session_logs table
    print("\n3. Creating session_logs_archived table...")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS session_logs_archived (
            log_id STRING,
            student_id STRING,
            exam_id STRING,
            tab_switches INT,
            idle_time_seconds DOUBLE,
            focus_loss_count INT,
            copy_count INT,
            screenshot_attempts INT,
            device_type STRING,
            browser STRING,
            ip_address STRING,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            warnings_issued INT
        )
        PARTITIONED BY (year INT, month INT, day INT, partition_key STRING)
        STORED AS PARQUET
        LOCATION '{HDFS_BASE_PATH}/session_logs'
    """)
    print("   ✓ Table created")
    
    # Repair partitions (discover existing partitions)
    print("\n4. Repairing partitions...")
    spark.sql("MSCK REPAIR TABLE exam_attempts_archived")
    spark.sql("MSCK REPAIR TABLE session_logs_archived")
    print("   ✓ Partitions discovered")
    
    # Show partition info
    print("\n5. Partition Summary:")
    partitions = spark.sql("SHOW PARTITIONS exam_attempts_archived").collect()
    print(f"   exam_attempts: {len(partitions)} partitions")
    
    partitions = spark.sql("SHOW PARTITIONS session_logs_archived").collect()
    print(f"   session_logs: {len(partitions)} partitions")

def log_metadata(spark):
    """
    Log metadata about archived data
    """
    print("\n" + "=" * 70)
    print("LOGGING METADATA")
    print("=" * 70)
    
    metadata = {
        'archival_timestamp': datetime.now().isoformat(),
        'tables': {}
    }
    
    # Get stats for exam_attempts
    df_attempts = spark.sql("SELECT COUNT(*) as count FROM exam_attempts_archived")
    attempts_count = df_attempts.collect()[0]['count']
    
    metadata['tables']['exam_attempts_archived'] = {
        'record_count': attempts_count,
        'location': f"{HDFS_BASE_PATH}/exam_attempts",
        'format': 'parquet',
        'compression': 'snappy'
    }
    
    # Get stats for session_logs
    df_logs = spark.sql("SELECT COUNT(*) as count FROM session_logs_archived")
    logs_count = df_logs.collect()[0]['count']
    
    metadata['tables']['session_logs_archived'] = {
        'record_count': logs_count,
        'location': f"{HDFS_BASE_PATH}/session_logs",
        'format': 'parquet',
        'compression': 'snappy'
    }
    
    print(f"\nMetadata Summary:")
    print(json.dumps(metadata, indent=2))
    
    # Save metadata to HDFS
    metadata_path = f"{HDFS_BASE_PATH}/metadata/archival_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Convert to DataFrame and save
    metadata_df = spark.createDataFrame([json.dumps(metadata)], "string")
    metadata_df.write.mode("overwrite").text(metadata_path)
    
    print(f"\n✓ Metadata saved to: {metadata_path}")

def main():
    # Create Spark session with Hive support
    spark = SparkSession.builder \
        .appName("Hive Metastore Writer - Phase 9") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("✓ Spark session with Hive support created")
    
    # Create Hive tables
    create_hive_tables(spark)
    
    # Log metadata
    log_metadata(spark)
    
    print("\n" + "=" * 70)
    print("HIVE METASTORE REGISTRATION COMPLETE")
    print("=" * 70)
    
    spark.stop()

if __name__ == "__main__":
    main()
