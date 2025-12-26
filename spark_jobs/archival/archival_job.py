"""
MongoDB to HDFS Archival Job
Phase 9 - Step 32: Archive data older than 48 hours

Moves data from hot storage (MongoDB) to cold storage (HDFS)
Format: Parquet with Snappy compression
Partitioning: year/month/day/course_id
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pymongo import MongoClient
import sys

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
HDFS_BASE_PATH = 'hdfs://namenode:9000/archive/exam_data'
ARCHIVE_THRESHOLD_HOURS = 48

print(f"""
{'=' * 70}
ARCHIVAL JOB - Phase 9
Threshold: {ARCHIVE_THRESHOLD_HOURS} hours
Target: HDFS {HDFS_BASE_PATH}
{'=' * 70}
""")

def get_cutoff_timestamp():
    """Calculate cutoff timestamp for archival"""
    cutoff = datetime.now() - timedelta(hours=ARCHIVE_THRESHOLD_HOURS)
    print(f"\nCutoff timestamp: {cutoff}")
    return cutoff

def archive_exam_attempts(spark, cutoff_timestamp):
    """
    Archive exam_attempts older than cutoff
    """
    print("\n" + "=" * 70)
    print("ARCHIVING EXAM_ATTEMPTS")
    print("=" * 70)
    
    # Read from MongoDB
    print("\n1. Reading old data from MongoDB...")
    df = spark.read.format("mongodb") \
        .option("spark.mongodb.read.connection.uri", MONGO_URI) \
        .option("spark.mongodb.read.database", "academic_integrity") \
        .option("spark.mongodb.read.collection", "exam_attempts") \
        .load()


    
    total_count = df.count()
    print(f"   Total records in MongoDB: {total_count:,}")
    
    # Filter old records
    df_old = df.filter(col("submission_timestamp") < lit(cutoff_timestamp))
    old_count = df_old.count()
    
    print(f"   Records to archive: {old_count:,}")
    
    if old_count == 0:
        print("   ⚠ No data to archive")
        return 0
    
    # Add partitioning columns
    print("\n2. Adding partition columns...")
    df_partitioned = df_old \
        .withColumn("year", year(col("submission_timestamp"))) \
        .withColumn("month", month(col("submission_timestamp"))) \
        .withColumn("day", dayofmonth(col("submission_timestamp")))
    
    # Extract course_id from exam_id (need to join with exams table)
    # For now, use exam_id as partition
    df_partitioned = df_partitioned.withColumn("partition_key", col("exam_id"))
    
    # Write to HDFS
    print("\n3. Writing to HDFS (Parquet + Snappy)...")
    output_path = f"{HDFS_BASE_PATH}/exam_attempts"
    
    df_partitioned.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "partition_key") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(output_path)
    
    print(f"   ✓ Archived to: {output_path}")
    
    # Return IDs for deletion
    archived_ids = df_old.select("attempt_id").rdd.flatMap(lambda x: x).collect()
    
    print(f"\n4. Archive Summary:")
    print(f"   Records archived: {old_count:,}")
    print(f"   Unique IDs: {len(archived_ids):,}")
    
    return archived_ids

def archive_session_logs(spark, cutoff_timestamp):
    """
    Archive session_logs older than cutoff
    """
    print("\n" + "=" * 70)
    print("ARCHIVING SESSION_LOGS")
    print("=" * 70)
    
    # Read from MongoDB
    print("\n1. Reading old data from MongoDB...")
    df = spark.read.format("mongodb") \
        .option("spark.mongodb.read.connection.uri", MONGO_URI) \
        .option("spark.mongodb.read.database", "academic_integrity") \
        .option("spark.mongodb.read.collection", "session_logs") \
        .load()

    
    total_count = df.count()
    print(f"   Total records in MongoDB: {total_count:,}")
    
    # Filter old records
    df_old = df.filter(col("session_start") < lit(cutoff_timestamp))
    old_count = df_old.count()
    
    print(f"   Records to archive: {old_count:,}")
    
    if old_count == 0:
        print("   ⚠ No data to archive")
        return 0
    
    # Add partitioning columns
    print("\n2. Adding partition columns...")
    df_partitioned = df_old \
        .withColumn("year", year(col("session_start"))) \
        .withColumn("month", month(col("session_start"))) \
        .withColumn("day", dayofmonth(col("session_start"))) \
        .withColumn("partition_key", col("exam_id"))
    
    # Write to HDFS
    print("\n3. Writing to HDFS (Parquet + Snappy)...")
    output_path = f"{HDFS_BASE_PATH}/session_logs"
    
    df_partitioned.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "partition_key") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(output_path)
    
    print(f"   ✓ Archived to: {output_path}")
    
    # Return IDs for deletion
    archived_ids = df_old.select("log_id").rdd.flatMap(lambda x: x).collect()
    
    print(f"\n4. Archive Summary:")
    print(f"   Records archived: {old_count:,}")
    print(f"   Unique IDs: {len(archived_ids):,}")
    
    return archived_ids

def cleanup_mongodb(archived_attempts_ids, archived_logs_ids):
    """
    Delete archived records from MongoDB
    Phase 9 - Step 34
    """
    print("\n" + "=" * 70)
    print("CLEANING UP MONGODB")
    print("=" * 70)
    
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    # Delete exam_attempts
    if archived_attempts_ids:
        print(f"\n1. Deleting {len(archived_attempts_ids):,} exam_attempts...")
        result = db.exam_attempts.delete_many({'attempt_id': {'$in': archived_attempts_ids}})
        print(f"   ✓ Deleted: {result.deleted_count:,}")
    
    # Delete session_logs
    if archived_logs_ids:
        print(f"\n2. Deleting {len(archived_logs_ids):,} session_logs...")
        result = db.session_logs.delete_many({'log_id': {'$in': archived_logs_ids}})
        print(f"   ✓ Deleted: {result.deleted_count:,}")
    
    # Show remaining size
    stats = db.command('dbstats')
    size_mb = stats['dataSize'] / (1024 * 1024)
    print(f"\n3. MongoDB Size After Cleanup: {size_mb:.2f} MB")
    
    print("\n" + "=" * 70)

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Archival Job - Phase 9") \
        .config("spark.mongodb.input.uri", f"{MONGO_URI}academic_integrity") \
        .config("spark.mongodb.output.uri", f"{MONGO_URI}academic_integrity") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    print("\n✓ Spark session created")
    
    # Get cutoff timestamp
    cutoff = get_cutoff_timestamp()
    
    # Archive exam_attempts
    attempts_ids = archive_exam_attempts(spark, cutoff)
    
    # Archive session_logs
    logs_ids = archive_session_logs(spark, cutoff)
    
    # Cleanup MongoDB
    if attempts_ids or logs_ids:
        cleanup_mongodb(attempts_ids, logs_ids)
    else:
        print("\n⚠ No data archived, skipping cleanup")
    
    # Final summary
    print("\n" + "=" * 70)
    print("ARCHIVAL JOB COMPLETE")
    print("=" * 70)
    print(f"Exam attempts archived: {len(attempts_ids) if attempts_ids else 0:,}")
    print(f"Session logs archived: {len(logs_ids) if logs_ids else 0:,}")
    print("=" * 70)
    
    spark.stop()

if __name__ == "__main__":
    main()
