"""
Archival Job: MongoDB → HDFS
Phase 9 - FINAL: Scoping fix & HDFS destination
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from datetime import datetime, timedelta
from pymongo import MongoClient
import traceback

# --- GLOBAL UTILITIES ---
def batch_delete(collection, ids, id_field):
    """Deletes MongoDB documents in batches to avoid command size limits."""
    BATCH_SIZE = 1000
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i+BATCH_SIZE]
        result = collection.delete_many({id_field: {'$in': batch}})
        print(f"   ✓ Deleted {result.deleted_count:,} records from {collection.name} (batch {i//BATCH_SIZE + 1})")

print("="*70)
print("ARCHIVAL JOB - Phase 9")
print("="*70)

# MongoDB Config
MONGO_URI = "mongodb://admin:admin123@mongodb:27017/academic_integrity?authSource=admin"

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("MongoDB Archival Job").master("local[*]")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("\n✓ Spark session created")

# Calculate cutoff (48 hours ago)
cutoff_time = datetime.now() - timedelta(hours=48)
cutoff_str = cutoff_time.strftime('%Y-%m-%dT%H:%M:%S')

print(f"Cutoff time: {cutoff_time}")
print(f"Archiving data older than: {cutoff_str}")

try:
    # 1. Connect to MongoDB via PyMongo for Deletions
    mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = mongo_client['academic_integrity']

    # 2. Read from MongoDB via Spark
    print("\nReading from MongoDB...")
    
    df_attempts = spark.read.format("mongodb") \
        .option("database", "academic_integrity") \
        .option("collection", "exam_attempts").load()
    
    df_sessions = spark.read.format("mongodb") \
        .option("database", "academic_integrity") \
        .option("collection", "session_logs").load()

    # 3. Filter data
    old_attempts = df_attempts.filter(col("submission_timestamp") < cutoff_str)
    old_sessions = df_sessions.filter(col("session_start") < cutoff_str)
    
    attempts_count = old_attempts.count()
    sessions_count = old_sessions.count()

    print(f"   Old exam_attempts: {attempts_count:,}")
    print(f"   Old session_logs: {sessions_count:,}")

    if attempts_count == 0 and sessions_count == 0:
        print("\n✓ No data needs archiving.")
        spark.stop()
        exit(0)

    # --- ARCHIVE EXAM ATTEMPTS ---
    if attempts_count > 0:
        print(f"\nArchiving {attempts_count:,} attempts to HDFS...")
        df_attempts_arch = old_attempts \
            .withColumn("year", year(col("submission_timestamp"))) \
            .withColumn("month", month(col("submission_timestamp"))) \
            .withColumn("day", dayofmonth(col("submission_timestamp")))
        
        path = "hdfs://namenode:9000/archive/exam_data/exam_attempts"
        df_attempts_arch.write.mode("append").partitionBy("year", "month", "day").parquet(path)
        
        # Delete from Mongo
        ids = old_attempts.select("attempt_id").rdd.flatMap(lambda x: x).collect()
        batch_delete(db.exam_attempts, ids, 'attempt_id')

    # --- ARCHIVE SESSION LOGS ---
    if sessions_count > 0:
        print(f"\nArchiving {sessions_count:,} session logs to HDFS...")
        df_sessions_arch = old_sessions \
            .withColumn("year", year(col("session_start"))) \
            .withColumn("month", month(col("session_start"))) \
            .withColumn("day", dayofmonth(col("session_start")))
        
        path = "hdfs://namenode:9000/archive/exam_data/session_logs"
        df_sessions_arch.write.mode("append").partitionBy("year", "month", "day").parquet(path)
        
        # Delete from Mongo
        ids = old_sessions.select("log_id").rdd.flatMap(lambda x: x).collect()
        batch_delete(db.session_logs, ids, 'log_id')

    # Final Stats
    stats = db.command('dbstats')
    print("\n" + "="*70)
    print("ARCHIVAL COMPLETE")
    print(f"MongoDB size after cleanup: {stats['dataSize'] / (1024*1024):.2f} MB")
    print("="*70)

except Exception as e:
    print(f"\n❌ Error: {e}")
    traceback.print_exc()
finally:
    spark.stop()