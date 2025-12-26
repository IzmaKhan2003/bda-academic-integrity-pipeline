"""
Archival Job: MongoDB → HDFS
Phase 9 - FIXED: Proper MongoDB authentication
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col
from datetime import datetime, timedelta

print("="*70)
print("ARCHIVAL JOB - Phase 9")
print("="*70)

# FIXED: Proper MongoDB URI format with auth database
MONGO_URI = "mongodb://admin:admin123@mongodb:27017/academic_integrity.exam_attempts?authSource=admin"

# Create Spark session with correct MongoDB config
spark = SparkSession.builder \
    .appName("Archival Job") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n✓ Spark session created")

# Calculate cutoff (48 hours ago)
cutoff_time = datetime.now() - timedelta(hours=48)
cutoff_str = cutoff_time.strftime('%Y-%m-%dT%H:%M:%S')

print(f"\nCutoff time: {cutoff_time}")
print(f"Archiving data older than: {cutoff_str}")

# Read MongoDB data with proper auth
print("\nReading from MongoDB...")

try:
    # Read exam_attempts with auth
    df_attempts = spark.read \
        .format("mongodb") \
        .option("connection.uri", "mongodb://admin:admin123@mongodb:27017/academic_integrity.exam_attempts?authSource=admin") \
        .load()
    
    # Read session_logs with auth
    df_sessions = spark.read \
        .format("mongodb") \
        .option("connection.uri", "mongodb://admin:admin123@mongodb:27017/academic_integrity.session_logs?authSource=admin") \
        .load()
    
    print(f"  Total exam_attempts: {df_attempts.count():,}")
    print(f"  Total session_logs: {df_sessions.count():,}")
    
    # Filter old data
    print(f"\nFiltering data older than {cutoff_str}...")
    
    old_attempts = df_attempts.filter(col("submission_timestamp") < cutoff_str)
    old_sessions = df_sessions.filter(col("session_start") < cutoff_str)
    
    old_attempts_count = old_attempts.count()
    old_sessions_count = old_sessions.count()
    
    print(f"  Old exam_attempts: {old_attempts_count:,}")
    print(f"  Old session_logs: {old_sessions_count:,}")
    
    if old_attempts_count == 0 and old_sessions_count == 0:
        print("\n✓ No data needs archiving (all data is recent)")
        print("  TIP: Data must be older than 48 hours to archive")
        spark.stop()
        exit(0)
    
    # Archive exam_attempts
    if old_attempts_count > 0:
        print(f"\nArchiving {old_attempts_count:,} exam attempts to HDFS...")
        
        # Add partition columns
        df_archive = old_attempts \
            .withColumn("year", year(col("submission_timestamp"))) \
            .withColumn("month", month(col("submission_timestamp"))) \
            .withColumn("day", dayofmonth(col("submission_timestamp")))
        
        # Write to HDFS (Parquet format, partitioned)
        output_path = "/archive/exam_data/exam_attempts"
        
        df_archive.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        print(f"  ✓ Archived to HDFS: {output_path}")
        
        # Delete from MongoDB using PyMongo (which already works)
        print("  Deleting old records from MongoDB...")
        
        # Get list of IDs to delete
        ids_to_delete = old_attempts.select("attempt_id").rdd.flatMap(lambda x: x).collect()
        
        from pymongo import MongoClient
        mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = mongo_client['academic_integrity']
        
        result = db.exam_attempts.delete_many({
            'attempt_id': {'$in': ids_to_delete}
        })
        
        print(f"  ✓ Deleted {result.deleted_count:,} records from MongoDB")
    
    # Archive session_logs
    if old_sessions_count > 0:
        print(f"\nArchiving {old_sessions_count:,} session logs to HDFS...")
        
        df_archive = old_sessions \
            .withColumn("year", year(col("session_start"))) \
            .withColumn("month", month(col("session_start"))) \
            .withColumn("day", dayofmonth(col("session_start")))
        
        output_path = "/archive/exam_data/session_logs"
        
        df_archive.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        print(f"  ✓ Archived to HDFS: {output_path}")
        
        # --- Delete from MongoDB using batches ---
        from pymongo import MongoClient

        BATCH_SIZE = 1000  # Maximum IDs per delete command

        mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = mongo_client['academic_integrity']

        def batch_delete(collection, ids, id_field):
            for i in range(0, len(ids), BATCH_SIZE):
                batch = ids[i:i+BATCH_SIZE]
                result = collection.delete_many({id_field: {'$in': batch}})
                print(f"  ✓ Deleted {result.deleted_count:,} records from {collection.name} (batch {i//BATCH_SIZE + 1})")

        # --- Exam Attempts ---
        ids_to_delete = old_attempts.select("attempt_id").rdd.flatMap(lambda x: x).collect()
        if ids_to_delete:
            print("  Deleting old records from MongoDB (exam_attempts)...")
            batch_delete(db.exam_attempts, ids_to_delete, 'attempt_id')

        # --- Session Logs ---
        ids_to_delete = old_sessions.select("log_id").rdd.flatMap(lambda x: x).collect()
        if ids_to_delete:
            print("  Deleting old records from MongoDB (session_logs)...")
            batch_delete(db.session_logs, ids_to_delete, 'log_id')

    # Final verification
    from pymongo import MongoClient
    mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = mongo_client['academic_integrity']
    stats = db.command('dbstats')
    size_mb = stats['dataSize'] / (1024 * 1024)
    
    print("\n" + "="*70)
    print("ARCHIVAL COMPLETE")
    print("="*70)
    print(f"MongoDB size after cleanup: {size_mb:.2f} MB")
    print(f"Archived location: /archive/exam_data/")
    print("="*70)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
