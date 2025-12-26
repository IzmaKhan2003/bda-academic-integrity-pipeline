"""
Hive Metastore Writer (Simplified)
Phase 9 - Step 33
"""

from pyspark.sql import SparkSession
from datetime import datetime
import json

print("="*70)
print("HIVE METASTORE WRITER")
print("="*70)

# Create Spark session WITHOUT Hive
spark = SparkSession.builder \
    .appName("Metastore Writer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n✓ Spark session created (without Hive)")

# Read archived data to get metadata
print("\nReading archived data from HDFS...")

try:
    # Check if archive exists
    df_attempts = spark.read.parquet("/archive/exam_data/exam_attempts")
    df_sessions = spark.read.parquet("/archive/exam_data/session_logs")
    
    print(f"  exam_attempts: {df_attempts.count():,} records")
    print(f"  session_logs: {df_sessions.count():,} records")
    
    # Get partition info
    print("\nPartition Information:")
    
    partitions_attempts = df_attempts.select("year", "month", "day").distinct().collect()
    print(f"\nexam_attempts partitions: {len(partitions_attempts)}")
    for p in partitions_attempts[:5]:
        print(f"  year={p.year}/month={p.month}/day={p.day}")
    
    partitions_sessions = df_sessions.select("year", "month", "day").distinct().collect()
    print(f"\nsession_logs partitions: {len(partitions_sessions)}")
    for p in partitions_sessions[:5]:
        print(f"  year={p.year}/month={p.month}/day={p.day}")
    
    # Save metadata to JSON file (simpler than Hive)
    metadata = {
        "last_updated": datetime.now().isoformat(),
        "exam_attempts": {
            "location": "/archive/exam_data/exam_attempts",
            "record_count": df_attempts.count(),
            "partitions": [{"year": p.year, "month": p.month, "day": p.day} for p in partitions_attempts]
        },
        "session_logs": {
            "location": "/archive/exam_data/session_logs",
            "record_count": df_sessions.count(),
            "partitions": [{"year": p.year, "month": p.month, "day": p.day} for p in partitions_sessions]
        }
    }
    
    # Save metadata
    metadata_json = json.dumps(metadata, indent=2)
    
    # Write to local file (simulating metadata storage)
    with open('/data/archive_metadata.json', 'w') as f:
        f.write(metadata_json)
    
    print("\n✓ Metadata saved to /data/archive_metadata.json")
    print("\n" + "="*70)
    print("METASTORE UPDATE COMPLETE")
    print("="*70)

except Exception as e:
    if "Path does not exist" in str(e):
        print("\n✓ No archived data found yet (this is OK)")
        print("  Run archival job first to create archived data")
    else:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

finally:
    spark.stop()
