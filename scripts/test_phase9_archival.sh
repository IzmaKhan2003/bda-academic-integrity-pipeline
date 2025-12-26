#!/bin/bash

echo "======================================"
echo "TESTING ARCHIVAL PIPELINE - Phase 9"
echo "======================================"
echo ""

# Step 0: Ensure Spark worker is connected
echo "0. Checking Spark Cluster..."
docker logs spark-worker 2>&1 | grep -i "successfully registered" | tail -1
echo ""

# Step 1: Check current MongoDB size
echo "1. Current MongoDB Size:"
docker exec mongodb mongosh -u admin -p admin123 --quiet --eval "
use academic_integrity;
var stats = db.stats();
print('  Data Size: ' + (stats.dataSize / 1024 / 1024).toFixed(2) + ' MB');
print('  exam_attempts: ' + db.exam_attempts.count());
print('  session_logs: ' + db.session_logs.count());
"

echo ""

# Step 2: Copy updated archival job
echo "2. Deploying updated archival job..."
docker cp spark_jobs/archival/archival_job.py spark-master:/spark/jobs/archival/
docker cp spark_jobs/archival/hive_metastore_writer.py spark-master:/spark/jobs/archival/
echo "  ✓ Files copied"

echo ""

# Step 3: Run archival job
echo "3. Running Archival Job..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /spark/jars/mongo-spark-connector_2.12-10.1.1.jar,/spark/jars/mongodb-driver-sync-4.10.2.jar,/spark/jars/mongodb-driver-core-4.10.2.jar,/spark/jars/bson-4.10.2.jar \
  /spark/jobs/archival/archival_job.py

echo ""

# Step 4: Register metadata (simplified, no Hive/Derby)
echo "4. Registering Archive Metadata..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /spark/jobs/archival/hive_metastore_writer.py

echo ""

# Step 5: Verify HDFS archive
echo "5. Verifying HDFS Archive:"
docker exec namenode hdfs dfs -ls -R /archive/exam_data 2>/dev/null | head -20 || \
echo "  (No archived data yet - data must be >48 hours old)"

echo ""

# Step 6: Check metadata file
echo "6. Archive Metadata:"
docker exec spark-master cat /data/archive_metadata.json 2>/dev/null || \
echo "  (Metadata will be created after first archival)"

echo ""

# Step 7: MongoDB size after archival
echo "7. MongoDB Size After Archival:"
docker exec mongodb mongosh -u admin -p admin123 --quiet --eval "
use academic_integrity;
var stats = db.stats();
print('  Data Size: ' + (stats.dataSize / 1024 / 1024).toFixed(2) + ' MB');
print('  exam_attempts: ' + db.exam_attempts.count());
print('  session_logs: ' + db.session_logs.count());
"

echo ""
echo "======================================"
echo "✓ ARCHIVAL TEST COMPLETE"
echo "======================================"
