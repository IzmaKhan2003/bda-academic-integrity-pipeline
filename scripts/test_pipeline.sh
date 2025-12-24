#!/bin/bash

echo "======================================"
echo "WEEK 1 - PIPELINE INTEGRATION TEST"
echo "======================================"

echo -e "\n1. Checking Docker containers..."
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|mongodb|spark-master|airflow"

echo -e "\n2. Checking Kafka topics..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo -e "\n3. Testing data generation (30 seconds)..."
docker exec spark-master timeout 30 python3 /opt/spark-jobs/generators/simple_data_generator.py &
GENERATOR_PID=$!

echo -e "\n4. Starting consumer..."
docker exec -d spark-master python3 /opt/spark-jobs/kafka_to_mongo_consumer.py

sleep 35  # Wait for generation to complete

echo -e "\n5. Checking MongoDB data..."
docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
print('=================================');
print('MongoDB Data Statistics:');
print('=================================');
print('Exam Attempts:', db.exam_attempts.count());
print('Session Logs:', db.session_logs.count());
var stats = db.stats();
print('Database Size:', (stats.dataSize / 1024 / 1024).toFixed(2), 'MB');
print('=================================');
" --quiet

echo -e "\n6. Testing Airflow DAG..."
curl -s http://localhost:8081/health | grep -q "healthy" && echo "✓ Airflow is healthy" || echo "✗ Airflow is not responding"

echo -e "\n======================================"
echo "WEEK 1 TEST COMPLETE"
echo "======================================"
