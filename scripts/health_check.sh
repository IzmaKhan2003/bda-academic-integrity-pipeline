#!/bin/bash

# ============================================
# Pipeline Health Check Script
# Phase 12 - Step 51: Check all services
# ============================================

echo "======================================"
echo "PIPELINE HEALTH CHECK - Phase 12"
echo "======================================"
echo ""

PASSED=0
FAILED=0

# Function to check service
check_service() {
    local name=$1
    local command=$2
    
    echo -n "Checking $name... "
    
    if eval $command > /dev/null 2>&1; then
        echo "✓ PASS"
        ((PASSED++))
        return 0
    else
        echo "✗ FAIL"
        ((FAILED++))
        return 1
    fi
}

# 1. Check Kafka
echo "1. Kafka Broker"
check_service "Kafka responding" \
    "docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092"

echo ""

# 2. Check MongoDB
echo "2. MongoDB"
check_service "MongoDB accepting connections" \
    "docker exec mongodb mongosh -u admin -p admin123 --quiet --eval 'db.adminCommand({ping: 1})'"

check_service "MongoDB has data" \
    "docker exec mongodb mongosh -u admin -p admin123 --quiet --eval 'use academic_integrity; db.exam_attempts.count()' | grep -v '^[0-9]*$' || [ $(docker exec mongodb mongosh -u admin -p admin123 --quiet --eval 'use academic_integrity; db.exam_attempts.count()') -gt 0 ]"

echo ""

# 3. Check Spark
echo "3. Apache Spark"
check_service "Spark Master active" \
    "docker exec spark-master curl -s http://localhost:8080 | grep -q 'Spark Master'"

check_service "Spark Worker registered" \
    "docker exec spark-master curl -s http://localhost:8080 | grep -q 'Workers (1)'"

echo ""

# 4. Check Airflow
echo "4. Apache Airflow"
check_service "Airflow Webserver running" \
    "docker exec airflow-webserver curl -s http://localhost:8080/health | grep -q 'healthy'"

check_service "Airflow Scheduler running" \
    "docker exec airflow-scheduler pgrep -f 'airflow scheduler'"

echo ""

# 5. Check HDFS
echo "5. Hadoop HDFS"
check_service "HDFS NameNode accessible" \
    "docker exec namenode curl -s http://localhost:9870 | grep -q 'NameNode'"

check_service "HDFS DataNode connected" \
    "docker exec namenode hdfs dfsadmin -report | grep -q 'Live datanodes'"

echo ""

# 6. Check Redis
echo "6. Redis Cache"
check_service "Redis responding to ping" \
    "docker exec redis redis-cli ping | grep -q PONG"

check_service "Redis has cached data" \
    "[ $(docker exec redis redis-cli DBSIZE | grep -o '[0-9]*') -gt 0 ]"

echo ""

# 7. Check Superset
echo "7. Apache Superset"
check_service "Superset responding" \
    "docker exec superset curl -s http://localhost:8088/health | grep -q 'OK' || docker exec superset pgrep -f superset"

echo ""

# Summary
echo "======================================"
echo "HEALTH CHECK SUMMARY"
echo "======================================"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "✓ ALL CHECKS PASSED"
    echo "======================================"
    exit 0
else
    echo "✗ SOME CHECKS FAILED"
    echo "======================================"
    exit 1
fi
