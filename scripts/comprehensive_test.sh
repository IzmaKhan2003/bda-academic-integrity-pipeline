#!/bin/bash

echo "======================================"
echo "COMPREHENSIVE SYSTEM TEST"
echo "======================================"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results
PASSED=0
FAILED=0

test_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC}: $2"
        ((PASSED++))
    else
        echo -e "${RED}✗ FAIL${NC}: $2"
        ((FAILED++))
    fi
}

echo -e "\n${YELLOW}TEST 1: Docker Containers${NC}"
echo "-----------------------------------"
REQUIRED_CONTAINERS=("mongodb" "kafka" "redis" "spark-master" "airflow-webserver" "grafana")
for container in "${REQUIRED_CONTAINERS[@]}"; do
    docker ps | grep -q $container
    test_result $? "$container is running"
done

echo -e "\n${YELLOW}TEST 2: Service Health Checks${NC}"
echo "-----------------------------------"

# MongoDB
docker exec mongodb mongosh -u admin -p admin123 --eval "db.adminCommand('ping')" > /dev/null 2>&1
test_result $? "MongoDB is responsive"

# Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1
test_result $? "Kafka is responsive"

# Redis
docker exec redis redis-cli ping | grep -q PONG
test_result $? "Redis is responsive"

# Airflow
curl -s http://localhost:8081/health | grep -q "healthy"
test_result $? "Airflow is healthy"

# Grafana
curl -s http://localhost:3000/api/health | grep -q "ok"
test_result $? "Grafana is healthy"

echo -e "\n${YELLOW}TEST 3: Data Volume Check${NC}"
echo "-----------------------------------"

# MongoDB data size
SIZE=$(docker exec mongodb mongosh -u admin -p admin123 --quiet --eval "use academic_integrity; print(db.stats().dataSize);")
SIZE=$(echo $SIZE | tail -n1)  # Keep only the last line (the number)


if [ "$SIZE" -gt 314572800 ]; then  # 300 MB in bytes
    test_result 0 "MongoDB size >= 300 MB ($(echo "scale=2; $SIZE/1024/1024" | bc) MB)"
else
    test_result 1 "MongoDB size < 300 MB ($(echo "scale=2; $SIZE/1024/1024" | bc) MB)"
fi

# Check record counts
EXAM_COUNT=$(docker exec mongodb mongosh -u admin -p admin123 --quiet --eval "use academic_integrity; print(db.exam_attempts.count());")
EXAM_COUNT=$(echo $EXAM_COUNT | tail -n1)

SESSION_COUNT=$(docker exec mongodb mongosh -u admin -p admin123 --quiet --eval "use academic_integrity; print(db.session_logs.count());")
SESSION_COUNT=$(echo $SESSION_COUNT | tail -n1)

test_result $([ "$EXAM_COUNT" -gt 10000 ] && echo 0 || echo 1) "Exam attempts > 10,000 ($EXAM_COUNT)"
test_result $([ "$SESSION_COUNT" -gt 1000 ] && echo 0 || echo 1) "Session logs > 1,000 ($SESSION_COUNT)"

echo -e "\n${YELLOW}TEST 4: Real-Time Pipeline${NC}"
echo "-----------------------------------"

# Check if data is being generated (last 5 minutes)
RECENT=$(docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
var fiveMinAgo = new Date(Date.now() - 5*60*1000);
print(db.exam_attempts.count({submission_timestamp: {\$gte: fiveMinAgo}}));
" --quiet)

test_result $([ "$RECENT" -gt 100 ] && echo 0 || echo 1) "Recent data generation active ($RECENT events in 5min)"

echo -e "\n${YELLOW}TEST 5: Kafka Topics${NC}"
echo "-----------------------------------"

TOPICS=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092)
echo "$TOPICS" | grep -q "exam_attempts"
test_result $? "exam_attempts topic exists"
echo "$TOPICS" | grep -q "session_logs"
test_result $? "session_logs topic exists"

echo -e "\n${YELLOW}TEST 6: MongoDB Collections${NC}"
echo "-----------------------------------"

COLLECTIONS=$(docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
db.getCollectionNames().forEach(c => print(c));
" --quiet)

for coll in "students" "courses" "exams" "questions" "exam_attempts" "session_logs" "aggregated_kpis"; do
    echo "$COLLECTIONS" | grep -q "^$coll\$"
    test_result $? "$coll collection exists"
done

echo -e "\n${YELLOW}TEST 7: Redis Cache${NC}"
echo "-----------------------------------"

# Check if cache keys exist
CACHE_KEYS=$(docker exec redis redis-cli KEYS "kpi:*" | wc -l)
test_result $([ "$CACHE_KEYS" -gt 5 ] && echo 0 || echo 1) "Redis cache has active keys ($CACHE_KEYS keys)"

echo -e "\n${YELLOW}TEST 8: Airflow DAGs${NC}"
echo "-----------------------------------"

DAGS=$(docker exec airflow-webserver airflow dags list 2>/dev/null)
echo "$DAGS" | grep -q "realtime_data_generation"
test_result $? "realtime_data_generation DAG exists"
echo "$DAGS" | grep -q "analytics_processing"
test_result $? "analytics_processing DAG exists"
echo "$DAGS" | grep -q "data_archival"
test_result $? "data_archival DAG exists"
echo "$DAGS" | grep -q "dashboard_data_refresh"
test_result $? "dashboard_data_refresh DAG exists"

echo -e "\n${YELLOW}TEST 9: HDFS Storage${NC}"
echo "-----------------------------------"

# Check if HDFS is accessible
docker exec namenode hdfs dfs -ls / > /dev/null 2>&1
test_result $? "HDFS is accessible"

# Check archive directory
docker exec namenode hdfs dfs -test -d /archive
test_result $? "/archive directory exists in HDFS"

echo -e "\n${YELLOW}TEST 10: Grafana Dashboards${NC}"
echo "-----------------------------------"

DASHBOARDS=$(curl -s -u admin:admin123 http://localhost:3000/api/search?type=dash-db)
echo "$DASHBOARDS" | grep -q "Academic Integrity"
test_result $? "Academic Integrity dashboard exists"

# ============================================
# TEST SUMMARY
# ============================================

echo ""
echo "======================================"
echo "TEST SUMMARY"
echo "======================================"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"

if [ $FAILED -eq 0 ]; then
    echo -e "\n${GREEN}✓ ALL TESTS PASSED${NC}"
    exit 0
else
    echo -e "\n${RED}✗ SOME TESTS FAILED${NC}"
    exit 1
fi
