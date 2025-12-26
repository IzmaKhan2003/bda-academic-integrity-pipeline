#!/bin/bash

# System Monitoring Script
# Displays real-time system metrics

while true; do
    clear
    echo "======================================"
    echo "SYSTEM MONITORING DASHBOARD"
    echo "$(date '+%Y-%m-%d %H:%M:%S')"
    echo "======================================"
    
    echo -e "\n�� CONTAINER STATUS"
    echo "--------------------------------------"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | head -14
    
    echo -e "\n💾 MONGODB METRICS"
    echo "--------------------------------------"
    docker exec mongodb mongosh -u admin -p admin123 --eval "
    use academic_integrity;
    var stats = db.stats();
    print('Size:', (stats.dataSize/1024/1024).toFixed(2), 'MB');
    print('exam_attempts:', db.exam_attempts.count());
    print('session_logs:', db.session_logs.count());
    print('aggregated_kpis:', db.aggregated_kpis.count());
    " --quiet 2>/dev/null
    
    echo -e "\n🔴 REDIS METRICS"
    echo "--------------------------------------"
    docker exec redis redis-cli INFO memory | grep "used_memory_human"
    docker exec redis redis-cli DBSIZE
    
    echo -e "\n📈 KAFKA METRICS"
    echo "--------------------------------------"
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 \
      --topic exam_attempts \
      --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print "Total Messages:", sum}'
    
    echo -e "\n✈️  AIRFLOW STATUS"
    echo "--------------------------------------"
    curl -s http://localhost:8081/health 2>/dev/null | grep -o '"scheduler":{"status":"[^"]*"' | cut -d'"' -f5 | xargs echo "Scheduler:"
    curl -s http://localhost:8081/health 2>/dev/null | grep -o '"metadatabase":{"status":"[^"]*"' | cut -d'"' -f5 | xargs echo "Database:"
    
    echo -e "\n🎨 GRAFANA STATUS"
    echo "--------------------------------------"
    curl -s http://localhost:3000/api/health 2>/dev/null | grep -o '"database":"[^"]*"' | cut -d'"' -f4 | xargs echo "Database:"
    
    echo -e "\nPress Ctrl+C to exit. Refreshing in 10 seconds..."
    sleep 10
done
