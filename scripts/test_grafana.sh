#!/bin/bash

echo "======================================"
echo "TESTING GRAFANA DASHBOARDS"
echo "======================================"

echo -e "\n1. Checking Grafana health..."
curl -s http://localhost:3000/api/health | grep "ok" && echo "✓ Grafana is healthy"

echo -e "\n2. Checking datasources..."
curl -s -u admin:admin123 http://localhost:3000/api/datasources | grep -o '"name":"[^"]*"' | head -5

echo -e "\n3. Checking dashboards..."
curl -s -u admin:admin123 http://localhost:3000/api/search?type=dash-db | grep -o '"title":"[^"]*"'

echo -e "\n4. Verifying data refresh..."
docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
print('Collections:');
print('  aggregated_kpis:', db.aggregated_kpis.count());
print('  high_risk_students:', db.high_risk_students.count());
print('  time_series_kpis:', db.time_series_kpis.count());
" --quiet

echo -e "\n5. Testing Redis cache..."
docker exec redis redis-cli GET kpi:total_events

echo -e "\n======================================"
echo "✓ GRAFANA TEST COMPLETE"
echo "======================================"
echo -e "\nAccess dashboards:"
echo "  URL: http://localhost:3000"
echo "  Username: admin"
echo "  Password: admin123"
