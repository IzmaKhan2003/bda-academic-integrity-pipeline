#!/bin/bash

echo "======================================"
echo "COMPLETING PHASES 10-11"
echo "======================================"
echo ""

# Phase 10: Copy DAGs
echo "Phase 10: Deploying Airflow DAGs..."
for dag in realtime_generation_dag.py analytics_processing_dag.py archival_dag.py cache_refresh_dag.py; do
    docker cp dags/$dag airflow-webserver:/opt/airflow/dags/
    docker cp dags/$dag airflow-scheduler:/opt/airflow/dags/
done

sleep 30

# Enable DAGs
echo "Enabling DAGs..."
docker exec airflow-webserver airflow dags unpause realtime_data_generation
docker exec airflow-webserver airflow dags unpause analytics_processing
docker exec airflow-webserver airflow dags unpause data_archival
docker exec airflow-webserver airflow dags unpause cache_refresh

echo "✓ Phase 10 Complete"
echo ""

# Phase 11: Initialize Superset
echo "Phase 11: Initializing Superset..."
./scripts/init_superset.sh

echo ""
echo "======================================"
echo "✓ PHASES 10-11 COMPLETE"
echo "======================================"
echo ""
echo "Next Steps:"
echo "1. Access Airflow: http://localhost:8081 (admin/admin)"
echo "2. Access Superset: http://localhost:8088 (admin/admin123)"
echo "3. Follow docs/SUPERSET_DASHBOARD_GUIDE.md to create dashboards"
echo ""

