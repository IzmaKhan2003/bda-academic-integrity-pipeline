#!/bin/bash

echo "======================================"
echo "TESTING AIRFLOW DAGS"
echo "======================================"
echo ""

# List all DAGs
echo "1. Listing all DAGs..."
docker exec airflow-webserver airflow dags list | grep -E "realtime_data_generation|analytics_processing|data_archival|cache_refresh"

echo ""

# Unpause DAGs (enable them)
echo "2. Enabling DAGs..."
docker exec airflow-webserver airflow dags unpause realtime_data_generation
docker exec airflow-webserver airflow dags unpause analytics_processing
docker exec airflow-webserver airflow dags unpause data_archival
docker exec airflow-webserver airflow dags unpause cache_refresh

echo ""

# Trigger a test run
echo "3. Triggering test run of analytics_processing..."
docker exec airflow-webserver airflow dags trigger analytics_processing

echo ""

# Wait 30 seconds
echo "Waiting 30 seconds for DAG to execute..."
sleep 30

# Check recent DAG runs
echo ""
echo "4. Recent DAG runs:"
docker exec airflow-webserver airflow dags list-runs -d analytics_processing --limit 3

echo ""
echo "======================================"
echo "✓ AIRFLOW TEST COMPLETE"
echo "======================================"
echo ""
echo "Next: Open http://localhost:8081 to see DAGs in UI"
