#!/bin/bash

# DAG Control Script

case "$1" in
    deploy)
        echo "📦 Deploying DAGs..."
        docker cp dags/*.py airflow-webserver:/opt/airflow/dags/
        docker cp dags/*.py airflow-scheduler:/opt/airflow/dags/
        sleep 30
        echo "✅ DAGs deployed"
        ;;
    
    enable)
        echo "▶️ Enabling all DAGs..."
        docker exec airflow-webserver airflow dags unpause realtime_generation_dag
        docker exec airflow-webserver airflow dags unpause analytics_processing_dag
        docker exec airflow-webserver airflow dags unpause dashboard_data_refresh_dag_updated
        docker exec airflow-webserver airflow dags unpause archival_dag
        echo "✅ All DAGs enabled"
        ;;
    
    disable)
        echo "⏸️ Disabling all DAGs..."
        docker exec airflow-webserver airflow dags pause realtime_generation_dag
        docker exec airflow-webserver airflow dags pause analytics_processing_dag
        docker exec airflow-webserver airflow dags pause dashboard_data_refresh_dag_updated
        docker exec airflow-webserver airflow dags pause archival_dag
        echo "✅ All DAGs disabled"
        ;;
    
    trigger)
        echo "🚀 Triggering all DAGs manually..."
        docker exec airflow-webserver airflow dags trigger realtime_generation_dag
        docker exec airflow-webserver airflow dags trigger analytics_processing_dag
        docker exec airflow-webserver airflow dags trigger dashboard_data_refresh_dag_updated
        echo "✅ All DAGs triggered"
        ;;
    
    status)
        echo "📊 DAG Status:"
        docker exec airflow-webserver airflow dags list-runs --limit 20
        ;;
    
    logs)
        if [ -z "$2" ]; then
            echo "Usage: $0 logs [dag_name]"
            exit 1
        fi
        docker logs airflow-scheduler | grep "$2"
        ;;
    
    *)
        echo "Usage: $0 {deploy|enable|disable|trigger|status|logs [dag_name]}"
        exit 1
        ;;
esac