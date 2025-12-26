"""
Real-Time Data Generation DAG
Phase 10 - Step 37: Triggers data generation every 60 seconds
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import subprocess

default_args = {
    'owner': 'academic-integrity-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'realtime_data_generation',
    default_args=default_args,
    description='Generate real-time exam data every 60 seconds',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['generation', 'realtime'],
)


def check_kafka_health():
    """Check if Kafka is responsive"""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'kafka', 'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("✓ Kafka is healthy")
            return True
        else:
            raise Exception(f"Kafka unhealthy: {result.stderr}")
    except Exception as e:
        raise Exception(f"Kafka health check failed: {e}")


def monitor_generation_rate():
    """Monitor data generation rate"""
    mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = mongo_client['academic_integrity']

    two_min_ago = datetime.now() - timedelta(minutes=2)
    recent_count = db.exam_attempts.count_documents({
        'submission_timestamp': {'$gte': two_min_ago}
    })

    rate = recent_count / 2  # per minute
    print(f"Recent generation rate: {rate:.0f} events/minute")

    if rate < 500:
        print("⚠ Warning: Generation rate is low")
    else:
        print("✓ Generation rate is healthy")

    return rate


# Task 1: Check Kafka health
check_kafka_task = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 2: Trigger data generator (run for 50 seconds)
generate_data_task = BashOperator(
    task_id='trigger_data_generator',
    bash_command='docker exec spark-master timeout 50 python3 /spark/jobs/realtime_data_generator.py || true',
    dag=dag,
)

# Task 3: Monitor generation rate
monitor_task = PythonOperator(
    task_id='monitor_generation_rate',
    python_callable=monitor_generation_rate,
    dag=dag,
)

# Define task dependencies
check_kafka_task >> generate_data_task >> monitor_task
