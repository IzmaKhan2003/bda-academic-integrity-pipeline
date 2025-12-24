"""
Simple Data Generation DAG for Week 1 Testing
Triggers data generation every 2 minutes
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'academic-integrity-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

def check_kafka_health():
    """Check if Kafka is healthy before starting generation"""
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
            print(f"✗ Kafka check failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Error checking Kafka: {e}")
        return False

def check_mongodb_health():
    """Check if MongoDB is healthy"""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'mongodb', 'mongosh', '--eval', "db.adminCommand('ping')", '--quiet'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if 'ok' in result.stdout:
            print("✓ MongoDB is healthy")
            return True
        else:
            print(f"✗ MongoDB check failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Error checking MongoDB: {e}")
        return False

# Create DAG
dag = DAG(
    'simple_data_generation_pipeline',
    default_args=default_args,
    description='Simple data generation pipeline for Week 1 testing',
    schedule_interval=timedelta(minutes=2),  # Run every 2 minutes
    catchup=False,
    tags=['week1', 'testing', 'data-generation'],
)

# Task 1: Check Kafka health
check_kafka = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 2: Check MongoDB health
check_mongo = PythonOperator(
    task_id='check_mongodb_health',
    python_callable=check_mongodb_health,
    dag=dag,
)

# Task 3: Start Kafka consumer (MongoDB writer) in background
start_consumer = BashOperator(
    task_id='start_kafka_consumer',
    bash_command="""
    docker exec -d spark-master python3 /opt/spark-jobs/kafka_to_mongo_consumer.py
    echo "✓ Kafka consumer started"
    """,
    dag=dag,
)

# Task 4: Generate data for 60 seconds
generate_data = BashOperator(
    task_id='generate_streaming_data',
    bash_command="""
    docker exec spark-master timeout 60 python3 /opt/spark-jobs/generators/simple_data_generator.py
    echo "✓ Data generation completed (60 seconds)"
    """,
    dag=dag,
)

# Task 5: Verify data in MongoDB
verify_data = BashOperator(
    task_id='verify_mongodb_data',
    bash_command="""
    docker exec mongodb mongosh -u admin -p admin123 --eval "
    use academic_integrity;
    print('Exam Attempts:', db.exam_attempts.count());
    print('Session Logs:', db.session_logs.count());
    " --quiet
    """,
    dag=dag,
)

# Define task dependencies
check_kafka >> check_mongo >> start_consumer >> generate_data >> verify_data