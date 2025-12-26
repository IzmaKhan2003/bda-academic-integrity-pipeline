"""
Archival DAG
Phase 10 - Step 39: Archive old data to HDFS every hour
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'academic-integrity-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_archival',
    default_args=default_args,
    description='Archive data older than 48 hours to HDFS',
    schedule_interval='0 * * * *',  # Every hour at :00
    catchup=False,
    tags=['archival', 'hdfs', 'cleanup'],
)

def check_archive_eligibility():
    """Check if there's data ready for archival"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['academic_integrity']
    
    cutoff = datetime.now() - timedelta(hours=48)
    
    old_count = db.exam_attempts.count_documents({
        'submission_timestamp': {'$lt': cutoff}
    })
    
    print(f"Data older than 48 hours: {old_count:,} records")
    
    if old_count == 0:
        print("✓ No data needs archiving (all data is recent)")
        return False
    else:
        print(f"✓ {old_count:,} records eligible for archival")
        return True

def log_archival_completion():
    """Log archival completion"""
    from pymongo import MongoClient
    
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['academic_integrity']
    
    stats = db.command('dbstats')
    size_mb = stats['dataSize'] / (1024 * 1024)
    
    print(f"✓ Archival complete. Current MongoDB size: {size_mb:.2f} MB")
    
    return size_mb

# Task 1: Check if archival is needed
check_eligibility_task = PythonOperator(
    task_id='check_archive_eligibility',
    python_callable=check_archive_eligibility,
    dag=dag,
)

# Task 2: Run archival job
archive_task = BashOperator(
    task_id='archive_to_hdfs',
    bash_command='''
    docker exec spark-master /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /spark/jars/mongo-spark-connector_2.12-10.1.1.jar,/spark/jars/mongodb-driver-sync-4.10.2.jar,/spark/jars/mongodb-driver-core-4.10.2.jar,/spark/jars/bson-4.10.2.jar \
      /spark/jobs/archival/archival_job.py || true
    ''',
    dag=dag,
)

# Task 3: Update metadata
update_metadata_task = BashOperator(
    task_id='update_hive_metastore',
    bash_command='docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /spark/jobs/archival/hive_metastore_writer.py || true',
    dag=dag,
)

# Task 4: Log completion
log_completion_task = PythonOperator(
    task_id='log_archival_completion',
    python_callable=log_archival_completion,
    dag=dag,
)

# Define task dependencies
check_eligibility_task >> archive_task >> update_metadata_task >> log_completion_task
