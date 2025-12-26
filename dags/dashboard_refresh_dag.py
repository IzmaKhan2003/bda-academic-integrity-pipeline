"""
Dashboard Data Refresh DAG
Updates Grafana data sources every minute
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import redis
import json

default_args = {
    'owner': 'academic-integrity-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'dashboard_data_refresh',
    default_args=default_args,
    description='Refresh Grafana dashboard data every minute',
    schedule_interval='*/1 * * * *',
    catchup=False,
    tags=['dashboard', 'grafana'],
)

def refresh_aggregated_collections():
    """Refresh aggregated_kpis collection"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['academic_integrity']
    
    two_hours_ago = datetime.now() - timedelta(hours=2)
    
    # Recreate aggregated_kpis
    db.aggregated_kpis.drop()
    
    pipeline = [
        {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
        {'$group': {
            '_id': {'student_id': '$student_id', 'exam_id': '$exam_id'},
            'avg_response_time': {'$avg': '$response_time'},
            'accuracy_rate': {'$avg': {'$cond': ['$is_correct', 1, 0]}},
            'avg_ai_score': {'$avg': '$ai_usage_score'},
            'response_variance': {'$stdDevPop': '$response_time'},
            'total_attempts': {'$sum': 1},
            'last_updated': {'$max': '$submission_timestamp'}
        }},
        {'$addFields': {
            'student_id': '$_id.student_id',
            'exam_id': '$_id.exam_id',
            'accuracy_rate': {'$multiply': ['$accuracy_rate', 100]},
            'risk_level': {
                '$switch': {
                    'branches': [
                        {'case': {'$gte': ['$avg_ai_score', 0.7]}, 'then': 'High'},
                        {'case': {'$gte': ['$avg_ai_score', 0.4]}, 'then': 'Medium'}
                    ],
                    'default': 'Low'
                }
            }
        }},
        {'$project': {'_id': 0}},
        {'$out': 'aggregated_kpis'}
    ]
    
    list(db.exam_attempts.aggregate(pipeline))
    
    count = db.aggregated_kpis.count_documents({})
    print(f"✓ Refreshed aggregated_kpis: {count:,} records")
    
    # Update high_risk_students
    db.high_risk_students.drop()
    
    pipeline = [
        {'$match': {'avg_ai_score': {'$gte': 0.7}}},
        {'$lookup': {
            'from': 'students',
            'localField': 'student_id',
            'foreignField': 'student_id',
            'as': 'student_info'
        }},
        {'$unwind': '$student_info'},
        {'$lookup': {
            'from': 'exams',
            'localField': 'exam_id',
            'foreignField': 'exam_id',
            'as': 'exam_info'
        }},
        {'$unwind': '$exam_info'},
        {'$project': {
            'student_id': 1,
            'student_name': '$student_info.name',
            'program': '$student_info.program',
            'exam_id': 1,
            'exam_name': '$exam_info.exam_name',
            'avg_ai_score': 1,
            'avg_response_time': 1,
            'accuracy_rate': 1,
            'total_attempts': 1
        }},
        {'$sort': {'avg_ai_score': -1}},
        {'$limit': 50},
        {'$out': 'high_risk_students'}
    ]
    
    list(db.aggregated_kpis.aggregate(pipeline))
    
    count = db.high_risk_students.count_documents({})
    print(f"✓ Refreshed high_risk_students: {count:,} records")
    
    return count

def refresh_time_series():
    """Refresh time_series_kpis collection"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['academic_integrity']
    
    two_hours_ago = datetime.now() - timedelta(hours=2)
    
    db.time_series_kpis.drop()
    
    pipeline = [
        {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
        {'$group': {
            '_id': {
                'time_bucket': {
                    '$dateTrunc': {
                        'date': '$submission_timestamp',
                        'unit': 'minute',
                        'binSize': 5
                    }
                },
                'exam_id': '$exam_id'
            },
            'avg_ai_score': {'$avg': '$ai_usage_score'},
            'avg_response_time': {'$avg': '$response_time'},
            'total_events': {'$sum': 1}
        }},
        {'$addFields': {
            'timestamp': '$_id.time_bucket',
            'exam_id': '$_id.exam_id'
        }},
        {'$project': {'_id': 0}},
        {'$sort': {'timestamp': -1}},
        {'$out': 'time_series_kpis'}
    ]
    
    list(db.exam_attempts.aggregate(pipeline))
    
    count = db.time_series_kpis.count_documents({})
    print(f"✓ Refreshed time_series_kpis: {count:,} records")
    
    return count

refresh_agg_task = PythonOperator(
    task_id='refresh_aggregated_collections',
    python_callable=refresh_aggregated_collections,
    dag=dag,
)

refresh_ts_task = PythonOperator(
    task_id='refresh_time_series',
    python_callable=refresh_time_series,
    dag=dag,
)

refresh_agg_task >> refresh_ts_task
