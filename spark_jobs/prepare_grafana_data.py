"""
Prepare MongoDB data for Grafana
Creates aggregated collections for faster dashboard queries
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
import time

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'

def create_aggregated_collections():
    """Create pre-aggregated collections for Grafana"""
    
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("="*70)
    print("PREPARING GRAFANA DATA")
    print("="*70)
    
    # Create aggregated_kpis collection
    print("\n1. Creating aggregated_kpis collection...")
    
    # Drop existing
    db.aggregated_kpis.drop()
    
    # Calculate last 2 hours data
    two_hours_ago = datetime.now() - timedelta(days=7)
    
    # Aggregate by student and exam
    pipeline = [
        {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
        {'$group': {
            '_id': {
                'student_id': '$student_id',
                'exam_id': '$exam_id'
            },
            'avg_response_time': {'$avg': '$response_time'},
            'accuracy_rate': {
                '$avg': {'$cond': ['$is_correct', 1, 0]}
            },
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
                        {
                            'case': {'$gte': ['$avg_ai_score', 0.7]},
                            'then': 'High'
                        },
                        {
                            'case': {'$gte': ['$avg_ai_score', 0.4]},
                            'then': 'Medium'
                        }
                    ],
                    'default': 'Low'
                }
            }
        }},
        {'$project': {'_id': 0}},
        {'$out': 'aggregated_kpis'}
    ]
    
    result = list(db.exam_attempts.aggregate(pipeline))
    
    count = db.aggregated_kpis.count_documents({})
    print(f"   ✓ Created aggregated_kpis: {count:,} records")
    
    # Create indexes
    db.aggregated_kpis.create_index([('student_id', 1)])
    db.aggregated_kpis.create_index([('exam_id', 1)])
    db.aggregated_kpis.create_index([('avg_ai_score', -1)])
    db.aggregated_kpis.create_index([('risk_level', 1)])
    print(f"   ✓ Indexes created")
    
    # Create high_risk_students collection
    print("\n2. Creating high_risk_students collection...")
    
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
            'total_attempts': 1,
            'last_updated': 1
        }},
        {'$sort': {'avg_ai_score': -1}},
        {'$limit': 50},
        {'$out': 'high_risk_students'}
    ]
    
    result = list(db.aggregated_kpis.aggregate(pipeline))
    
    count = db.high_risk_students.count_documents({})
    print(f"   ✓ Created high_risk_students: {count:,} records")
    
    # Create time-series aggregation
    print("\n3. Creating time_series_kpis collection...")
    
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
    
    result = list(db.exam_attempts.aggregate(pipeline))
    
    count = db.time_series_kpis.count_documents({})
    print(f"   ✓ Created time_series_kpis: {count:,} records")
    
    print("\n" + "="*70)
    print("DATA PREPARATION COMPLETE")
    print("="*70)
    print("\nGrafana-optimized collections created:")
    print("  - aggregated_kpis")
    print("  - high_risk_students")
    print("  - time_series_kpis")
    print("\nThese will auto-refresh via Airflow DAG")
    print("="*70)

if __name__ == "__main__":
    create_aggregated_collections()
