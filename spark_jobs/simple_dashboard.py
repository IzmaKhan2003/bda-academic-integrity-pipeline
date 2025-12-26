"""
Simple Web Dashboard (Alternative to Superset)
Lightweight real-time dashboard using Flask
"""

from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
import redis
import json

app = Flask(__name__)

# Connections
mongo_client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
db = mongo_client['academic_integrity']
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/kpis')
def get_kpis():
    """Get current KPIs from Redis or MongoDB"""
    kpis = {}
    
    # Try to get from cache first
    for key in ['avg_response_time', 'high_risk_count', 'total_events']:
        full_key = f'kpi:{key}'
        value = redis_client.get(full_key)
        if value:
            kpis[key] = json.loads(value)
    
    # If cache empty, compute from MongoDB
    if not kpis:
        two_hours_ago = datetime.now() - timedelta(hours=2)
        
        # Compute avg response time
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {'_id': None, 'avg_response_time': {'$avg': '$response_time'}}}
        ]
        result = list(db.exam_attempts.aggregate(pipeline))
        if result and result[0]['avg_response_time'] is not None:
            kpis['avg_response_time'] = {'value': round(result[0]['avg_response_time'], 2)}
        else:
            kpis['avg_response_time'] = {'value': 0}
        
        # Count high-risk students
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {'_id': '$student_id', 'avg_ai': {'$avg': '$ai_usage_score'}}},
            {'$match': {'avg_ai': {'$gte': 0.7}}}
        ]
        high_risk_count = len(list(db.exam_attempts.aggregate(pipeline)))
        kpis['high_risk_count'] = {'value': high_risk_count}
        
        # Total events
        total = db.exam_attempts.count_documents({})
        kpis['total_events'] = {'value': total}
        
        # Cache results for 30 seconds
        for key, val in kpis.items():
            redis_client.setex(f'kpi:{key}', 30, json.dumps(val))
    
    return jsonify(kpis)

@app.route('/api/trend')
def get_trend():
    """Get AI usage trend (last 2 hours, 10-minute buckets)"""
    two_hours_ago = datetime.now() - timedelta(hours=2)
    
    pipeline = [
        {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
        {'$group': {
            '_id': {
                '$dateTrunc': {
                    'date': '$submission_timestamp',
                    'unit': 'minute',
                    'binSize': 10
                }
            },
            'avg_ai_score': {'$avg': '$ai_usage_score'},
            'count': {'$sum': 1}
        }},
        {'$sort': {'_id': 1}}
    ]
    
    results = list(db.exam_attempts.aggregate(pipeline))
    
    trend_data = [
        {
            'timestamp': r['_id'].isoformat() if r['_id'] else None,
            'avg_ai_score': round(r['avg_ai_score'], 3),
            'count': r['count']
        }
        for r in results
    ]
    
    return jsonify({'trend': trend_data})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
