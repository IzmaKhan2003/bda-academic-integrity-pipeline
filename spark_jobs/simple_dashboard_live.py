from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import redis
import datetime
import random

app = Flask(__name__)

# -------------------------------
# Connect to Redis and MongoDB
# -------------------------------
redis_client = redis.Redis(host='redis', port=6379, db=0)
mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client.academic_integrity

# -------------------------------
# Routes
# -------------------------------

@app.route('/')
def index():
    return render_template('dashboard_live.html')

@app.route('/api/kpis')
def get_kpis():
    kpis = {}
    keys = ["avg_response_time", "accuracy_rate", "high_risk_count", "total_events"]
    for key in keys:
        val = redis_client.get(key)
        kpis[key] = float(val) if val else 0
    return jsonify(kpis)

@app.route('/api/trend')
def get_trend():
    pipeline = [
        {"$group": {
            "_id": "$student_id",
            "avg_score": {"$avg": "$score"},
            "ai_usage": {"$sum": "$ai_usage"}
        }}
    ]
    results = list(db.exam_attempts.aggregate(pipeline))
    return jsonify(results)

# -------------------------------
# Run server
# -------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
