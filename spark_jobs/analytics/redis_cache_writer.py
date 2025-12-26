"""
Redis Cache Writer
Phase 8 - Step 29: Cache aggregated KPI results
"""

import sys
import redis
import json
from datetime import datetime, timedelta
from pymongo import MongoClient

REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_TTL = 60

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'

class RedisCacheWriter:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=0,
            decode_responses=True
        )
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client['academic_integrity']
        print("✓ Connected to Redis and MongoDB")
    
    def cache_all_kpis(self):
        print("\n" + "=" * 70)
        print("CACHING AGGREGATED KPIS")
        print("=" * 70)
        
        two_hours_ago = datetime.now() - timedelta(hours=2)
        cached_count = 0
        
        # KPI 1: Average Response Time
        print("\n1. Caching avg_response_time...")
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {'_id': None, 'avg': {'$avg': '$response_time'}}}
        ]
        result = list(self.db.exam_attempts.aggregate(pipeline))
        
        if result:
            avg_time = result[0]['avg']
            self.redis_client.setex(
                'kpi:avg_response_time:all',
                REDIS_TTL,
                json.dumps({'value': round(avg_time, 2), 'timestamp': datetime.now().isoformat()})
            )
            print(f"   ✓ Overall avg: {avg_time:.2f}s")
            cached_count += 1
        
        # Per-exam averages
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {'_id': '$exam_id', 'avg': {'$avg': '$response_time'}}}
        ]
        results = list(self.db.exam_attempts.aggregate(pipeline))
        
        for item in results:
            self.redis_client.setex(
                f'kpi:avg_response_time:exam:{item["_id"]}',
                REDIS_TTL,
                json.dumps({'value': round(item['avg'], 2)})
            )
            cached_count += 1
        
        print(f"   ✓ {len(results)} per-exam averages")
        
        # KPI 2: Accuracy Rate
        print("\n2. Caching accuracy_rate...")
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {
                '_id': None,
                'correct': {'$sum': {'$cond': ['$is_correct', 1, 0]}},
                'total': {'$sum': 1}
            }}
        ]
        result = list(self.db.exam_attempts.aggregate(pipeline))
        
        if result:
            accuracy = (result[0]['correct'] / result[0]['total']) * 100
            self.redis_client.setex(
                'kpi:accuracy_rate:all',
                REDIS_TTL,
                json.dumps({'value': round(accuracy, 2)})
            )
            print(f"   ✓ Overall accuracy: {accuracy:.2f}%")
            cached_count += 1
        
        # KPI 3: AI Usage Distribution
        print("\n3. Caching ai_usage distribution...")
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$bucket': {
                'groupBy': '$ai_usage_score',
                'boundaries': [0.0, 0.3, 0.7, 1.0],
                'default': 'Other',
                'output': {'count': {'$sum': 1}}
            }}
        ]
        results = list(self.db.exam_attempts.aggregate(pipeline))
        
        labels = {0.0: 'low', 0.3: 'medium', 0.7: 'high'}
        for item in results:
            label = labels.get(item['_id'], 'other')
            self.redis_client.setex(
                f'kpi:ai_usage:{label}',
                REDIS_TTL,
                json.dumps({'count': item['count']})
            )
            print(f"   ✓ {label}: {item['count']}")
            cached_count += 1
        
        # KPI 4: High-Risk Students
        print("\n4. Caching high-risk students...")
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {
                '_id': '$student_id',
                'avg_ai_score': {'$avg': '$ai_usage_score'},
                'attempts': {'$sum': 1}
            }},
            {'$match': {'avg_ai_score': {'$gte': 0.7}}},
            {'$sort': {'avg_ai_score': -1}},
            {'$limit': 20}
        ]
        results = list(self.db.exam_attempts.aggregate(pipeline))
        
        high_risk = [
            {'student_id': r['_id'], 'ai_score': round(r['avg_ai_score'], 3)}
            for r in results
        ]
        
        self.redis_client.setex(
            'kpi:high_risk_students',
            REDIS_TTL,
            json.dumps({'students': high_risk, 'count': len(high_risk)})
        )
        print(f"   ✓ {len(high_risk)} high-risk students")
        cached_count += 1
        
        # KPI 5: Collusion Pairs
        print("\n5. Caching collusion pairs...")
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {
                '_id': {'exam_id': '$exam_id', 'question_id': '$question_id', 'hash': '$answer_hash'},
                'students': {'$addToSet': '$student_id'},
                'count': {'$sum': 1}
            }},
            {'$match': {'count': {'$gte': 2}}},
            {'$limit': 50}
        ]
        results = list(self.db.exam_attempts.aggregate(pipeline))
        
        collusion_cases = []
        for item in results:
            if len(item['students']) >= 2:
                collusion_cases.append({
                    'exam_id': item['_id']['exam_id'],
                    'question_id': item['_id']['question_id'],
                    'students': item['students'][:5],
                    'count': item['count']
                })
        
        self.redis_client.setex(
            'kpi:collusion_cases',
            REDIS_TTL,
            json.dumps({'cases': collusion_cases, 'total': len(collusion_cases)})
        )
        print(f"   ✓ {len(collusion_cases)} collusion cases")
        cached_count += 1
        
        # Summary
        print("\n" + "=" * 70)
        print(f"CACHING COMPLETE: {cached_count} keys cached")
        print("=" * 70)
        
        return cached_count
    
    def test_cache_retrieval(self):
        print("\n" + "=" * 70)
        print("TESTING CACHE RETRIEVAL")
        print("=" * 70)
        
        test_keys = [
            'kpi:avg_response_time:all',
            'kpi:accuracy_rate:all',
            'kpi:ai_usage:high',
            'kpi:high_risk_students'
        ]
        
        for key in test_keys:
            value = self.redis_client.get(key)
            if value:
                data = json.loads(value)
                print(f"\n{key}:")
                print(f"  {json.dumps(data, indent=2)}")
            else:
                print(f"\n{key}: NOT FOUND")
        
        # Check TTL
        ttl = self.redis_client.ttl('kpi:avg_response_time:all')
        print(f"\nTTL for kpi:avg_response_time:all: {ttl} seconds")
        
        print("\n" + "=" * 70)

def main():
    print("=" * 70)
    print("REDIS CACHE WRITER - Phase 8")
    print("=" * 70)
    
    writer = RedisCacheWriter()
    writer.cache_all_kpis()
    writer.test_cache_retrieval()
    
    print("\n✓ Phase 8 Complete: Redis caching working")

if __name__ == "__main__":
    main()
