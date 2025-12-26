"""
Create Collusion Pairs Collection
Detects student pairs with matching answers
"""

from pymongo import MongoClient
from datetime import datetime, timedelta

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'

def create_collusion_pairs():
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("="*70)
    print("CREATING COLLUSION PAIRS COLLECTION")
    print("="*70)
    
    # Drop existing
    db.collusion_pairs.drop()
    
    # Time window
    two_hours_ago = datetime.now() - timedelta(hours=2)
    
    # Detect collusion via self-join on answer_hash
    pipeline = [
        {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
        {'$lookup': {
            'from': 'exam_attempts',
            'let': {
                'exam_id': '$exam_id',
                'question_id': '$question_id',
                'answer_hash': '$answer_hash',
                'student_id': '$student_id',
                'timestamp': '$submission_timestamp'
            },
            'pipeline': [
                {'$match': {
                    '$expr': {
                        '$and': [
                            {'$eq': ['$exam_id', '$$exam_id']},
                            {'$eq': ['$question_id', '$$question_id']},
                            {'$eq': ['$answer_hash', '$$answer_hash']},
                            {'$ne': ['$student_id', '$$student_id']}
                        ]
                    }
                }}
            ],
            'as': 'matches'
        }},
        {'$unwind': '$matches'},
        {'$project': {
            'student_1_id': {
                '$cond': [
                    {'$lt': ['$student_id', '$matches.student_id']},
                    '$student_id',
                    '$matches.student_id'
                ]
            },
            'student_2_id': {
                '$cond': [
                    {'$lt': ['$student_id', '$matches.student_id']},
                    '$matches.student_id',
                    '$student_id'
                ]
            },
            'exam_id': 1,
            'question_id': 1,
            'answer_hash': 1,
            'time_diff': {
                '$abs': {
                    '$subtract': ['$submission_timestamp', '$matches.submission_timestamp']
                }
            }
        }},
        {'$group': {
            '_id': {
                'student_1': '$student_1_id',
                'student_2': '$student_2_id',
                'exam_id': '$exam_id'
            },
            'matching_questions_count': {'$sum': 1},
            'avg_time_difference_seconds': {
                '$avg': {'$divide': ['$time_diff', 1000]}
            },
            'questions': {'$addToSet': '$question_id'}
        }},
        {'$match': {'matching_questions_count': {'$gte': 3}}},
        {'$addFields': {
            'student_1_id': '$_id.student_1',
            'student_2_id': '$_id.student_2',
            'exam_id': '$_id.exam_id',
            'similarity_score': {
                '$divide': ['$matching_questions_count', 50]
            },
            'risk_level': {
                '$switch': {
                    'branches': [
                        {'case': {'$gte': ['$matching_questions_count', 10]}, 'then': 'High'},
                        {'case': {'$gte': ['$matching_questions_count', 5]}, 'then': 'Medium'}
                    ],
                    'default': 'Low'
                }
            },
            'detected_at': datetime.now()
        }},
        {'$project': {'_id': 0}},
        {'$out': 'collusion_pairs'}
    ]
    
    print("\nRunning collusion detection pipeline...")
    list(db.exam_attempts.aggregate(pipeline, allowDiskUse=True))
    
    count = db.collusion_pairs.count_documents({})
    print(f"\n✅ Created collusion_pairs: {count:,} suspicious pairs")
    
    # Create indexes
    db.collusion_pairs.create_index([('student_1_id', 1)])
    db.collusion_pairs.create_index([('student_2_id', 1)])
    db.collusion_pairs.create_index([('exam_id', 1)])
    db.collusion_pairs.create_index([('matching_questions_count', -1)])
    db.collusion_pairs.create_index([('similarity_score', -1)])
    print("✅ Indexes created")
    
    # Show sample
    print("\nSample collusion pairs:")
    for pair in db.collusion_pairs.find().limit(5):
        print(f"  {pair['student_1_id']} <-> {pair['student_2_id']}: "
              f"{pair['matching_questions_count']} matches, "
              f"similarity={pair['similarity_score']:.2f}")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    create_collusion_pairs()
