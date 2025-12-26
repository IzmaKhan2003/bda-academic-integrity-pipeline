"""
Data Validation Script
Phase 5 - Step 19
"""

from pymongo import MongoClient

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'

def validate():
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("=" * 70)
    print("DATA VALIDATION REPORT")
    print("=" * 70)
    
    # Check size
    stats = db.command('dbstats')
    size_mb = stats['dataSize'] / (1024 * 1024)
    print(f"\n1. Database Size: {size_mb:.2f} MB")
    
    if size_mb >= 250:
        print("   ✓ PASS: Size >= 250 MB")
    else:
        print(f"   ✗ FAIL: Size {size_mb:.2f} MB < 250 MB")
    
    # Check record counts
    print(f"\n2. Record Counts:")
    counts = {
        'students': db.students.count_documents({}),
        'courses': db.courses.count_documents({}),
        'exams': db.exams.count_documents({}),
        'questions': db.questions.count_documents({}),
        'exam_attempts': db.exam_attempts.count_documents({}),
        'session_logs': db.session_logs.count_documents({})
    }
    
    for coll, count in counts.items():
        print(f"   {coll}: {count:,}")
    
    # Check for nulls
    print(f"\n3. Data Quality:")
    null_checks = {
        'exam_attempts': ['student_id', 'exam_id', 'ai_usage_score'],
        'session_logs': ['student_id', 'exam_id']
    }
    
    all_pass = True
    for coll, fields in null_checks.items():
        for field in fields:
            null_count = db[coll].count_documents({field: None})
            if null_count > 0:
                print(f"   ✗ {coll}.{field}: {null_count} nulls found")
                all_pass = False
    
    if all_pass:
        print("   ✓ PASS: No null values in required fields")
    
    # Check distributions
    print(f"\n4. AI Usage Distribution:")
    pipeline = [
        {'$bucket': {
            'groupBy': '$ai_usage_score',
            'boundaries': [0.0, 0.3, 0.7, 1.0],
            'default': 'Other',
            'output': {'count': {'$sum': 1}}
        }}
    ]
    result = list(db.exam_attempts.aggregate(pipeline))
    for bucket in result:
        pct = bucket['count'] / counts['exam_attempts'] * 100
        print(f"   {bucket['_id']:.1f}: {bucket['count']:,} ({pct:.1f}%)")
    
    print("\n" + "=" * 70)
    print(f"VALIDATION {'PASSED' if size_mb >= 250 and all_pass else 'FAILED'}")
    print("=" * 70)

if __name__ == "__main__":
    validate()
