"""
Cache Refresh DAG
Phase 10 - Step 40: Invalidate stale cache every 60 seconds
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import redis

default_args = {
    'owner': 'academic-integrity-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'cache_refresh',
    default_args=default_args,
    description='Invalidate expired cache keys every 60 seconds',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    catchup=False,
    tags=['cache', 'redis'],
)

def invalidate_expired_keys():
    """Remove keys with TTL < 10 seconds (about to expire)"""
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    
    # Get all keys matching our pattern
    pattern = 'kpi:*'
    keys = redis_client.keys(pattern)
    
    print(f"Total cache keys: {len(keys)}")
    
    expired_count = 0
    for key in keys:
        ttl = redis_client.ttl(key)
        if ttl < 10 and ttl > 0:  # About to expire
            redis_client.delete(key)
            expired_count += 1
    
    print(f"✓ Invalidated {expired_count} expiring keys")
    print(f"✓ Active cache keys: {len(keys) - expired_count}")
    
    return expired_count

def verify_cache_status():
    """Verify cache has fresh data"""
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    
    # Check key cache keys
    key_checks = [
        'kpi:avg_response_time:current',
        'kpi:high_risk_students:current',
        'kpi:total_events'
    ]
    
    fresh_count = 0
    for key in key_checks:
        ttl = redis_client.ttl(key)
        if ttl > 30:  # Fresh (more than 30s left)
            fresh_count += 1
            print(f"✓ {key}: {ttl}s remaining")
        else:
            print(f"⚠ {key}: {ttl}s remaining (stale)")
    
    print(f"\nCache health: {fresh_count}/{len(key_checks)} keys are fresh")
    
    return fresh_count

# Task 1: Invalidate expiring keys
invalidate_task = PythonOperator(
    task_id='invalidate_expired_keys',
    python_callable=invalidate_expired_keys,
    dag=dag,
)

# Task 2: Verify cache status
verify_task = PythonOperator(
    task_id='verify_cache_status',
    python_callable=verify_cache_status,
    dag=dag,
)

# Define task dependencies
invalidate_task >> verify_task
