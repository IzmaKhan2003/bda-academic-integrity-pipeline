"""
Data Volume Monitoring Script
Phase 12 - Step 50: Continuous MongoDB size tracking
"""

from pymongo import MongoClient
from datetime import datetime
import time
import sys

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MIN_SIZE_MB = 300
MAX_SIZE_MB = 600
CHECK_INTERVAL = 60  # seconds

def get_db_size():
    """Get current database size in MB"""
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    stats = db.command('dbstats')
    size_mb = stats['dataSize'] / (1024 * 1024)
    return size_mb, stats

def check_size_alerts(size_mb):
    """Check if size triggers alerts"""
    alerts = []
    
    if size_mb < MIN_SIZE_MB:
        alerts.append({
            'level': 'WARNING',
            'message': f'Size {size_mb:.2f} MB < minimum {MIN_SIZE_MB} MB',
            'action': 'Generate more data or reduce archival frequency'
        })
    
    if size_mb > MAX_SIZE_MB:
        alerts.append({
            'level': 'CRITICAL',
            'message': f'Size {size_mb:.2f} MB > maximum {MAX_SIZE_MB} MB',
            'action': 'Trigger archival immediately'
        })
    
    return alerts

def get_collection_stats(db):
    """Get statistics for each collection"""
    collections = ['students', 'courses', 'exams', 'questions', 'exam_attempts', 'session_logs']
    stats = {}
    
    for coll in collections:
        count = db[coll].count_documents({})
        size = db.command('collStats', coll).get('size', 0) / (1024 * 1024)
        stats[coll] = {'count': count, 'size_mb': round(size, 2)}
    
    return stats

def monitor_continuous():
    """Continuous monitoring loop"""
    print("=" * 70)
    print("DATA VOLUME MONITORING - Phase 12")
    print("=" * 70)
    print(f"Alert Thresholds: {MIN_SIZE_MB} MB - {MAX_SIZE_MB} MB")
    print(f"Check Interval: {CHECK_INTERVAL} seconds")
    print("=" * 70)
    print("\nPress Ctrl+C to stop\n")
    
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    try:
        while True:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Get size
            size_mb, db_stats = get_db_size()
            
            # Check alerts
            alerts = check_size_alerts(size_mb)
            
            # Get collection stats
            coll_stats = get_collection_stats(db)
            
            # Display status
            print(f"\n[{timestamp}]")
            print(f"Database Size: {size_mb:.2f} MB")
            print(f"Storage Size: {db_stats['storageSize'] / (1024 * 1024):.2f} MB")
            print(f"Index Size: {db_stats['indexSize'] / (1024 * 1024):.2f} MB")
            
            # Show collection breakdown
            print("\nCollection Breakdown:")
            for coll, stats in coll_stats.items():
                print(f"  {coll:20s}: {stats['count']:>8,} records | {stats['size_mb']:>6.2f} MB")
            
            # Show alerts
            if alerts:
                print("\n⚠️  ALERTS:")
                for alert in alerts:
                    print(f"  [{alert['level']}] {alert['message']}")
                    print(f"    → Action: {alert['action']}")
            else:
                print("\n✓ Status: Normal (within thresholds)")
            
            print("-" * 70)
            
            # Wait for next check
            time.sleep(CHECK_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
        print("=" * 70)

def run_single_check():
    """Run a single check and exit"""
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("=" * 70)
    print("DATA VOLUME CHECK - Phase 12")
    print("=" * 70)
    
    size_mb, db_stats = get_db_size()
    alerts = check_size_alerts(size_mb)
    coll_stats = get_collection_stats(db)
    
    print(f"\nDatabase Size: {size_mb:.2f} MB")
    print(f"Storage Size: {db_stats['storageSize'] / (1024 * 1024):.2f} MB")
    print(f"Index Size: {db_stats['indexSize'] / (1024 * 1024):.2f} MB")
    
    print("\nCollection Breakdown:")
    for coll, stats in coll_stats.items():
        print(f"  {coll:20s}: {stats['count']:>8,} records | {stats['size_mb']:>6.2f} MB")
    
    if alerts:
        print("\n⚠️  ALERTS:")
        for alert in alerts:
            print(f"  [{alert['level']}] {alert['message']}")
            print(f"    → Action: {alert['action']}")
        print("\n✗ FAILED: Size outside acceptable range")
        sys.exit(1)
    else:
        print("\n✓ PASSED: Size within acceptable range ({MIN_SIZE_MB}-{MAX_SIZE_MB} MB)")
        print("=" * 70)
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        monitor_continuous()
    else:
        run_single_check()
