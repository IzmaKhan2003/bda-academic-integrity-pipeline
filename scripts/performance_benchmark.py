"""
Performance Benchmark Suite
Measures system performance across all components
"""

from pymongo import MongoClient
import redis
import time
from datetime import datetime, timedelta
import statistics

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
KAFKA_BROKER = 'kafka:9092'

class PerformanceBenchmark:
    def __init__(self):
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client['academic_integrity']
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.results = {}
    
    def benchmark_mongodb_read(self, iterations=100):
        """Benchmark MongoDB read performance"""
        print("\n1. MongoDB Read Performance")
        print("-" * 50)
        
        times = []
        for i in range(iterations):
            start = time.time()
            list(self.db.exam_attempts.find().limit(100))
            elapsed = (time.time() - start) * 1000  # ms
            times.append(elapsed)
        
        self.results['mongodb_read_avg_ms'] = statistics.mean(times)
        self.results['mongodb_read_p95_ms'] = statistics.quantiles(times, n=20)[18]
        
        print(f"  Average: {self.results['mongodb_read_avg_ms']:.2f} ms")
        print(f"  P95: {self.results['mongodb_read_p95_ms']:.2f} ms")
        print(f"  Min: {min(times):.2f} ms")
        print(f"  Max: {max(times):.2f} ms")
    
    def benchmark_mongodb_aggregation(self, iterations=10):
        """Benchmark MongoDB aggregation performance"""
        print("\n2. MongoDB Aggregation Performance")
        print("-" * 50)
        
        times = []
        two_hours_ago = datetime.now() - timedelta(hours=2)
        
        pipeline = [
            {'$match': {'submission_timestamp': {'$gte': two_hours_ago}}},
            {'$group': {
                '_id': '$student_id',
                'avg_ai_score': {'$avg': '$ai_usage_score'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'avg_ai_score': -1}},
            {'$limit': 100}
        ]
        
        for i in range(iterations):
            start = time.time()
            list(self.db.exam_attempts.aggregate(pipeline))
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
        
        self.results['mongodb_agg_avg_ms'] = statistics.mean(times)
        self.results['mongodb_agg_p95_ms'] = statistics.quantiles(times, n=20)[18]
        
        print(f"  Average: {self.results['mongodb_agg_avg_ms']:.2f} ms")
        print(f"  P95: {self.results['mongodb_agg_p95_ms']:.2f} ms")
    
    def benchmark_redis_cache(self, iterations=1000):
        """Benchmark Redis cache performance"""
        print("\n3. Redis Cache Performance")
        print("-" * 50)
        
        # Write performance
        write_times = []
        for i in range(iterations):
            start = time.time()
            self.redis_client.set(f'test_key_{i}', f'value_{i}', ex=60)
            elapsed = (time.time() - start) * 1000
            write_times.append(elapsed)
        
        # Read performance
        read_times = []
        for i in range(iterations):
            start = time.time()
            self.redis_client.get(f'test_key_{i}')
            elapsed = (time.time() - start) * 1000
            read_times.append(elapsed)
        
        # Cleanup
        for i in range(iterations):
            self.redis_client.delete(f'test_key_{i}')
        
        self.results['redis_write_avg_ms'] = statistics.mean(write_times)
        self.results['redis_read_avg_ms'] = statistics.mean(read_times)
        
        print(f"  Write Average: {self.results['redis_write_avg_ms']:.2f} ms")
        print(f"  Read Average: {self.results['redis_read_avg_ms']:.2f} ms")
        print(f"  Cache Hit Ratio: {self._calculate_cache_hit_ratio():.2%}")
    
    def _calculate_cache_hit_ratio(self):
        """Calculate Redis cache hit ratio"""
        info = self.redis_client.info('stats')
        hits = info.get('keyspace_hits', 0)
        misses = info.get('keyspace_misses', 0)
        total = hits + misses
        return hits / total if total > 0 else 0
    
    def benchmark_mongodb_write(self, iterations=100):
        """Benchmark MongoDB write performance"""
        print("\n4. MongoDB Write Performance")
        print("-" * 50)
        
        times = []
        test_collection = self.db['benchmark_test']
        
        for i in range(iterations):
            doc = {
                'test_id': f'TEST{i:06d}',
                'timestamp': datetime.now(),
                'value': i * 1.5
            }
            start = time.time()
            test_collection.insert_one(doc)
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
        
        # Cleanup
        test_collection.drop()
        
        self.results['mongodb_write_avg_ms'] = statistics.mean(times)
        self.results['mongodb_write_p95_ms'] = statistics.quantiles(times, n=20)[18]
        
        print(f"  Average: {self.results['mongodb_write_avg_ms']:.2f} ms")
        print(f"  P95: {self.results['mongodb_write_p95_ms']:.2f} ms")
    
    def benchmark_end_to_end_latency(self):
        """Measure end-to-end pipeline latency"""
        print("\n5. End-to-End Pipeline Latency")
        print("-" * 50)
        
        # Simulate: Data generation → Kafka → MongoDB → Aggregation → Redis
        start_time = time.time()
        
        # Step 1: Write to MongoDB
        doc = {
            'attempt_id': f'BENCH{int(time.time())}',
            'student_id': 'STU_BENCH',
            'exam_id': 'EXM_BENCH',
            'question_id': 'QST_BENCH',
            'response_time': 100.0,
            'is_correct': True,
            'ai_usage_score': 0.5,
            'answer_hash': 'benchmark_hash',
            'submission_timestamp': datetime.now()
        }
        self.db.exam_attempts.insert_one(doc)
        
        # Step 2: Aggregate
        pipeline = [
            {'$match': {'student_id': 'STU_BENCH'}},
            {'$group': {
                '_id': '$student_id',
                'avg_ai_score': {'$avg': '$ai_usage_score'}
            }}
        ]
        list(self.db.exam_attempts.aggregate(pipeline))
        
        # Step 3: Cache result
        self.redis_client.setex('bench_result', 60, '0.5')
        
        # Step 4: Read from cache
        self.redis_client.get('bench_result')
        
        end_time = time.time()
        latency = (end_time - start_time) * 1000
        
        # Cleanup
        self.db.exam_attempts.delete_many({'student_id': 'STU_BENCH'})
        self.redis_client.delete('bench_result')
        
        self.results['end_to_end_latency_ms'] = latency
        print(f"  Total Latency: {latency:.2f} ms")
    
    def get_system_metrics(self):
        """Get current system metrics"""
        print("\n6. Current System Metrics")
        print("-" * 50)
        
        # MongoDB stats
        stats = self.db.command('dbstats')
        self.results['mongodb_size_mb'] = stats['dataSize'] / (1024 * 1024)
        self.results['mongodb_collections'] = stats['collections']
        self.results['mongodb_objects'] = stats['objects']
        
        print(f"  MongoDB Size: {self.results['mongodb_size_mb']:.2f} MB")
        print(f"  Total Documents: {self.results['mongodb_objects']:,}")
        print(f"  Collections: {self.results['mongodb_collections']}")
        
        # Redis stats
        redis_info = self.redis_client.info()
        self.results['redis_used_memory_mb'] = redis_info['used_memory'] / (1024 * 1024)
        self.results['redis_connected_clients'] = redis_info['connected_clients']
        
        print(f"  Redis Memory: {self.results['redis_used_memory_mb']:.2f} MB")
        print(f"  Redis Clients: {self.results['redis_connected_clients']}")
    
    def run_all_benchmarks(self):
        """Run all benchmark tests"""
        print("=" * 70)
        print("PERFORMANCE BENCHMARK SUITE")
        print("=" * 70)
        
        self.benchmark_mongodb_read()
        self.benchmark_mongodb_aggregation()
        self.benchmark_redis_cache()
        self.benchmark_mongodb_write()
        self.benchmark_end_to_end_latency()
        self.get_system_metrics()
        
        # Generate report
        print("\n" + "=" * 70)
        print("BENCHMARK SUMMARY")
        print("=" * 70)
        
        print("\nPerformance Targets:")
        print("  ✓ MongoDB Read: < 100ms ✓" if self.results['mongodb_read_avg_ms'] < 100 else "  ✗ MongoDB Read: >= 100ms")
        print("  ✓ MongoDB Aggregation: < 500ms ✓" if self.results['mongodb_agg_avg_ms'] < 500 else "  ✗ MongoDB Aggregation: >= 500ms")
        print("  ✓ Redis Cache: < 5ms ✓" if self.results['redis_read_avg_ms'] < 5 else "  ✗ Redis Cache: >= 5ms")
        print("  ✓ End-to-End: < 1000ms ✓" if self.results['end_to_end_latency_ms'] < 1000 else "  ✗ End-to-End: >= 1000ms")
        print("  ✓ MongoDB Size: >= 300MB ✓" if self.results['mongodb_size_mb'] >= 300 else "  ✗ MongoDB Size: < 300MB")
        
        return self.results

if __name__ == "__main__":
    benchmark = PerformanceBenchmark()
    results = benchmark.run_all_benchmarks()
    
    # Save results
    import json
    with open('/data/benchmark_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n✅ Results saved to /data/benchmark_results.json")
