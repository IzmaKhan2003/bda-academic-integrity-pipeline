"""
Enhanced Kafka to MongoDB Consumer
Phase 6 - Step 22: High-performance streaming consumer with batching

Features:
- Batch inserts for performance
- Error handling and retry logic
- Deduplication
- Monitoring and statistics
- Graceful shutdown
"""

import sys
import subprocess

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

try:
    from kafka import KafkaConsumer
except ImportError:
    print("Installing kafka-python...")
    install_package('kafka-python')
    from kafka import KafkaConsumer

try:
    from pymongo import MongoClient
except ImportError:
    print("Installing pymongo...")
    install_package('pymongo')
    from pymongo import MongoClient

import json
import time
from datetime import datetime
from collections import defaultdict, deque
import signal
import sys as system


# Configuration
KAFKA_BROKER = 'kafka:9092'
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'
TOPICS = ['exam_attempts', 'session_logs']

BATCH_SIZE = 100  # Insert every N messages
BATCH_TIMEOUT = 5  # Or every N seconds
MAX_RETRIES = 3


class EnhancedKafkaConsumer:
    """High-performance Kafka consumer with batching and monitoring"""
    
    def __init__(self):
        print("Initializing Enhanced Kafka Consumer...")
        
        # Connect to MongoDB
        self.mongo_client = self._connect_mongodb()
        self.db = self.mongo_client[MONGO_DB]
        
        # Connect to Kafka
        self.consumer = self._connect_kafka()
        
        # Batch buffers
        self.batches = {
            'exam_attempts': [],
            'session_logs': []
        }
        self.last_insert_time = {
            'exam_attempts': time.time(),
            'session_logs': time.time()
        }
        
        # Deduplication cache (rolling window)
        self.seen_ids = {
            'exam_attempts': deque(maxlen=10000),
            'session_logs': deque(maxlen=10000)
        }
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_inserted': 0,
            'duplicates_skipped': 0,
            'errors': 0,
            'batches_inserted': 0,
            'start_time': time.time()
        }
        
        # Graceful shutdown flag
        self.running = True
        self._setup_signal_handlers()
    
    def _connect_mongodb(self):
        """Connect to MongoDB with retries"""
        print("Connecting to MongoDB...")
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
                client.admin.command('ping')
                print(f"✓ MongoDB connected")
                return client
            except Exception as e:
                print(f"✗ Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
        
        raise Exception("Failed to connect to MongoDB")
    
    def _connect_kafka(self):
        """Connect to Kafka with retries"""
        print("Connecting to Kafka...")
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    *TOPICS,
                    bootstrap_servers=[KAFKA_BROKER],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='mongo-writer-enhanced',
                    max_poll_records=500,
                    session_timeout_ms=30000
                )
                print(f"✓ Kafka connected")
                print(f"✓ Subscribed to: {TOPICS}")
                return consumer
            except Exception as e:
                print(f"✗ Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka")
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        def signal_handler(sig, frame):
            print("\n\n🛑 Shutdown signal received...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _is_duplicate(self, collection, record_id):
        """Check if record ID has been seen recently"""
        if record_id in self.seen_ids[collection]:
            return True
        
        self.seen_ids[collection].append(record_id)
        return False
    
    def _insert_batch(self, collection_name):
        """Insert batched records to MongoDB"""
        batch = self.batches[collection_name]
        
        if not batch:
            return
        
        collection = self.db[collection_name]
        
        try:
            # Bulk insert
            result = collection.insert_many(batch, ordered=False)
            
            # Update statistics
            self.stats['messages_inserted'] += len(result.inserted_ids)
            self.stats['batches_inserted'] += 1
            
            # Clear batch
            self.batches[collection_name] = []
            self.last_insert_time[collection_name] = time.time()
            
            print(f"  ✓ Inserted batch: {len(result.inserted_ids)} → {collection_name}")
            
        except Exception as e:
            self.stats['errors'] += 1
            print(f"  ✗ Batch insert error: {e}")
            
            # Try individual inserts on failure
            self._fallback_individual_inserts(collection_name, batch)
    
    def _fallback_individual_inserts(self, collection_name, batch):
        """Fallback to individual inserts if batch fails"""
        print(f"  ⚠ Attempting individual inserts...")
        
        collection = self.db[collection_name]
        success_count = 0
        
        for record in batch:
            try:
                collection.insert_one(record)
                success_count += 1
                self.stats['messages_inserted'] += 1
            except Exception as e:
                self.stats['errors'] += 1
                # Continue with next record
        
        print(f"  ✓ Recovered {success_count}/{len(batch)} records")
        
        # Clear batch
        self.batches[collection_name] = []
    
    def _should_flush_batch(self, collection_name):
        """Check if batch should be flushed"""
        batch_size = len(self.batches[collection_name])
        time_elapsed = time.time() - self.last_insert_time[collection_name]
        
        return (
            batch_size >= BATCH_SIZE or
            (batch_size > 0 and time_elapsed >= BATCH_TIMEOUT)
        )
    
    def process_message(self, message):
        """Process a single message"""
        topic = message.topic
        data = message.value
        
        self.stats['messages_received'] += 1
        
        # Get record ID
        id_field = 'attempt_id' if topic == 'exam_attempts' else 'log_id'
        record_id = data.get(id_field)
        
        if not record_id:
            self.stats['errors'] += 1
            print(f"  ✗ Missing {id_field} in message")
            return
        
        # Check for duplicates
        if self._is_duplicate(topic, record_id):
            self.stats['duplicates_skipped'] += 1
            return
        
        # Parse datetime fields
        if 'submission_timestamp' in data and isinstance(data['submission_timestamp'], str):
            try:
                data['submission_timestamp'] = datetime.fromisoformat(data['submission_timestamp'].replace('Z', '+00:00'))
            except:
                pass
        
        if 'session_start' in data and isinstance(data['session_start'], str):
            try:
                data['session_start'] = datetime.fromisoformat(data['session_start'].replace('Z', '+00:00'))
            except:
                pass
        
        if 'session_end' in data and isinstance(data['session_end'], str):
            try:
                data['session_end'] = datetime.fromisoformat(data['session_end'].replace('Z', '+00:00'))
            except:
                pass
        
        # Add to batch
        self.batches[topic].append(data)
        
        # Check if should flush
        if self._should_flush_batch(topic):
            self._insert_batch(topic)
    
    def print_stats(self):
        """Print consumption statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate_received = self.stats['messages_received'] / elapsed * 60 if elapsed > 0 else 0
        rate_inserted = self.stats['messages_inserted'] / elapsed * 60 if elapsed > 0 else 0
        
        print(f"\r📊 Received: {self.stats['messages_received']:,} ({rate_received:.0f}/min) | "
              f"Inserted: {self.stats['messages_inserted']:,} ({rate_inserted:.0f}/min) | "
              f"Batches: {self.stats['batches_inserted']} | "
              f"Duplicates: {self.stats['duplicates_skipped']} | "
              f"Errors: {self.stats['errors']}", end='')
    
    def consume(self):
        """Main consumption loop"""
        print("\n" + "=" * 70)
        print("STARTING CONSUMPTION")
        print("=" * 70)
        print(f"\nConfiguration:")
        print(f"  Batch Size: {BATCH_SIZE}")
        print(f"  Batch Timeout: {BATCH_TIMEOUT}s")
        print(f"  Topics: {', '.join(TOPICS)}")
        print("\nConsuming messages...\n")
        
        last_stat_print = time.time()
        
        try:
            while self.running:
                # Poll messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                # Process messages
                for topic_partition, records in messages.items():
                    for message in records:
                        self.process_message(message)
                
                # Check batch timeouts
                for collection_name in self.batches.keys():
                    if self._should_flush_batch(collection_name):
                        self._insert_batch(collection_name)
                
                # Print stats every 5 seconds
                if time.time() - last_stat_print >= 5:
                    self.print_stats()
                    last_stat_print = time.time()
                
        except Exception as e:
            print(f"\n\n✗ Fatal error: {e}")
            self.stats['errors'] += 1
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        print("\n\n" + "=" * 70)
        print("SHUTTING DOWN")
        print("=" * 70)
        
        # Flush remaining batches
        print("\nFlushing remaining batches...")
        for collection_name in self.batches.keys():
            if self.batches[collection_name]:
                self._insert_batch(collection_name)
        
        # Final statistics
        elapsed = time.time() - self.stats['start_time']
        
        print(f"\nFinal Statistics:")
        print(f"  Messages Received: {self.stats['messages_received']:,}")
        print(f"  Messages Inserted: {self.stats['messages_inserted']:,}")
        print(f"  Batches Inserted: {self.stats['batches_inserted']}")
        print(f"  Duplicates Skipped: {self.stats['duplicates_skipped']}")
        print(f"  Errors: {self.stats['errors']}")
        print(f"  Duration: {elapsed/60:.1f} minutes")
        print(f"  Average Rate: {self.stats['messages_inserted']/elapsed*60:.0f} messages/min")
        
        # Verify MongoDB data
        print(f"\nMongoDB Verification:")
        for collection in ['exam_attempts', 'session_logs']:
            count = self.db[collection].count_documents({})
            print(f"  {collection}: {count:,} records")
        
        # Close connections
        self.consumer.close()
        self.mongo_client.close()
        
        print("\n✓ Cleanup complete")
        print("=" * 70)


def main():
    """Main execution"""
    print("=" * 70)
    print("ENHANCED KAFKA TO MONGODB CONSUMER")
    print("Phase 6 - Step 22")
    print("=" * 70)
    
    consumer = EnhancedKafkaConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
