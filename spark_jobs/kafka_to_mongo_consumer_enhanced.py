"""
Enhanced Kafka to MongoDB Consumer - Process from Beginning
Use this version to consume ALL messages from Kafka topics
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import time
from datetime import datetime
import numpy as np

KAFKA_BROKER = 'kafka:9092'
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
TOPICS = ['exam_attempts', 'session_logs']
BATCH_SIZE = 100

print(f"""
{'=' * 70}
KAFKA TO MONGODB CONSUMER (Process from Beginning)
Topics: {', '.join(TOPICS)}
Batch Size: {BATCH_SIZE}
{'=' * 70}
""")

# ---------------- Helper Functions ---------------- #

def convert_to_python_types(obj):
    """Recursively convert numpy types to native Python types for MongoDB"""
    if isinstance(obj, dict):
        return {k: convert_to_python_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_python_types(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj

def create_connections():
    """Create Kafka and MongoDB connections"""
    # Kafka - Use 'earliest' to read from beginning and unique group_id
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # Start from beginning
        enable_auto_commit=True,
        group_id=f'mongo-writer-full-{int(time.time())}',  # Unique group ID
        max_poll_records=BATCH_SIZE,
        consumer_timeout_ms=10000  # Stop if no messages for 10 seconds
    )
    
    # MongoDB
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("✓ Connections established")
    
    # Show current MongoDB counts
    counts = {t: db[t].count_documents({}) for t in TOPICS}
    print(f"Current MongoDB counts:")
    for t in TOPICS:
        print(f"  {t}: {counts[t]:,}")
    print()
    
    return consumer, db

def convert_timestamps(data):
    """Convert ISO timestamp strings to datetime objects"""
    if 'submission_timestamp' in data:
        data['submission_timestamp'] = datetime.fromisoformat(data['submission_timestamp'].replace('Z', '+00:00'))
    if 'session_start' in data:
        data['session_start'] = datetime.fromisoformat(data['session_start'].replace('Z', '+00:00'))
    if 'session_end' in data:
        data['session_end'] = datetime.fromisoformat(data['session_end'].replace('Z', '+00:00'))
    return data

def safe_batch_insert(collection, documents):
    """
    Insert documents with duplicate key error handling
    Returns: (inserted_count, duplicate_count)
    """
    if not documents:
        return 0, 0
    
    try:
        result = collection.insert_many(documents, ordered=False)
        return len(result.inserted_ids), 0
    except BulkWriteError as e:
        inserted = e.details.get('nInserted', 0)
        duplicates = sum(1 for err in e.details.get('writeErrors', []) if err['code'] == 11000)
        other_errors = len(e.details.get('writeErrors', [])) - duplicates
        
        if other_errors > 0:
            print(f"⚠ Warning: {other_errors} non-duplicate errors in batch")
        
        return inserted, duplicates

# ---------------- Main Consumer ---------------- #

def main():
    consumer, db = create_connections()
    
    print("Processing all messages from topics...\n")
    
    message_count = 0
    inserted_count = 0
    duplicate_count = 0
    start_time = time.time()
    last_report = time.time()
    
    buffers = {topic: [] for topic in TOPICS}
    stats = {topic: {'inserted': 0, 'duplicates': 0} for topic in TOPICS}
    
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            data = convert_timestamps(data)
            data = convert_to_python_types(data)
            
            buffers[topic].append(data)
            message_count += 1
            
            # Batch insert when buffer is full
            if len(buffers[topic]) >= BATCH_SIZE:
                inserted, dups = safe_batch_insert(db[topic], buffers[topic])
                stats[topic]['inserted'] += inserted
                stats[topic]['duplicates'] += dups
                inserted_count += inserted
                duplicate_count += dups
                buffers[topic] = []
            
            # Report every 5 seconds or every 1000 messages
            now = time.time()
            if now - last_report >= 5 or message_count % 1000 == 0:
                elapsed = now - start_time
                rate = message_count / elapsed * 60 if elapsed > 0 else 0
                
                print(f"✓ Processed {message_count:,} | Rate: {rate:.0f}/min | "
                      f"Inserted: {inserted_count:,} | Duplicates: {duplicate_count:,}")
                last_report = now
    
    except KeyboardInterrupt:
        print("\n\nStopping consumer...")
    
    except Exception as e:
        print(f"\nConsumer stopped: {e}")
    
    finally:
        # Flush remaining buffers
        print("\nFlushing remaining buffers...")
        for topic, buffer in buffers.items():
            if buffer:
                inserted, dups = safe_batch_insert(db[topic], buffer)
                stats[topic]['inserted'] += inserted
                stats[topic]['duplicates'] += dups
                inserted_count += inserted
                duplicate_count += dups
                print(f"✓ {topic}: {len(buffer)} records ({inserted} inserted, {dups} duplicates)")
        
        consumer.close()
        
        elapsed = time.time() - start_time
        rate = message_count / elapsed * 60 if elapsed > 0 else 0
        
        counts = {t: db[t].count_documents({}) for t in TOPICS}
        
        print(f"\n{'=' * 70}")
        print(f"PROCESSING COMPLETE")
        print(f"{'=' * 70}")
        print(f"Total messages processed: {message_count:,}")
        print(f"Successfully inserted: {inserted_count:,}")
        print(f"Duplicates skipped: {duplicate_count:,}")
        print(f"Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"Average rate: {rate:.0f} messages/minute")
        print(f"\nPer-topic stats:")
        for t in TOPICS:
            print(f"  {t}:")
            print(f"    Inserted: {stats[t]['inserted']:,}")
            print(f"    Duplicates: {stats[t]['duplicates']:,}")
        print(f"\nFinal MongoDB totals:")
        for t in TOPICS:
            print(f"  {t}: {counts[t]:,}")
        print(f"{'=' * 70}")

if __name__ == "__main__":
    main()