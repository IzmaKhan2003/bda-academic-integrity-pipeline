"""
Enhanced Kafka to MongoDB Consumer
Phase 6 - Step 22: High-performance streaming consumer
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
from datetime import datetime
import numpy as np

KAFKA_BROKER = 'kafka:9092'
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
TOPICS = ['exam_attempts', 'session_logs']
BATCH_SIZE = 100  # Batch inserts for performance

print(f"""
{'=' * 70}
KAFKA TO MONGODB CONSUMER (Enhanced)
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
    # Kafka
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='mongo-writer-enhanced',
        max_poll_records=BATCH_SIZE
    )
    
    # MongoDB
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("✓ Connections established\n")
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

# ---------------- Main Consumer ---------------- #

def main():
    consumer, db = create_connections()
    
    print("Consuming messages (batch mode)...\n")
    
    message_count = 0
    start_time = time.time()
    last_report = time.time()
    
    # Buffers for batch inserts
    buffers = {topic: [] for topic in TOPICS}
    
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            # Convert timestamps
            data = convert_timestamps(data)
            
            # Convert numpy types to native Python types
            data = convert_to_python_types(data)
            
            # Add to buffer
            buffers[topic].append(data)
            message_count += 1
            
            # Batch insert when buffer is full
            if len(buffers[topic]) >= BATCH_SIZE:
                db[topic].insert_many(buffers[topic])
                buffers[topic] = []
            
            # Report every 10 seconds
            now = time.time()
            if now - last_report >= 10:
                elapsed = now - start_time
                rate = message_count / elapsed * 60 if elapsed > 0 else 0
                
                # Get MongoDB counts
                counts = {t: db[t].count_documents({}) for t in TOPICS}
                
                print(f"✓ Processed {message_count:,} | Rate: {rate:.0f}/min | "
                      f"MongoDB: {counts['exam_attempts']:,} attempts, {counts['session_logs']:,} logs")
                last_report = now
    
    except KeyboardInterrupt:
        print("\n\nStopping consumer...")
    
    finally:
        # Flush remaining buffers
        for topic, buffer in buffers.items():
            if buffer:
                db[topic].insert_many(buffer)
                print(f"✓ Flushed {len(buffer)} {topic} records")
        
        consumer.close()
        
        elapsed = time.time() - start_time
        rate = message_count / elapsed * 60 if elapsed > 0 else 0
        
        # Final MongoDB counts
        counts = {t: db[t].count_documents({}) for t in TOPICS}
        
        print(f"\n{'=' * 70}")
        print(f"CONSUMER STOPPED")
        print(f"{'=' * 70}")
        print(f"Total messages: {message_count:,}")
        print(f"Time: {elapsed/60:.1f} minutes")
        print(f"Average rate: {rate:.0f} messages/minute")
        print(f"\nMongoDB totals:")
        for t in TOPICS:
            print(f"  {t}: {counts[t]:,}")
        print(f"{'=' * 70}")

if __name__ == "__main__":
    main()
