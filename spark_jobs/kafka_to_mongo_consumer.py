"""
Kafka to MongoDB Consumer
"""

import subprocess
import sys

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

KAFKA_BROKER = 'kafka:9092'
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'
TOPICS = ['exam_attempts', 'session_logs']

def create_mongo_connection():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            client = MongoClient(MONGO_URI)
            client.admin.command('ping')
            print(f"✓ Connected to MongoDB")
            return client
        except Exception as e:
            print(f"✗ MongoDB attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to MongoDB")

def create_kafka_consumer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='mongo-writer-group'
                # Removed consumer_timeout_ms to keep it running
            )
            print(f"✓ Connected to Kafka")
            print(f"✓ Subscribed to: {TOPICS}")
            return consumer
        except Exception as e:
            print(f"✗ Kafka attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")

def main():
    print("=" * 70)
    print("KAFKA TO MONGODB CONSUMER")
    print("=" * 70)
    
    mongo_client = create_mongo_connection()
    db = mongo_client[MONGO_DB]
    consumer = create_kafka_consumer()
    
    print("\nConsuming messages...")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            collection = db[topic]
            
            try:
                collection.insert_one(data)
                message_count += 1
                
                if message_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = message_count / elapsed * 60 if elapsed > 0 else 0
                    print(f"✓ Processed {message_count} messages | Rate: {rate:.0f} msg/min")
                    
            except Exception as e:
                print(f"✗ Error: {e}")
                
    except KeyboardInterrupt:
        print("\n\nStopping...")
        elapsed = time.time() - start_time
        print(f"\nStats:")
        print(f"  Messages: {message_count}")
        print(f"  Time: {elapsed:.1f}s")
        
        print(f"\nMongoDB Counts:")
        for topic in TOPICS:
            count = db[topic].count_documents({})
            print(f"  {topic}: {count}")
        
    finally:
        consumer.close()
        mongo_client.close()
        print("\n✓ Consumer closed")

if __name__ == "__main__":
    main()
