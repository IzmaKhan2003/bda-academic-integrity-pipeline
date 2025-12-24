"""
Simple Data Generator for Week 1 Testing
"""

import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

try:
    from kafka import KafkaProducer
except ImportError:
    print("Installing kafka-python...")
    install_package('kafka-python')
    from kafka import KafkaProducer

try:
    from faker import Faker
except ImportError:
    print("Installing faker...")
    install_package('faker')
    from faker import Faker

import json
import time
import random
from datetime import datetime

fake = Faker()

KAFKA_BROKER = 'kafka:9092'
TOPIC_EXAM_ATTEMPTS = 'exam_attempts'
TOPIC_SESSION_LOGS = 'session_logs'

def create_producer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✓ Connected to Kafka broker: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"✗ Connection attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")

def generate_exam_attempt():
    return {
        'attempt_id': f'ATT{random.randint(10000, 99999)}',
        'student_id': f'STU{random.randint(1000, 9999)}',
        'exam_id': f'EXM{random.randint(100, 999)}',
        'question_id': f'QST{random.randint(1000, 9999)}',
        'response_time': round(random.uniform(10, 300), 2),
        'is_correct': random.choice([True, False]),
        'ai_usage_score': round(random.uniform(0.0, 1.0), 2),
        'answer_hash': fake.sha256(),
        'answer_text': fake.sentence(nb_words=20),
        'submission_timestamp': datetime.now().isoformat(),
        'keystroke_count': random.randint(50, 500),
        'paste_count': random.randint(0, 10)
    }

def generate_session_log():
    return {
        'log_id': f'LOG{random.randint(10000, 99999)}',
        'student_id': f'STU{random.randint(1000, 9999)}',
        'exam_id': f'EXM{random.randint(100, 999)}',
        'tab_switches': random.randint(0, 20),
        'idle_time_seconds': round(random.uniform(0, 600), 2),
        'focus_loss_count': random.randint(0, 15),
        'device_type': random.choice(['Desktop', 'Laptop', 'Tablet']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
        'ip_address': fake.ipv4(),
        'session_start': datetime.now().isoformat(),
        'session_end': datetime.now().isoformat()
    }

def main():
    print("=" * 70)
    print("SIMPLE DATA GENERATOR - Week 1 Testing")
    print("=" * 70)
    
    producer = create_producer()
    
    print("\nStarting data generation...")
    print(f"Target rate: 100 events/minute")
    print(f"Topics: {TOPIC_EXAM_ATTEMPTS}, {TOPIC_SESSION_LOGS}")
    print("\nPress Ctrl+C to stop\n")
    
    event_count = 0
    start_time = time.time()
    
    try:
        while True:
            exam_attempt = generate_exam_attempt()
            producer.send(TOPIC_EXAM_ATTEMPTS, exam_attempt)
            event_count += 1
            
            if event_count % 5 == 0:
                session_log = generate_session_log()
                producer.send(TOPIC_SESSION_LOGS, session_log)
                event_count += 1
            
            if event_count % 50 == 0:
                elapsed = time.time() - start_time
                rate = event_count / elapsed * 60
                print(f"✓ Generated {event_count} events | Rate: {rate:.0f} events/min")
            
            time.sleep(0.6)
            
    except KeyboardInterrupt:
        print("\n\nStopping data generation...")
        elapsed = time.time() - start_time
        print(f"\nFinal Statistics:")
        print(f"  Total events: {event_count}")
        print(f"  Total time: {elapsed:.1f}s")
        print(f"  Avg rate: {event_count / elapsed * 60:.0f} events/min")
        
    finally:
        producer.flush()
        producer.close()
        print("\n✓ Producer closed successfully")

if __name__ == "__main__":
    main()
