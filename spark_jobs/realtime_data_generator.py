"""
Real-time Data Generator with High Velocity
Phase 6 - Step 20: 5000+ events/minute streaming
"""

import sys
sys.path.append('/spark/jobs')

from models.student_profiles import create_student_profile, SessionBehaviorGenerator
from utils.distributions import DistributionGenerator
from utils.hash_generator import AnswerHashGenerator
from kafka import KafkaProducer
from pymongo import MongoClient
from faker import Faker
import numpy as np
from datetime import datetime, timedelta
import json
import time
import signal

fake = Faker()
Faker.seed(42)
np.random.seed(42)

KAFKA_BROKER = 'kafka:9092'
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
TARGET_RATE = 5000  # events per minute
CONCURRENT_EXAMS = 10
STUDENTS_PER_EXAM = 500

running = True

def signal_handler(sig, frame):
    global running
    print("\n\nShutdown signal received...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

print(f"""
{'=' * 70}
REAL-TIME DATA GENERATOR - Phase 6
Target Rate: {TARGET_RATE} events/minute
Configuration:
  - {CONCURRENT_EXAMS} concurrent exams
  - {STUDENTS_PER_EXAM} students per exam
  - Realistic behavioral patterns
{'=' * 70}
""")

def convert_to_python_types(obj):
    """Recursively convert numpy types to native Python types for JSON/Kafka"""
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

def create_producer():
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5
            )
            print(f"✓ Connected to Kafka: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"✗ Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")

def load_metadata():
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    print("Loading metadata...")
    students = list(db.students.find())
    exams = list(db.exams.find().limit(CONCURRENT_EXAMS))
    
    exam_ids = [e['exam_id'] for e in exams]
    questions = {}
    for exam_id in exam_ids:
        questions[exam_id] = list(db.questions.find({'exam_id': exam_id}))
    
    print(f"  Students: {len(students)}")
    print(f"  Exams: {len(exams)}")
    print(f"  Questions per exam: {len(questions[exam_ids[0]])}")
    
    student_profiles = {}
    for student in students[:CONCURRENT_EXAMS * STUDENTS_PER_EXAM]:
        profile_type = np.random.choice(['normal', 'ai_cheater', 'colluding'], p=[0.75, 0.15, 0.10])
        student_profiles[student['student_id']] = {
            'profile': create_student_profile(student['student_id'], student['skill_level'], profile_type),
            'type': profile_type,
            'data': student
        }
    
    return students, exams, questions, student_profiles

def generate_exam_attempt(student_id, exam_id, question, profile, event_count):
    response_time = profile.generate_response_time(question['difficulty_level'])
    is_correct = profile.generate_accuracy(question['difficulty_level'])
    ai_score = profile.generate_ai_score()
    answer_text = fake.paragraph(nb_sentences=np.random.randint(2, 6))
    answer_hash = AnswerHashGenerator.generate_hash(answer_text)
    keystrokes = profile.generate_keystrokes(len(answer_text))
    paste_count = profile.generate_paste_count()
    
    attempt = {
        'attempt_id': f"ATT{event_count:09d}",
        'student_id': student_id,
        'exam_id': exam_id,
        'question_id': question['question_id'],
        'response_time': float(response_time),
        'is_correct': bool(is_correct),
        'ai_usage_score': float(ai_score),
        'answer_hash': answer_hash,
        'answer_text': answer_text[:500],
        'submission_timestamp': datetime.now().isoformat(),
        'keystroke_count': int(keystrokes),
        'paste_count': int(paste_count),
        'marks_obtained': int(question['max_marks'] if is_correct else np.random.randint(0, question['max_marks'])),
        'flagged': bool(ai_score > 0.7)
    }
    return convert_to_python_types(attempt)

def generate_session_log(student_id, exam_id, profile_type, event_count):
    device, browser = SessionBehaviorGenerator.generate_device_browser()
    
    session_log = {
        'log_id': f"LOG{event_count:09d}",
        'student_id': student_id,
        'exam_id': exam_id,
        'tab_switches': int(SessionBehaviorGenerator.generate_tab_switches(profile_type)),
        'idle_time_seconds': float(SessionBehaviorGenerator.generate_idle_time(120, profile_type)),
        'focus_loss_count': int(SessionBehaviorGenerator.generate_focus_loss(profile_type)),
        'copy_count': int(np.random.poisson(2)),
        'screenshot_attempts': int(np.random.poisson(0.5)),
        'device_type': device,
        'browser': browser,
        'ip_address': fake.ipv4(),
        'session_start': datetime.now().isoformat(),
        'session_end': (datetime.now() + timedelta(minutes=120)).isoformat(),
        'warnings_issued': int(np.random.choice([0, 1, 2], p=[0.8, 0.15, 0.05]))
    }
    return convert_to_python_types(session_log)

def main():
    global running
    
    producer = create_producer()
    students, exams, questions, student_profiles = load_metadata()
    
    print("\nStarting generation...")
    print("Press Ctrl+C to stop\n")
    
    event_count = 0
    start_time = time.time()
    last_report = time.time()
    
    try:
        while running:
            exam = np.random.choice(exams)
            exam_id = exam['exam_id']
            exam_questions = questions[exam_id]
            
            student_id = np.random.choice(list(student_profiles.keys()))
            profile_data = student_profiles[student_id]
            profile = profile_data['profile']
            profile_type = profile_data['type']
            
            question = np.random.choice(exam_questions)
            attempt = generate_exam_attempt(student_id, exam_id, question, profile, event_count)
            producer.send('exam_attempts', attempt)
            event_count += 1
            
            if event_count % 5 == 0:
                session_log = generate_session_log(student_id, exam_id, profile_type, event_count)
                producer.send('session_logs', session_log)
                event_count += 1
            
            time.sleep(0.012)
            
            now = time.time()
            if now - last_report >= 10:
                elapsed = now - start_time
                rate = event_count / elapsed * 60 if elapsed > 0 else 0
                print(f"✓ Generated {event_count:,} events | Rate: {rate:.0f}/min | Running time: {elapsed/60:.1f} min")
                last_report = now
    
    except KeyboardInterrupt:
        print("\n\nStopping generation...")
    
    finally:
        producer.flush()
        producer.close()
        
        elapsed = time.time() - start_time
        rate = event_count / elapsed * 60 if elapsed > 0 else 0
        print(f"\n{'=' * 70}")
        print(f"GENERATION STOPPED")
        print(f"{'=' * 70}")
        print(f"Total events: {event_count:,}")
        print(f"Time: {elapsed/60:.1f} minutes")
        print(f"Average rate: {rate:.0f} events/minute")
        print(f"{'=' * 70}")

if __name__ == "__main__":
    main()
