"""
Real-Time High-Velocity Data Generator
Phase 6 - Step 20: Generate 5000+ events/minute streaming data

This generator produces realistic exam data at high velocity for
real-time analytics testing.

Target Rate: 5000-10000 events/minute
Concurrent Exams: 10
Students per Exam: 500
"""

import sys
import subprocess

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

try:
    from kafka import KafkaProducer
except ImportError:
    print("Installing kafka-python...")
    install_package('kafka-python')
    from kafka import KafkaProducer

try:
    from pymongo import MongoClient
except ImportError:
    print("Installing pymongo...")
    install_package('pymongo')
    from pymongo import MongoClient

try:
    from faker import Faker
except ImportError:
    print("Installing faker...")
    install_package('faker')
    from faker import Faker

import json
import time
import random
import numpy as np
from datetime import datetime, timedelta
from threading import Thread, Lock
from collections import defaultdict

# Import custom modules
sys.path.append('/spark/jobs')
from models.student_profiles import NormalStudent, AIAssistedCheater, ColludingStudent
from utils.hash_generator import AnswerHashGenerator

# Configuration
KAFKA_BROKER = 'kafka:9092'
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'

TOPIC_EXAM_ATTEMPTS = 'exam_attempts'
TOPIC_SESSION_LOGS = 'session_logs'

TARGET_RATE = 5000  # events per minute
CONCURRENT_EXAMS = 10
STUDENTS_PER_EXAM = 500

fake = Faker()
hash_gen = AnswerHashGenerator()


class ExamSession:
    """Represents a single ongoing exam session"""
    
    def __init__(self, exam, students, questions, profiles):
        self.exam = exam
        self.students = students
        self.questions = questions
        self.profiles = profiles
        
        self.session_start = datetime.now()
        self.duration_seconds = exam['duration_minutes'] * 60
        self.session_end = self.session_start + timedelta(seconds=self.duration_seconds)
        
        # Track student progress
        self.student_progress = {s['student_id']: 0 for s in students}
        self.student_sessions = {}
        
        # Generate session logs for all students
        self._initialize_sessions()
    
    def _initialize_sessions(self):
        """Create session log records for all students"""
        for student in self.students:
            student_id = student['student_id']
            profile = self.profiles.get(student_id)
            
            if not profile:
                continue
            
            # Session behavioral metrics
            if profile.profile_type == 'ai_assisted':
                tab_switches = random.randint(15, 30)
                focus_loss_count = random.randint(10, 20)
                idle_time = random.uniform(50, 150)
            elif profile.profile_type == 'colluding':
                tab_switches = random.randint(8, 15)
                focus_loss_count = random.randint(5, 12)
                idle_time = random.uniform(30, 100)
            else:
                tab_switches = int(np.random.poisson(3))
                focus_loss_count = int(np.random.poisson(2))
                idle_time = float(np.random.exponential(20))
            
            session_log = {
                'log_id': f'LOG{random.randint(100000, 999999)}',
                'student_id': student_id,
                'exam_id': self.exam['exam_id'],
                'tab_switches': int(tab_switches),
                'idle_time_seconds': float(idle_time),
                'focus_loss_count': int(focus_loss_count),
                'copy_count': random.randint(0, 5),
                'screenshot_attempts': 0,
                'device_type': random.choice(['Desktop', 'Laptop', 'Tablet']),
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                'ip_address': fake.ipv4(),
                'session_start': self.session_start.isoformat(),
                'session_end': self.session_end.isoformat(),
                'warnings_issued': 0
            }
            
            self.student_sessions[student_id] = session_log
    
    def is_active(self):
        """Check if exam session is still ongoing"""
        return datetime.now() < self.session_end
    
    def get_next_events(self, num_events):
        """Generate next batch of exam attempt events"""
        events = []
        
        for _ in range(num_events):
            # Select random student
            student = random.choice(self.students)
            student_id = student['student_id']
            
            # Check if student has questions left
            progress = self.student_progress[student_id]
            if progress >= len(self.questions):
                continue  # Student finished
            
            question = self.questions[progress]
            profile = self.profiles.get(student_id)
            
            if not profile:
                continue
            
            # Generate exam attempt
            response_time = profile.generate_response_time(
                question['difficulty_level'],
                question['expected_time_seconds']
            )
            
            is_correct = profile.calculate_accuracy(question['difficulty_level'])
            
            # Generate answer text
            answer_text = self._generate_answer_text(
                question['question_type'], 
                is_correct
            )
            
            # Generate answer hash
            if profile.profile_type == 'colluding':
                answer_hash = profile.generate_shared_answer_hash(
                    question['question_id'],
                    str(random.randint(1, 100))
                )
            else:
                answer_hash = hash_gen.generate_hash(
                    f"{student_id}_{question['question_id']}_{answer_text}"
                )
            
            ai_usage_score = profile.generate_ai_score()
            keystroke_count, paste_count = profile.generate_keystroke_pattern(len(answer_text))
            
            marks_obtained = question['max_marks'] if is_correct else 0
            flagged = ai_usage_score > 0.85 or response_time < 15
            
            attempt = {
                'attempt_id': f'ATT{random.randint(100000, 999999)}',
                'student_id': student_id,
                'exam_id': self.exam['exam_id'],
                'question_id': question['question_id'],
                'response_time': float(response_time),
                'is_correct': bool(is_correct),
                'ai_usage_score': float(ai_usage_score),
                'answer_hash': answer_hash,
                'answer_text': answer_text,
                'submission_timestamp': datetime.now().isoformat(),
                'keystroke_count': int(keystroke_count),
                'paste_count': int(paste_count),
                'marks_obtained': int(marks_obtained),
                'flagged': bool(flagged)
            }
            
            events.append(attempt)
            
            # Update progress
            self.student_progress[student_id] += 1
        
        return events
    
    def get_session_logs(self):
        """Get all session logs"""
        return list(self.student_sessions.values())
    
    def _generate_answer_text(self, question_type, is_correct):
        """Generate realistic answer text"""
        if question_type == 'MCQ':
            return random.choice(['A', 'B', 'C', 'D'])
        elif question_type == 'true-false':
            return random.choice(['True', 'False'])
        elif question_type == 'short-answer':
            return fake.sentence(nb_words=random.randint(10, 30))
        elif question_type == 'essay':
            return ' '.join([fake.paragraph() for _ in range(random.randint(2, 5))])
        elif question_type == 'coding':
            return f"def solution():\n    # {'Correct' if is_correct else 'Incomplete'}\n    return result"
        return fake.sentence()


class RealtimeDataGenerator:
    """High-velocity streaming data generator"""
    
    def __init__(self):
        print("Initializing Real-Time Data Generator...")
        
        # Connect to MongoDB
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[MONGO_DB]
        
        # Load dimension data
        self._load_dimensions()
        
        # Create student profiles
        self._create_profiles()
        
        # Connect to Kafka
        self.producer = self._create_kafka_producer()
        
        # Active exam sessions
        self.active_sessions = []
        self.sessions_lock = Lock()
        
        # Statistics
        self.stats = {
            'events_sent': 0,
            'start_time': None,
            'errors': 0
        }
    
    def _load_dimensions(self):
        """Load dimension data from MongoDB"""
        print("Loading dimension data...")
        
        self.students = list(self.db.students.find({}, {'_id': 0}))
        self.exams = list(self.db.exams.find({}, {'_id': 0}))
        
        # Create question lookup
        self.questions_by_exam = defaultdict(list)
        for question in self.db.questions.find({}, {'_id': 0}):
            self.questions_by_exam[question['exam_id']].append(question)
        
        print(f"✓ Loaded {len(self.students)} students")
        print(f"✓ Loaded {len(self.exams)} exams")
        print(f"✓ Loaded {len(self.questions_by_exam)} exam-question mappings")
    
    def _create_profiles(self):
        print("Creating student profiles...")

        self.profiles = {}

        shuffled_students = self.students.copy()
        random.shuffle(shuffled_students)

        total = len(shuffled_students)
        normal_count = int(total * 0.70)
        ai_count = int(total * 0.20)
        colluding_count = total - normal_count - ai_count

        idx = 0

        # Normal students
        for _ in range(normal_count):
            s = shuffled_students[idx]
            self.profiles[s['student_id']] = NormalStudent(
                s['student_id'], s['skill_level']
            )
            idx += 1

        # AI-assisted
        for _ in range(ai_count):
            s = shuffled_students[idx]
            self.profiles[s['student_id']] = AIAssistedCheater(
                s['student_id'], s['skill_level']
            )
            idx += 1

        # Colluding (grouped)
        group_id = 1
        while idx < total:
            group_size = min(random.randint(2, 4), total - idx)
            for _ in range(group_size):
                s = shuffled_students[idx]
                self.profiles[s['student_id']] = ColludingStudent(
                    s['student_id'],
                    s['skill_level'],
                    collusion_group_id=f'GROUP_{group_id}'
                )
                idx += 1
            group_id += 1

        print(f"✓ Created {len(self.profiles)} profiles")
    
    def _create_kafka_producer(self):
        """Create Kafka producer with retries"""
        print("Connecting to Kafka...")
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=5,
                    compression_type='gzip',
                    batch_size=16384,
                    linger_ms=10
                )
                print(f"✓ Connected to Kafka: {KAFKA_BROKER}")
                return producer
            except Exception as e:
                print(f"✗ Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka")
    
    def start_exam_session(self):
        """Start a new exam session"""
        # Select random exam
        exam = random.choice(self.exams)
        
        # Select random students
        selected_students = random.sample(
            self.students, 
            min(STUDENTS_PER_EXAM, len(self.students))
        )
        
        # Get questions for this exam
        questions = self.questions_by_exam.get(exam['exam_id'], [])
        
        if not questions:
            print(f"⚠ No questions for exam {exam['exam_id']}")
            return None
        
        # Create session
        session = ExamSession(exam, selected_students, questions, self.profiles)
        
        with self.sessions_lock:
            self.active_sessions.append(session)
        
        # Send session logs to Kafka
        for session_log in session.get_session_logs():
            self.producer.send(TOPIC_SESSION_LOGS, session_log)
        
        print(f"✓ Started exam session: {exam['exam_id']} "
              f"({len(selected_students)} students, {len(questions)} questions)")
        
        return session
    
    def generate_events(self, target_rate_per_minute):
        """Generate events at target rate"""
        events_per_second = target_rate_per_minute / 60
        sleep_time = 1.0 / events_per_second
        
        print(f"\nGenerating events at {target_rate_per_minute}/min "
              f"({events_per_second:.1f}/sec)...")
        
        self.stats['start_time'] = time.time()
        
        try:
            while True:
                with self.sessions_lock:
                    # Remove completed sessions
                    self.active_sessions = [s for s in self.active_sessions if s.is_active()]
                    
                    # Start new sessions if needed
                    while len(self.active_sessions) < CONCURRENT_EXAMS:
                        self.start_exam_session()
                    
                    # Generate events from active sessions
                    if self.active_sessions:
                        session = random.choice(self.active_sessions)
                        events = session.get_next_events(1)
                        
                        for event in events:
                            try:
                                self.producer.send(TOPIC_EXAM_ATTEMPTS, event)
                                self.stats['events_sent'] += 1
                            except Exception as e:
                                self.stats['errors'] += 1
                                print(f"✗ Send error: {e}")
                
                # Progress update
                if self.stats['events_sent'] % 1000 == 0:
                    self.print_stats()
                
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\n\nStopping generation...")
            self.cleanup()
    
    def print_stats(self):
        """Print generation statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['events_sent'] / elapsed * 60 if elapsed > 0 else 0
        
        print(f"\rEvents: {self.stats['events_sent']:,} | "
              f"Rate: {rate:.0f}/min | "
              f"Active sessions: {len(self.active_sessions)} | "
              f"Errors: {self.stats['errors']}", end='')
    
    def cleanup(self):
        """Cleanup resources"""
        print("\n\nFinal Statistics:")
        elapsed = time.time() - self.stats['start_time']
        avg_rate = self.stats['events_sent'] / elapsed * 60
        
        print(f"  Total events: {self.stats['events_sent']:,}")
        print(f"  Duration: {elapsed/60:.1f} minutes")
        print(f"  Average rate: {avg_rate:.0f} events/min")
        print(f"  Errors: {self.stats['errors']}")
        
        self.producer.flush()
        self.producer.close()
        self.mongo_client.close()
        
        print("\n✓ Cleanup complete")


def main():
    """Main execution"""
    print("=" * 70)
    print("REAL-TIME HIGH-VELOCITY DATA GENERATOR")
    print("Phase 6 - Step 20")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Target Rate: {TARGET_RATE} events/min")
    print(f"  Concurrent Exams: {CONCURRENT_EXAMS}")
    print(f"  Students per Exam: {STUDENTS_PER_EXAM}")
    print(f"  Kafka Broker: {KAFKA_BROKER}")
    print(f"  Topics: {TOPIC_EXAM_ATTEMPTS}, {TOPIC_SESSION_LOGS}")
    print("=" * 70)
    
    generator = RealtimeDataGenerator()
    generator.generate_events(TARGET_RATE)


if __name__ == "__main__":
    main()
