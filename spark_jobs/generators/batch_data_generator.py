"""
Batch Historical Data Generator - FIXED VERSION
Phase 5 - Step 16: Generate 250+ MB of historical exam data

FIXES:
1. Proper student profile mapping
2. Increased attempts per student to reach 250 MB
3. Proper AI cheater and colluding student distribution
"""

import sys
import subprocess

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

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

import numpy as np
import random
from datetime import datetime, timedelta
import time

# Import our custom modules
sys.path.append('/spark/jobs')
from models.student_profiles import (
    NormalStudent, AIAssistedCheater, ColludingStudent
)
from utils.hash_generator import AnswerHashGenerator

# MongoDB connection
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'

# Faker instance
fake = Faker()

# Configuration - INCREASED TO REACH 250 MB
TARGET_EXAM_ATTEMPTS = 400000  # Increased from 200,000
TARGET_SESSION_LOGS = 80000     # Increased from 40,000
BATCH_SIZE = 1000
TARGET_SIZE_MB = 250


class BatchDataGenerator:
    """Generate large-scale historical data efficiently"""

    def __init__(self):
        print("Connecting to MongoDB...")
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]

        # Verify dimension data exists
        self.verify_dimensions()

        # Load dimension data into memory
        self.load_dimensions()

        # Create student profiles - FIXED
        self.create_student_profiles()

        # Initialize hash generator
        self.hash_gen = AnswerHashGenerator()
        
        # Counter for unique IDs
        self.attempt_counter = 1
        self.session_counter = 1

    def verify_dimensions(self):
        counts = {
            'students': self.db.students.count_documents({}),
            'courses': self.db.courses.count_documents({}),
            'exams': self.db.exams.count_documents({}),
            'questions': self.db.questions.count_documents({})
        }

        print("\nDimension data counts:")
        for table, count in counts.items():
            print(f"  {table}: {count}")

        if any(count == 0 for count in counts.values()):
            print("\n❌ ERROR: Missing dimension data!")
            print("Run dimension_generator.py first!")
            sys.exit(1)

        print("✓ All dimensions verified\n")

    def load_dimensions(self):
        print("Loading dimension data into memory...")

        self.students_list = list(self.db.students.find({}, {'_id': 0}))
        self.courses_list = list(self.db.courses.find({}, {'_id': 0}))
        self.exams_list = list(self.db.exams.find({}, {'_id': 0}))
        self.questions_list = list(self.db.questions.find({}, {'_id': 0}))

        self.questions_by_exam = {}
        for question in self.questions_list:
            exam_id = question['exam_id']
            self.questions_by_exam.setdefault(exam_id, []).append(question)

        print(f"✓ Loaded {len(self.students_list)} students")
        print(f"✓ Loaded {len(self.exams_list)} exams")
        print(f"✓ Loaded {len(self.questions_list)} questions\n")

    def create_student_profiles(self):
        """FIXED: Create profiles using actual student IDs from MongoDB"""
        print("Creating student behavioral profiles...")
        
        self.profiles = {}
        
        # Shuffle students for randomization
        shuffled_students = self.students_list.copy()
        random.shuffle(shuffled_students)
        
        # Calculate distribution
        total = len(shuffled_students)
        normal_count = int(total * 0.70)      # 70%
        ai_count = int(total * 0.20)          # 20%
        colluding_count = total - normal_count - ai_count  # Remaining ~10%
        
        # Assign profiles
        idx = 0
        
        # Normal students (70%)
        for i in range(normal_count):
            student = shuffled_students[idx]
            self.profiles[student['student_id']] = NormalStudent(
                student['student_id'], 
                student['skill_level']
            )
            idx += 1
        
        # AI-assisted cheaters (20%)
        for i in range(ai_count):
            student = shuffled_students[idx]
            self.profiles[student['student_id']] = AIAssistedCheater(
                student['student_id'],
                student['skill_level']
            )
            idx += 1
        
        # Colluding students (10%) in groups of 2-4
        group_id = 1
        while idx < total:
            group_size = min(random.randint(2, 4), total - idx)
            for _ in range(group_size):
                student = shuffled_students[idx]
                self.profiles[student['student_id']] = ColludingStudent(
                    student['student_id'],
                    student['skill_level'],
                    collusion_group_id=f'GROUP_{group_id}'
                )
                idx += 1
            group_id += 1
        
        # Verify distribution
        normal = sum(1 for p in self.profiles.values() if p.profile_type == 'normal')
        ai = sum(1 for p in self.profiles.values() if p.profile_type == 'ai_assisted')
        colluding = sum(1 for p in self.profiles.values() if p.profile_type == 'colluding')
        
        print("✓ Profile distribution:")
        print(f"  Normal: {normal} ({normal/len(self.profiles)*100:.1f}%)")
        print(f"  AI-Assisted: {ai} ({ai/len(self.profiles)*100:.1f}%)")
        print(f"  Colluding: {colluding} ({colluding/len(self.profiles)*100:.1f}%)\n")

    def generate_answer_text(self, question_type, is_correct):
        if question_type == 'MCQ':
            return random.choice(['A', 'B', 'C', 'D']) if is_correct else random.choice(['X', 'Y'])
        elif question_type == 'true-false':
            return random.choice(['True', 'False'])
        elif question_type == 'short-answer':
            words = random.randint(10, 30) if is_correct else random.randint(5, 15)
            return fake.sentence(nb_words=words)
        elif question_type == 'essay':
            paragraphs = random.randint(2, 5) if is_correct else random.randint(1, 3)
            return '\n\n'.join([fake.paragraph(nb_sentences=5) for _ in range(paragraphs)])
        elif question_type == 'coding':
            if is_correct:
                return f"def solution():\n    # Correct implementation\n    {fake.sentence()}\n    return result"
            else:
                return f"def solution():\n    # Incomplete\n    {fake.sentence()}"
        return fake.sentence()

    def generate_exam_attempt(self, student, exam, question, submission_time):
        profile = self.profiles[student['student_id']]

        response_time = profile.generate_response_time(question['difficulty_level'])
        is_correct = profile.generate_accuracy(question['difficulty_level'])
        answer_text = self.generate_answer_text(question['question_type'], is_correct)

        if profile.profile_type == 'colluding':
            answer_hash = profile.generate_shared_answer_hash(
                question['question_id'], 
                str(random.randint(1, 100))
            )
        else:
            answer_hash = self.hash_gen.generate_hash(
                f"{student['student_id']}_{question['question_id']}_{answer_text}"
            )

        ai_usage_score = profile.generate_ai_score()
        answer_length = len(answer_text)
        keystroke_count = profile.generate_keystroke_count(answer_length)
        paste_count = profile.generate_paste_count()

        marks_obtained = question['max_marks'] if is_correct else (
            random.randint(0, question['max_marks'] // 2) if random.random() < 0.3 else 0
        )

        flagged = ai_usage_score > 0.85 or response_time < 15 or paste_count > 5

        attempt_id = f'ATT{str(self.attempt_counter).zfill(8)}'
        self.attempt_counter += 1

        return {
            'attempt_id': attempt_id,
            'student_id': student['student_id'],
            'exam_id': exam['exam_id'],
            'question_id': question['question_id'],
            'response_time': float(response_time),
            'is_correct': bool(is_correct),
            'ai_usage_score': float(ai_usage_score),
            'answer_hash': answer_hash,
            'answer_text': answer_text,
            'submission_timestamp': submission_time,
            'keystroke_count': int(keystroke_count),
            'paste_count': int(paste_count),
            'marks_obtained': int(marks_obtained),
            'flagged': bool(flagged)
        }

    def generate_session_log(self, student, exam, session_start):
        profile = self.profiles[student['student_id']]
        session_duration = exam['duration_minutes'] * 60
        session_end = session_start + timedelta(seconds=session_duration)

        if profile.profile_type == 'ai_assisted':
            tab_switches, focus_loss_count = random.randint(15, 30), random.randint(10, 20)
            idle_time, copy_count = random.uniform(50, 150), random.randint(5, 10)
        elif profile.profile_type == 'colluding':
            tab_switches, focus_loss_count = random.randint(8, 15), random.randint(5, 12)
            idle_time, copy_count = random.uniform(30, 100), random.randint(2, 6)
        else:
            tab_switches, focus_loss_count = int(np.random.poisson(3)), int(np.random.poisson(2))
            idle_time, copy_count = float(np.random.exponential(20)), random.randint(0, 2)

        screenshot_attempts = 0 if exam.get('proctored', False) else random.randint(0, 2)
        warnings_issued = sum([tab_switches > 10, focus_loss_count > 8, screenshot_attempts > 0])

        log_id = f'LOG{str(self.session_counter).zfill(8)}'
        self.session_counter += 1

        return {
            'log_id': log_id,
            'student_id': student['student_id'],
            'exam_id': exam['exam_id'],
            'tab_switches': int(tab_switches),
            'idle_time_seconds': float(idle_time),
            'focus_loss_count': int(focus_loss_count),
            'copy_count': int(copy_count),
            'screenshot_attempts': int(screenshot_attempts),
            'device_type': random.choice(['Desktop', 'Laptop', 'Tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'ip_address': fake.ipv4(),
            'session_start': session_start,
            'session_end': session_end,
            'warnings_issued': int(warnings_issued)
        }

    def generate_historical_data(self):
        print("="*70)
        print("GENERATING HISTORICAL DATA")
        print("="*70)
        print(f"\nTarget: {TARGET_EXAM_ATTEMPTS:,} exam attempts")
        print(f"Target: {TARGET_SESSION_LOGS:,} session logs")
        print(f"Target size: {TARGET_SIZE_MB} MB\n")

        print("Clearing existing fact data...")
        self.db.exam_attempts.delete_many({})
        self.db.session_logs.delete_many({})
        print("✓ Cleared\n")

        exam_attempts_batch, session_logs_batch = [], []
        start_time = time.time()

        # INCREASED: Each student takes more exams to reach 400k attempts
        attempts_per_student = TARGET_EXAM_ATTEMPTS // len(self.students_list)
        exams_per_student = max(10, attempts_per_student // 40)  # ~40 questions per exam

        for student_idx, student in enumerate(self.students_list):
            # More exams per student
            num_exams = min(exams_per_student, len(self.exams_list))
            student_exams = random.sample(self.exams_list, num_exams)

            for exam in student_exams:
                questions = self.questions_by_exam.get(exam['exam_id'], [])
                if not questions:
                    continue

                days_ago = random.randint(1, 60)
                session_start = datetime.now() - timedelta(
                    days=days_ago, 
                    hours=random.randint(8, 20), 
                    minutes=random.randint(0, 59)
                )

                session_log = self.generate_session_log(student, exam, session_start)
                session_logs_batch.append(session_log)

                # Answer more questions per exam
                num_questions_answered = random.randint(
                    int(len(questions) * 0.8),  # 80% minimum
                    len(questions)  # 100% maximum
                )
                answered_questions = random.sample(questions, num_questions_answered)

                for question in answered_questions:
                    submission_offset = random.randint(0, exam['duration_minutes']*60)
                    submission_time = session_start + timedelta(seconds=submission_offset)

                    attempt = self.generate_exam_attempt(student, exam, question, submission_time)
                    exam_attempts_batch.append(attempt)

                    # Insert in batches
                    if len(exam_attempts_batch) >= BATCH_SIZE:
                        try:
                            self.db.exam_attempts.insert_many(exam_attempts_batch)
                            exam_attempts_batch = []
                        except Exception as e:
                            print(f"Error: {e}")

                    if len(session_logs_batch) >= BATCH_SIZE // 5:
                        try:
                            self.db.session_logs.insert_many(session_logs_batch)
                            session_logs_batch = []
                        except Exception as e:
                            print(f"Error: {e}")

                # Check if we've reached target
                if self.attempt_counter >= TARGET_EXAM_ATTEMPTS:
                    break

            if (student_idx+1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = self.attempt_counter / elapsed if elapsed > 0 else 0
                eta = (TARGET_EXAM_ATTEMPTS - self.attempt_counter) / rate / 60 if rate > 0 else 0
                print(f"Progress: {self.attempt_counter:,}/{TARGET_EXAM_ATTEMPTS:,} attempts "
                      f"({self.attempt_counter/TARGET_EXAM_ATTEMPTS*100:.1f}%) | "
                      f"Rate: {rate:.0f}/s | ETA: {eta:.1f}min")

            if self.attempt_counter >= TARGET_EXAM_ATTEMPTS:
                break

        # Insert remaining batches
        if exam_attempts_batch:
            self.db.exam_attempts.insert_many(exam_attempts_batch)
        if session_logs_batch:
            self.db.session_logs.insert_many(session_logs_batch)

        total_time = time.time() - start_time
        
        print("\n" + "="*70)
        print("GENERATION COMPLETE")
        print("="*70)
        
        actual_attempts = self.db.exam_attempts.count_documents({})
        actual_sessions = self.db.session_logs.count_documents({})
        
        print(f"\nRecords created:")
        print(f"  Exam Attempts: {actual_attempts:,}")
        print(f"  Session Logs: {actual_sessions:,}")
        print(f"\nTime taken: {total_time/60:.1f} minutes")
        print(f"Rate: {actual_attempts/total_time:.0f} attempts/second")
        
        self.calculate_data_size()

    def calculate_data_size(self):
        print("\n" + "="*70)
        print("DATA SIZE CALCULATION")
        print("="*70)
        
        stats = self.db.command('dbStats')
        total_size_mb = stats.get('dataSize', 0) / (1024 * 1024)
        
        print(f"\nTotal database size: {total_size_mb:.2f} MB")
        
        for collection in ['students', 'courses', 'exams', 'questions', 
                          'exam_attempts', 'session_logs']:
            col_stats = self.db.command('collStats', collection)
            size_mb = col_stats.get('size', 0) / (1024 * 1024)
            count = col_stats.get('count', 0)
            avg_size = col_stats.get('avgObjSize', 0)
            
            print(f"\n{collection}:")
            print(f"  Count: {count:,}")
            print(f"  Size: {size_mb:.2f} MB")
            print(f"  Avg document: {avg_size:,} bytes")
        
        if total_size_mb >= TARGET_SIZE_MB:
            print(f"\n✓ Target size {TARGET_SIZE_MB} MB ACHIEVED!")
        else:
            print(f"\n⚠ Size {total_size_mb:.1f} MB below target {TARGET_SIZE_MB} MB")
        
        print("="*70)


def main():
    print("="*70)
    print("BATCH HISTORICAL DATA GENERATOR - FIXED")
    print("Phase 5 - Step 16")
    print("="*70)

    generator = BatchDataGenerator()
    generator.generate_historical_data()
    
    print("\n✓ Batch generation complete!")
    print("="*70)


if __name__ == "__main__":
    main()
