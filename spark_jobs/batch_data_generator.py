"""
Batch Data Generator for Historical Data (250 MB baseline)
Phase 5 - Step 16: Generate large volume of exam attempts
"""

import sys
import os
sys.path.append('/spark/jobs')

from models.student_profiles import create_student_profile, SessionBehaviorGenerator
from utils.distributions import DistributionGenerator
from utils.hash_generator import AnswerHashGenerator
from pymongo import MongoClient
from faker import Faker
import numpy as np
from datetime import datetime, timedelta
import time

fake = Faker()
np.random.seed(42)

MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
TARGET_SIZE_MB = 250
ESTIMATED_RECORD_SIZE_KB = 1.2  # exam_attempt size
TARGET_RECORDS = int((TARGET_SIZE_MB * 1024) / ESTIMATED_RECORD_SIZE_KB)

print(f"""
{'=' * 70}
BATCH DATA GENERATOR - Phase 5
Target: {TARGET_SIZE_MB} MB = {TARGET_RECORDS:,} exam attempts
{'=' * 70}
""")

def convert_numpy(obj):
    """Recursively convert numpy types to native Python types."""
    if isinstance(obj, dict):
        return {k: convert_numpy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    else:
        return obj

def generate_historical_data():
    """Generate 250 MB of historical exam data"""
    
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client['academic_integrity']
    
    # Fetch dimension data
    print("Loading dimension data...")
    students = list(db.students.find())
    exams = list(db.exams.find())
    
    if not students or not exams:
        print("❌ Error: Dimension data not found. Run dimension_generator.py first!")
        return
    
    print(f"  Students: {len(students)}")
    print(f"  Exams: {len(exams)}")
    
    # Assign profiles to students (75% normal, 15% AI, 10% colluding)
    student_profiles = {}
    for student in students:
        profile_type = np.random.choice(
            ['normal', 'ai_cheater', 'colluding'],
            p=[0.75, 0.15, 0.10]
        )
        student_profiles[student['student_id']] = {
            'profile': create_student_profile(
                student['student_id'],
                student['skill_level'],
                profile_type
            ),
            'type': profile_type
        }
    
    print(f"\n Profile distribution:")
    profile_counts = {}
    for s_id, data in student_profiles.items():
        ptype = data['type']
        profile_counts[ptype] = profile_counts.get(ptype, 0) + 1
    for ptype, count in profile_counts.items():
        print(f"  {ptype}: {count} ({count/len(students)*100:.1f}%)")
    
    print(f"\nGenerating {TARGET_RECORDS:,} exam attempts...")
    print("This will take 10-15 minutes...\n")
    
    attempts = []
    session_logs = []
    batch_size = 1000
    total_generated = 0
    start_time = time.time()
    
    # Select random exams for historical data
    selected_exams = np.random.choice(exams, size=min(20, len(exams)), replace=False)
    
    for exam in selected_exams:
        exam_id = exam['exam_id']
        exam_date = exam['exam_date']
        duration = exam['duration_minutes']
        
        # Get questions for this exam
        questions = list(db.questions.find({'exam_id': exam_id}))
        
        # Select random students for this exam (200-500 per exam)
        n_students_taking = np.random.randint(200, 501)
        selected_students = np.random.choice(students, size=n_students_taking, replace=False)
        
        print(f"Exam {exam_id}: {n_students_taking} students, {len(questions)} questions")
        
        for student in selected_students:
            student_id = student['student_id']
            profile_data = student_profiles[student_id]
            profile = profile_data['profile']
            profile_type = profile_data['type']
            
            # Generate session log
            session_log = {
                'log_id': f"LOG{total_generated:09d}",
                'student_id': student_id,
                'exam_id': exam_id,
                'tab_switches': int(SessionBehaviorGenerator.generate_tab_switches(profile_type)),
                'idle_time_seconds': float(SessionBehaviorGenerator.generate_idle_time(duration, profile_type)),
                'focus_loss_count': int(SessionBehaviorGenerator.generate_focus_loss(profile_type)),
                'copy_count': int(np.random.poisson(2)),
                'screenshot_attempts': int(np.random.poisson(0.5)),
                'device_type': None,
                'browser': None,
                'ip_address': fake.ipv4(),
                'session_start': exam_date,
                'session_end': exam_date + timedelta(minutes=duration),
                'warnings_issued': int(np.random.choice([0, 1, 2], p=[0.8, 0.15, 0.05]))
            }
            
            device, browser = SessionBehaviorGenerator.generate_device_browser()
            session_log['device_type'] = device
            session_log['browser'] = browser
            
            session_logs.append(convert_numpy(session_log))
            
            # Generate attempts for each question
            for question in questions:
                attempt_id = f"ATT{total_generated:09d}"
                
                # Generate response
                response_time = profile.generate_response_time(question['difficulty_level'])
                is_correct = profile.generate_accuracy(question['difficulty_level'])
                ai_score = profile.generate_ai_score()
                
                # Generate answer text
                answer_text = fake.paragraph(nb_sentences=np.random.randint(2, 8))
                answer_hash = AnswerHashGenerator.generate_hash(answer_text)
                
                # Keystroke patterns
                keystrokes = profile.generate_keystrokes(len(answer_text))
                paste_count = profile.generate_paste_count()
                
                # Submission time
                time_offset = np.random.uniform(0, duration * 60)
                submission_time = exam_date + timedelta(seconds=time_offset)
                
                attempt = {
                    'attempt_id': attempt_id,
                    'student_id': student_id,
                    'exam_id': exam_id,
                    'question_id': question['question_id'],
                    'response_time': float(response_time),
                    'is_correct': bool(is_correct),
                    'ai_usage_score': float(ai_score),
                    'answer_hash': answer_hash,
                    'answer_text': answer_text,
                    'submission_timestamp': submission_time,
                    'keystroke_count': int(keystrokes),
                    'paste_count': int(paste_count),
                    'marks_obtained': int(question['max_marks'] if is_correct else np.random.randint(0, question['max_marks'])),
                    'flagged': ai_score > 0.7
                }
                
                attempts.append(convert_numpy(attempt))
                total_generated += 1
                
                # Batch insert
                if len(attempts) >= batch_size:
                    db.exam_attempts.insert_many(attempts)
                    db.session_logs.insert_many(session_logs)
                    attempts = []
                    session_logs = []
                    
                    # Progress update
                    elapsed = time.time() - start_time
                    rate = total_generated / elapsed if elapsed > 0 else 0
                    eta = (TARGET_RECORDS - total_generated) / rate if rate > 0 else 0
                    
                    print(f"  Progress: {total_generated:,}/{TARGET_RECORDS:,} ({total_generated/TARGET_RECORDS*100:.1f}%) | "
                          f"Rate: {rate:.0f}/s | ETA: {eta/60:.1f} min")
                
                # Stop if target reached
                if total_generated >= TARGET_RECORDS:
                    break
            
            if total_generated >= TARGET_RECORDS:
                break
        
        if total_generated >= TARGET_RECORDS:
            break
    
    # Insert remaining records
    if attempts:
        db.exam_attempts.insert_many(attempts)
        db.session_logs.insert_many(session_logs)
    
    elapsed = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"GENERATION COMPLETE")
    print(f"{'=' * 70}")
    print(f"Total records: {total_generated:,}")
    print(f"Time taken: {elapsed/60:.1f} minutes")
    print(f"Rate: {total_generated/elapsed:.0f} records/second")
    
    # Verify data size
    stats = db.command('dbstats')
    size_mb = stats['dataSize'] / (1024 * 1024)
    print(f"\nDatabase size: {size_mb:.2f} MB")
    
    if size_mb >= TARGET_SIZE_MB:
        print(f"✓ TARGET ACHIEVED: {size_mb:.2f} MB >= {TARGET_SIZE_MB} MB")
    else:
        print(f"⚠ Warning: Size {size_mb:.2f} MB < target {TARGET_SIZE_MB} MB")
        print(f"  Run again to add more data")
    
    print(f"{'=' * 70}")

if __name__ == "__main__":
    generate_historical_data()
