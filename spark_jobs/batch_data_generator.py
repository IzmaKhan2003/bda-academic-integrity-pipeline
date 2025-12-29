"""
Batch Data Generator for Historical Data (250 MB baseline)
Fixed version that reliably generates 250+ MB of data
"""

import sys
import os
import json
import time
import logging
from datetime import datetime, timedelta

sys.path.append('/spark/jobs')

from models.student_profiles import create_student_profile, SessionBehaviorGenerator
from utils.distributions import DistributionGenerator
from utils.hash_generator import AnswerHashGenerator
from pymongo import MongoClient, errors
from faker import Faker
import numpy as np

# ================= CONFIGURATION =================
CONFIG = {
    "MONGO_URI": "mongodb://admin:admin123@mongodb:27017/",
    "TARGET_SIZE_MB": 250,
    "ESTIMATED_RECORD_SIZE_KB": 0.525,  # Adjusted based on actual data size
    "BATCH_SIZE": 1000,
    "PROFILE_PROBS": {"normal": 0.75, "ai_cheater": 0.15, "colluding": 0.10},
    "AI_FLAG_THRESHOLD": 0.7,
    "CHECKPOINT_FILE": "generation_checkpoint.json",
    "PROGRESS_PRINT_EVERY": 10000
}

# ================= SETUP =================
fake = Faker()
np.random.seed(42)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

TARGET_RECORDS = int((CONFIG["TARGET_SIZE_MB"] * 1024) / CONFIG["ESTIMATED_RECORD_SIZE_KB"])

# ================= UTILITIES =================
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

def load_checkpoint():
    if os.path.exists(CONFIG["CHECKPOINT_FILE"]):
        try:
            with open(CONFIG["CHECKPOINT_FILE"], "r") as f:
                data = json.load(f)
                # Ensure required keys exist
                if "total_attempts_generated" not in data:
                    data["total_attempts_generated"] = 0
                if "total_logs_generated" not in data:
                    data["total_logs_generated"] = 0
                return data
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Checkpoint file corrupted or invalid: {e}. Starting fresh.")
            return {"total_attempts_generated": 0, "total_logs_generated": 0}
    return {"total_attempts_generated": 0, "total_logs_generated": 0}

def save_checkpoint(total_attempts, total_logs):
    with open(CONFIG["CHECKPOINT_FILE"], "w") as f:
        json.dump({"total_attempts_generated": total_attempts, "total_logs_generated": total_logs}, f)

# ================= MAIN GENERATOR =================
def generate_historical_data():
    logger.info(f"Starting historical data generation: Target {CONFIG['TARGET_SIZE_MB']} MB (~{TARGET_RECORDS:,} records)")

    # Connect to MongoDB
    try:
        client = MongoClient(CONFIG["MONGO_URI"])
        db = client['academic_integrity']
    except errors.ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        return

    # Load dimension data
    students = list(db.students.find())
    exams = list(db.exams.find())
    questions_all = list(db.questions.find())
    
    if not students or not exams or not questions_all:
        logger.error("Dimension data not found. Run dimension_generator.py first!")
        return

    logger.info(f"Loaded: {len(students)} students, {len(exams)} exams, {len(questions_all)} questions")

    # Assign student profiles
    student_profiles = {}
    profile_counts = {}
    for student in students:
        profile_type = np.random.choice(
            list(CONFIG["PROFILE_PROBS"].keys()),
            p=list(CONFIG["PROFILE_PROBS"].values())
        )
        profile_counts[profile_type] = profile_counts.get(profile_type, 0) + 1
        student_profiles[student['student_id']] = {
            'profile': create_student_profile(student['student_id'], student['skill_level'], profile_type),
            'type': profile_type
        }

    logger.info("Profile distribution:")
    for ptype, count in profile_counts.items():
        logger.info(f"  {ptype}: {count} ({count/len(students)*100:.1f}%)")

    # Load checkpoint and verify against actual data
    checkpoint = load_checkpoint()
    
    # Verify checkpoint against actual database records
    actual_attempts_count = db.exam_attempts.count_documents({})
    actual_logs_count = db.session_logs.count_documents({})
    
    logger.info(f"Checkpoint says: {checkpoint['total_attempts_generated']:,} attempts, {checkpoint['total_logs_generated']:,} logs")
    logger.info(f"Database has: {actual_attempts_count:,} attempts, {actual_logs_count:,} logs")
    
    # If checkpoint doesn't match actual data, reset it
    if checkpoint['total_attempts_generated'] != actual_attempts_count:
        logger.warning("Checkpoint mismatch detected! Resetting to match actual database state.")
        checkpoint['total_attempts_generated'] = actual_attempts_count
        checkpoint['total_logs_generated'] = actual_logs_count
        save_checkpoint(actual_attempts_count, actual_logs_count)
    
    total_attempts_generated = checkpoint["total_attempts_generated"]
    total_logs_generated = checkpoint["total_logs_generated"]
    
    # Check if we've already reached the target
    if total_attempts_generated >= TARGET_RECORDS:
        logger.info(f"Target already reached ({total_attempts_generated:,} >= {TARGET_RECORDS:,})")
        logger.info("Checking actual database size...")
        try:
            db_stats = db.command("dbStats")
            actual_size_mb = db_stats['dataSize'] / (1024 * 1024)
            logger.info(f"Actual database size: {actual_size_mb:.2f} MB")
            if actual_size_mb >= CONFIG["TARGET_SIZE_MB"]:
                logger.info(f"✓ Target achieved ({actual_size_mb:.2f} MB >= {CONFIG['TARGET_SIZE_MB']} MB)")
                return
            else:
                logger.warning(f"Size below target. Continuing generation...")
        except Exception as e:
            logger.warning(f"Could not verify database size: {e}")

    attempts_batch, session_logs_batch = [], []
    start_time = time.time()

    # ================= CONTINUOUS GENERATION LOOP =================
    exam_cycle_count = 0
    
    while total_attempts_generated < TARGET_RECORDS:
        exam_cycle_count += 1
        logger.info(f"\n=== Exam Cycle {exam_cycle_count} ===")
        
        # Shuffle exams for variety
        np.random.shuffle(exams)
        
        for exam in exams:
            if total_attempts_generated >= TARGET_RECORDS:
                logger.info("Target reached, stopping generation")
                break
                
            exam_id = exam['exam_id']
            exam_date = exam['exam_date']
            duration = exam['duration_minutes']
            
            # Get questions for this exam
            questions = [q for q in questions_all if q['exam_id'] == exam_id]
            if not questions:
                continue

            # Select students for this exam (vary the count)
            n_students = min(np.random.randint(100, 400), len(students))
            selected_students = np.random.choice(students, size=n_students, replace=False)
            
            logger.info(f"Exam {exam_id}: {n_students} students × {len(questions)} questions = {n_students * len(questions)} attempts")

            for student in selected_students:
                if total_attempts_generated >= TARGET_RECORDS:
                    break
                    
                student_id = student['student_id']
                profile_data = student_profiles[student_id]
                profile = profile_data['profile']
                profile_type = profile_data['type']

                # ---------------- Session Log ----------------
                total_logs_generated += 1
                session_log = {
                    'log_id': f"LOG{total_logs_generated:09d}",
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
                    'warnings_issued': int(np.random.choice([0,1,2], p=[0.8,0.15,0.05]))
                }
                device, browser = SessionBehaviorGenerator.generate_device_browser()
                session_log['device_type'] = device
                session_log['browser'] = browser
                session_logs_batch.append(convert_numpy(session_log))

                # ---------------- Exam Attempts ----------------
                for question in questions:
                    if total_attempts_generated >= TARGET_RECORDS:
                        break

                    total_attempts_generated += 1
                    attempt_id = f"ATT{total_attempts_generated:09d}"
                    response_time = profile.generate_response_time(question['difficulty_level'])
                    is_correct = profile.generate_accuracy(question['difficulty_level'])
                    ai_score = profile.generate_ai_score()
                    answer_text = fake.paragraph(nb_sentences=np.random.randint(2, 8))
                    answer_hash = AnswerHashGenerator.generate_hash(answer_text)
                    keystrokes = profile.generate_keystrokes(len(answer_text))
                    paste_count = profile.generate_paste_count()
                    submission_time = exam_date + timedelta(seconds=np.random.uniform(0, duration*60))

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
                        'flagged': ai_score > CONFIG["AI_FLAG_THRESHOLD"]
                    }
                    attempts_batch.append(convert_numpy(attempt))

                    # ---------- Batch Insert ----------
                    if len(attempts_batch) >= CONFIG["BATCH_SIZE"]:
                        try:
                            db.exam_attempts.insert_many(attempts_batch, ordered=False)
                            attempts_batch = []
                        except errors.BulkWriteError as e:
                            logger.error(f"Exam Attempts batch insert error: {e.details}")

                    if len(session_logs_batch) >= CONFIG["BATCH_SIZE"]:
                        try:
                            db.session_logs.insert_many(session_logs_batch, ordered=False)
                            session_logs_batch = []
                        except errors.BulkWriteError as e:
                            logger.error(f"Session Logs batch insert error: {e.details}")

                    # ---------- Progress ----------
                    if total_attempts_generated % CONFIG["PROGRESS_PRINT_EVERY"] == 0:
                        elapsed = time.time() - start_time
                        rate = total_attempts_generated / elapsed if elapsed > 0 else 0
                        eta = (TARGET_RECORDS - total_attempts_generated) / rate if rate > 0 else 0
                        logger.info(f"Progress: {total_attempts_generated:,}/{TARGET_RECORDS:,} ({total_attempts_generated/TARGET_RECORDS*100:.1f}%) | Rate: {rate:.0f}/s | ETA: {eta/60:.1f} min")
                        save_checkpoint(total_attempts_generated, total_logs_generated)

    # Insert remaining records
    if attempts_batch:
        try:
            db.exam_attempts.insert_many(attempts_batch, ordered=False)
        except errors.BulkWriteError as e:
            logger.error(f"Final batch insert error: {e.details}")
    if session_logs_batch:
        try:
            db.session_logs.insert_many(session_logs_batch, ordered=False)
        except errors.BulkWriteError as e:
            logger.error(f"Final session logs insert error: {e.details}")

    elapsed = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"Generation complete!")
    logger.info(f"Total records: {total_attempts_generated:,}")
    logger.info(f"Time elapsed: {elapsed/60:.1f} minutes")
    logger.info(f"Rate: {total_attempts_generated/elapsed:.0f} records/sec")

    # Get actual database size
    try:
        db_stats = db.command("dbStats")
        actual_size_mb = db_stats['dataSize'] / (1024 * 1024)
        logger.info(f"Actual database size: {actual_size_mb:.2f} MB")
        
        if actual_size_mb >= CONFIG["TARGET_SIZE_MB"]:
            logger.info(f"✓ Target achieved ({actual_size_mb:.2f} MB >= {CONFIG['TARGET_SIZE_MB']} MB)")
        else:
            shortage_mb = CONFIG["TARGET_SIZE_MB"] - actual_size_mb
            logger.warning(f"⚠ Size below target: {actual_size_mb:.2f} MB < {CONFIG['TARGET_SIZE_MB']} MB")
            logger.warning(f"   Need {shortage_mb:.2f} MB more data")
            
            # Calculate how many more records needed
            if total_attempts_generated > 0:
                actual_record_size_kb = (actual_size_mb * 1024) / total_attempts_generated
                additional_records = int((shortage_mb * 1024) / actual_record_size_kb)
                logger.info(f"   Actual record size: {actual_record_size_kb:.3f} KB")
                logger.info(f"   Estimated additional records needed: {additional_records:,}")
                logger.info(f"   Run the script again to generate more data")
    except Exception as e:
        logger.warning(f"Could not get actual database size: {e}")
        est_size_mb = total_attempts_generated * CONFIG["ESTIMATED_RECORD_SIZE_KB"] / 1024
        logger.info(f"Estimated exam_attempts size: {est_size_mb:.2f} MB")

    save_checkpoint(total_attempts_generated, total_logs_generated)
    logger.info(f"{'='*60}\n")

if __name__ == "__main__":
    generate_historical_data()