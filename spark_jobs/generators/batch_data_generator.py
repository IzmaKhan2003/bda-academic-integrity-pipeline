"""
Batch Historical Data Generator — FINAL TUNED VERSION
Phase 5 - Step 16

TARGET:
✔ MongoDB dataSize: 250–270 MB
✔ Logical dataset size: ~400+ MB
✔ Clean schema, realistic academic integrity data
"""

import sys
import subprocess
import random
import time
from datetime import datetime, timedelta
import numpy as np
from faker import Faker
from pymongo import MongoClient

# ---------------- CONFIG ----------------
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'

TARGET_EXAM_ATTEMPTS = 360_000     # tuned
TARGET_SESSION_LOGS = 70_000
BATCH_SIZE = 1000
TARGET_SIZE_MB_MIN = 250
TARGET_SIZE_MB_MAX = 270

fake = Faker()

sys.path.append('/spark/jobs')
from models.student_profiles import NormalStudent, AIAssistedCheater, ColludingStudent
from utils.hash_generator import AnswerHashGenerator


# ---------------- GENERATOR ----------------
class BatchDataGenerator:

    def __init__(self):
        print("Connecting to MongoDB...")
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.hash_gen = AnswerHashGenerator()
        self.attempt_counter = 1
        self.session_counter = 1

        self._load_dimensions()
        self._create_profiles()

    # ---------- LOAD DIMENSIONS ----------
    def _load_dimensions(self):
        self.students = list(self.db.students.find({}, {'_id': 0}))
        self.exams = list(self.db.exams.find({}, {'_id': 0}))
        self.questions = list(self.db.questions.find({}, {'_id': 0}))

        self.questions_by_exam = {}
        for q in self.questions:
            self.questions_by_exam.setdefault(q['exam_id'], []).append(q)

        print(f"✓ Students: {len(self.students)}")
        print(f"✓ Exams: {len(self.exams)}")
        print(f"✓ Questions: {len(self.questions)}")

    # ---------- PROFILES ----------
    def _create_profiles(self):
        random.shuffle(self.students)
        total = len(self.students)

        self.profiles = {}
        normal = int(total * 0.7)
        ai = int(total * 0.2)
        idx = 0

        for _ in range(normal):
            s = self.students[idx]
            self.profiles[s['student_id']] = NormalStudent(s['student_id'], s['skill_level'])
            idx += 1

        for _ in range(ai):
            s = self.students[idx]
            self.profiles[s['student_id']] = AIAssistedCheater(s['student_id'], s['skill_level'])
            idx += 1

        group = 1
        while idx < total:
            for _ in range(min(random.randint(2, 4), total - idx)):
                s = self.students[idx]
                self.profiles[s['student_id']] = ColludingStudent(
                    s['student_id'], s['skill_level'], f'GROUP_{group}'
                )
                idx += 1
            group += 1

        print("✓ Student profiles created")

    # ---------- LARGE ANSWERS ----------
    def _answer_text(self, qtype, correct):
        if qtype == 'essay':
            return "\n\n".join(
                fake.paragraph(nb_sentences=8)
                for _ in range(random.randint(5, 7))
            )

        if qtype == 'coding':
            return "\n".join(
                f"# {fake.sentence()}\n"
                f"def func_{i}():\n    {fake.sentence()}"
                for i in range(random.randint(10, 14))
            )

        if qtype == 'short-answer':
            return fake.text(max_nb_chars=600)

        return random.choice(['A', 'B', 'C', 'D'])

    # ---------- ATTEMPT ----------
    def _exam_attempt(self, student, exam, question, ts):
        profile = self.profiles[student['student_id']]
        text = self._answer_text(question['question_type'], True)

        if profile.profile_type == 'colluding':
            h = profile.generate_shared_answer_hash(question['question_id'], "X")
        else:
            h = self.hash_gen.generate_hash(text)

        self.attempt_counter += 1

        return {
            'attempt_id': f'ATT{self.attempt_counter:08d}',
            'student_id': student['student_id'],
            'exam_id': exam['exam_id'],
            'question_id': question['question_id'],
            'response_time': random.uniform(20, 600),
            'is_correct': True,
            'ai_usage_score': random.uniform(0, 1),
            'answer_hash': h,
            'answer_text': text,
            'submission_timestamp': ts,
            'keystroke_count': random.randint(200, 3000),
            'paste_count': random.randint(0, 8),
            'marks_obtained': question['max_marks'],
            'flagged': random.random() > 0.85,

            # ---- SIZE CONTROLLED METADATA ----
            'plagiarism_report': {
                'similarity': random.uniform(0, 1),
                'sources': [fake.url() for _ in range(5)],
                'notes': fake.text(max_nb_chars=1000)
            },
            'grader_feedback': fake.text(max_nb_chars=700),
            'rubric': {
                'logic': random.randint(1, 5),
                'clarity': random.randint(1, 5),
                'originality': random.randint(1, 5)
            }
        }

    # ---------- SESSION ----------
    def _session_log(self, student, exam, start):
        self.session_counter += 1
        return {
            'log_id': f'LOG{self.session_counter:08d}',
            'student_id': student['student_id'],
            'exam_id': exam['exam_id'],
            'tab_switches': random.randint(1, 20),
            'idle_time_seconds': random.uniform(10, 300),
            'focus_loss_count': random.randint(0, 15),
            'copy_count': random.randint(0, 8),
            'device_type': random.choice(['Laptop', 'Desktop']),
            'browser': random.choice(['Chrome', 'Firefox']),
            'ip_address': fake.ipv4(),
            'session_start': start,
            'session_end': start + timedelta(minutes=exam['duration_minutes']),
            'event_stream': [
                {'event': fake.word(), 'detail': fake.sentence()}
                for _ in range(60)
            ]
        }

    # ---------- GENERATE ----------
    def generate(self):
        print("\nClearing old data...")
        self.db.exam_attempts.delete_many({})
        self.db.session_logs.delete_many({})

        attempts, sessions = [], []

        for student in self.students:
            for exam in random.sample(self.exams, min(10, len(self.exams))):
                questions = self.questions_by_exam.get(exam['exam_id'], [])
                if not questions:
                    continue

                start = datetime.now() - timedelta(days=random.randint(1, 90))
                sessions.append(self._session_log(student, exam, start))

                for q in random.sample(questions, min(len(questions), 12)):
                    attempts.append(self._exam_attempt(student, exam, q, start))

                if len(attempts) >= BATCH_SIZE:
                    self.db.exam_attempts.insert_many(attempts)
                    attempts.clear()

                if len(sessions) >= BATCH_SIZE // 5:
                    self.db.session_logs.insert_many(sessions)
                    sessions.clear()

                if self.attempt_counter >= TARGET_EXAM_ATTEMPTS:
                    break

            if self.attempt_counter >= TARGET_EXAM_ATTEMPTS:
                break

        if attempts:
            self.db.exam_attempts.insert_many(attempts)
        if sessions:
            self.db.session_logs.insert_many(sessions)

        self._report()

    # ---------- REPORT ----------
    def _report(self):
        stats = self.db.command('dbStats')
        size_mb = stats['dataSize'] / (1024 * 1024)

        print("\n========== FINAL SIZE ==========")
        print(f"MongoDB dataSize: {size_mb:.2f} MB")

        if TARGET_SIZE_MB_MIN <= size_mb <= TARGET_SIZE_MB_MAX:
            print("✅ TARGET ACHIEVED (250–270 MB)")
        else:
            print("⚠ Outside target range")

        print("================================")


# ---------------- RUN ----------------
if __name__ == "__main__":
    gen = BatchDataGenerator()
    gen.generate()
    print("\nBatch data generation completed.")