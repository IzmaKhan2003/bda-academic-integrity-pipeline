"""
Data Quality Validation Script
Phase 5 - Step 19: Validate data quality and volume

Checks:
1. Total size >= 250 MB
2. No null values in required fields
3. Foreign key integrity
4. Statistical distributions match models
5. Data type correctness
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

import numpy as np
from collections import defaultdict
from datetime import datetime


MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'


class DataValidator:
    """Comprehensive data quality validator"""
    
    def __init__(self):
        print("Connecting to MongoDB...")
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        
        self.errors = []
        self.warnings = []
        
    def add_error(self, message):
        """Add critical error"""
        self.errors.append(f"❌ ERROR: {message}")
        print(f"  ❌ {message}")
    
    def add_warning(self, message):
        """Add warning"""
        self.warnings.append(f"⚠️  WARNING: {message}")
        print(f"  ⚠️  {message}")
    
    def add_success(self, message):
        """Add success message"""
        print(f"  ✓ {message}")
    
    def validate_volume(self):
        """Check 1: Validate total data volume >= 250 MB"""
        print("\n" + "=" * 70)
        print("CHECK 1: DATA VOLUME (>= 250 MB)")
        print("=" * 70)
        
        stats = self.db.command('dbStats')
        total_size_bytes = stats.get('dataSize', 0)
        total_size_mb = total_size_bytes / (1024 * 1024)
        
        print(f"\nTotal database size: {total_size_mb:.2f} MB")
        
        if total_size_mb >= 250:
            self.add_success(f"Volume requirement met: {total_size_mb:.2f} MB >= 250 MB")
        else:
            self.add_error(f"Volume too small: {total_size_mb:.2f} MB < 250 MB")
        
        # Individual collections
        for collection in ['students', 'courses', 'exams', 'questions', 
                          'exam_attempts', 'session_logs']:
            col_stats = self.db.command('collStats', collection)
            size_mb = col_stats.get('size', 0) / (1024 * 1024)
            count = col_stats.get('count', 0)
            print(f"  {collection}: {count:,} records, {size_mb:.2f} MB")
    
    def validate_null_values(self):
        """Check 2: No null values in required fields"""
        print("\n" + "=" * 70)
        print("CHECK 2: NULL VALUE VALIDATION")
        print("=" * 70)
        
        required_fields = {
            'students': ['student_id', 'name', 'email', 'program', 'academic_year', 
                        'region', 'skill_level'],
            'courses': ['course_id', 'course_name', 'difficulty_level', 'department'],
            'exams': ['exam_id', 'course_id', 'exam_date', 'duration_minutes', 
                     'total_marks', 'exam_type'],
            'questions': ['question_id', 'exam_id', 'difficulty_level', 'question_type', 
                         'max_marks'],
            'exam_attempts': ['attempt_id', 'student_id', 'exam_id', 'question_id', 
                            'response_time', 'is_correct', 'ai_usage_score', 
                            'answer_hash', 'submission_timestamp'],
            'session_logs': ['log_id', 'student_id', 'exam_id', 'session_start']
        }
        
        for collection, fields in required_fields.items():
            print(f"\n{collection}:")
            has_errors = False
            
            for field in fields:
                null_count = self.db[collection].count_documents({field: None})
                missing_count = self.db[collection].count_documents({field: {"$exists": False}})
                
                if null_count > 0 or missing_count > 0:
                    self.add_error(f"{field}: {null_count} nulls, {missing_count} missing")
                    has_errors = True
            
            if not has_errors:
                self.add_success(f"All required fields present")
    
    def validate_foreign_keys(self):
        """Check 3: Foreign key integrity"""
        print("\n" + "=" * 70)
        print("CHECK 3: FOREIGN KEY INTEGRITY")
        print("=" * 70)
        
        # Get all IDs
        student_ids = set(doc['student_id'] for doc in self.db.students.find({}, {'student_id': 1}))
        course_ids = set(doc['course_id'] for doc in self.db.courses.find({}, {'course_id': 1}))
        exam_ids = set(doc['exam_id'] for doc in self.db.exams.find({}, {'exam_id': 1}))
        question_ids = set(doc['question_id'] for doc in self.db.questions.find({}, {'question_id': 1}))
        
        print(f"\nDimension key counts:")
        print(f"  Students: {len(student_ids)}")
        print(f"  Courses: {len(course_ids)}")
        print(f"  Exams: {len(exam_ids)}")
        print(f"  Questions: {len(question_ids)}")
        
        # Check exams.course_id references
        print(f"\nValidating exams.course_id -> courses.course_id:")
        orphaned_exams = 0
        for exam in self.db.exams.find({}, {'exam_id': 1, 'course_id': 1}):
            if exam['course_id'] not in course_ids:
                orphaned_exams += 1
        
        if orphaned_exams == 0:
            self.add_success("All exams reference valid courses")
        else:
            self.add_error(f"{orphaned_exams} exams reference invalid courses")
        
        # Check questions.exam_id references
        print(f"\nValidating questions.exam_id -> exams.exam_id:")
        orphaned_questions = 0
        for question in self.db.questions.find({}, {'question_id': 1, 'exam_id': 1}):
            if question['exam_id'] not in exam_ids:
                orphaned_questions += 1
        
        if orphaned_questions == 0:
            self.add_success("All questions reference valid exams")
        else:
            self.add_error(f"{orphaned_questions} questions reference invalid exams")
        
        # Check exam_attempts foreign keys
        print(f"\nValidating exam_attempts foreign keys:")
        sample_size = min(10000, self.db.exam_attempts.count_documents({}))
        attempts_sample = self.db.exam_attempts.aggregate([{'$sample': {'size': sample_size}}])
        
        invalid_students = 0
        invalid_exams = 0
        invalid_questions = 0
        
        for attempt in attempts_sample:
            if attempt['student_id'] not in student_ids:
                invalid_students += 1
            if attempt['exam_id'] not in exam_ids:
                invalid_exams += 1
            if attempt['question_id'] not in question_ids:
                invalid_questions += 1
        
        if invalid_students + invalid_exams + invalid_questions == 0:
            self.add_success(f"All exam_attempts ({sample_size} sampled) reference valid entities")
        else:
            if invalid_students > 0:
                self.add_error(f"{invalid_students}/{sample_size} attempts have invalid student_id")
            if invalid_exams > 0:
                self.add_error(f"{invalid_exams}/{sample_size} attempts have invalid exam_id")
            if invalid_questions > 0:
                self.add_error(f"{invalid_questions}/{sample_size} attempts have invalid question_id")
    
    def validate_distributions(self):
        """Check 4: Statistical distributions match models"""
        print("\n" + "=" * 70)
        print("CHECK 4: STATISTICAL DISTRIBUTION VALIDATION")
        print("=" * 70)
        
        # Sample exam attempts for analysis
        sample_size = min(10000, self.db.exam_attempts.count_documents({}))
        attempts = list(self.db.exam_attempts.aggregate([{'$sample': {'size': sample_size}}]))
        
        # Response times
        print(f"\nResponse Times (n={len(attempts)}):")
        response_times = [a['response_time'] for a in attempts]
        print(f"  Mean: {np.mean(response_times):.1f}s")
        print(f"  Median: {np.median(response_times):.1f}s")
        print(f"  Std Dev: {np.std(response_times):.1f}s")
        print(f"  Min: {np.min(response_times):.1f}s")
        print(f"  Max: {np.max(response_times):.1f}s")
        
        # Expected: mean ~90s, median ~60-120s for log-normal
        mean_time = np.mean(response_times)
        if 50 <= mean_time <= 200:
            self.add_success(f"Response time mean ({mean_time:.1f}s) in expected range")
        else:
            self.add_warning(f"Response time mean ({mean_time:.1f}s) outside expected range (50-200s)")
        
        # AI Usage Scores
        print(f"\nAI Usage Scores:")
        ai_scores = [a['ai_usage_score'] for a in attempts]
        print(f"  Mean: {np.mean(ai_scores):.3f}")
        print(f"  Median: {np.median(ai_scores):.3f}")
        print(f"  Std Dev: {np.std(ai_scores):.3f}")
        
        # Distribution check (should have 70% low, 20% high)
        low_ai = sum(1 for s in ai_scores if s < 0.3)
        high_ai = sum(1 for s in ai_scores if s > 0.7)
        
        print(f"  Low AI (<0.3): {low_ai} ({low_ai/len(ai_scores)*100:.1f}%)")
        print(f"  High AI (>0.7): {high_ai} ({high_ai/len(ai_scores)*100:.1f}%)")
        
        # Expected: ~70% low, ~20% high (with variance)
        low_pct = low_ai / len(ai_scores) * 100
        high_pct = high_ai / len(ai_scores) * 100
        
        if 60 <= low_pct <= 80:
            self.add_success(f"Low AI percentage ({low_pct:.1f}%) in expected range")
        else:
            self.add_warning(f"Low AI percentage ({low_pct:.1f}%) outside expected range (60-80%)")
        
        if 10 <= high_pct <= 30:
            self.add_success(f"High AI percentage ({high_pct:.1f}%) in expected range")
        else:
            self.add_warning(f"High AI percentage ({high_pct:.1f}%) outside expected range (10-30%)")
        
        # Accuracy Rate
        print(f"\nAccuracy:")
        accuracy = sum(1 for a in attempts if a['is_correct']) / len(attempts) * 100
        print(f"  Overall: {accuracy:.1f}%")
        
        if 50 <= accuracy <= 80:
            self.add_success(f"Accuracy ({accuracy:.1f}%) in expected range")
        else:
            self.add_warning(f"Accuracy ({accuracy:.1f}%) outside expected range (50-80%)")
    
    def validate_data_types(self):
        """Check 5: Data type correctness"""
        print("\n" + "=" * 70)
        print("CHECK 5: DATA TYPE VALIDATION")
        print("=" * 70)
        
        # Sample exam_attempts for type checking
        sample = self.db.exam_attempts.find_one()
        
        if not sample:
            self.add_error("No exam_attempts found for type validation")
            return
        
        print(f"\nValidating exam_attempts data types:")
        
        type_checks = {
            'attempt_id': str,
            'student_id': str,
            'exam_id': str,
            'question_id': str,
            'response_time': (int, float),
            'is_correct': bool,
            'ai_usage_score': (int, float),
            'answer_hash': str,
            'answer_text': str,
            'submission_timestamp': datetime,
            'keystroke_count': int,
            'paste_count': int,
            'marks_obtained': int,
            'flagged': bool
        }
        
        for field, expected_type in type_checks.items():
            if field not in sample:
                self.add_error(f"Missing field: {field}")
                continue
            
            value = sample[field]
            if isinstance(expected_type, tuple):
                if not isinstance(value, expected_type):
                    self.add_error(f"{field} type {type(value)} not in {expected_type}")
                else:
                    self.add_success(f"{field}: {type(value).__name__}")
            else:
                if not isinstance(value, expected_type):
                    self.add_error(f"{field} type {type(value)} != {expected_type}")
                else:
                    self.add_success(f"{field}: {type(value).__name__}")
        
        # Validate value ranges
        print(f"\nValidating value ranges:")
        
        range_checks = {
            'response_time': (0, 1000),
            'ai_usage_score': (0.0, 1.0),
            'keystroke_count': (0, 100000),
            'paste_count': (0, 100),
            'marks_obtained': (0, 100)
        }
        
        for field, (min_val, max_val) in range_checks.items():
            value = sample[field]
            if min_val <= value <= max_val:
                self.add_success(f"{field}={value} in range [{min_val}, {max_val}]")
            else:
                self.add_warning(f"{field}={value} outside range [{min_val}, {max_val}]")
    
    def validate_collusion_patterns(self):
        """Bonus Check: Detect collusion patterns"""
        print("\n" + "=" * 70)
        print("BONUS CHECK: COLLUSION PATTERN DETECTION")
        print("=" * 70)
        
        # Find duplicate answer hashes
        pipeline = [
            {'$group': {
                '_id': {'exam_id': '$exam_id', 'question_id': '$question_id', 
                       'answer_hash': '$answer_hash'},
                'count': {'$sum': 1},
                'students': {'$addToSet': '$student_id'}
            }},
            {'$match': {'count': {'$gte': 2}}},
            {'$limit': 10}
        ]
        
        collusion_cases = list(self.db.exam_attempts.aggregate(pipeline))
        
        print(f"\nSample collusion cases (identical answers):")
        for case in collusion_cases[:5]:
            exam_id = case['_id']['exam_id']
            question_id = case['_id']['question_id']
            count = case['count']
            students = case['students']
            
            print(f"  Exam {exam_id}, Question {question_id}:")
            print(f"    {count} students with identical answers")
            print(f"    Students: {', '.join(students[:5])}")
        
        if collusion_cases:
            self.add_success(f"Found {len(collusion_cases)} collusion patterns (expected)")
        else:
            self.add_warning("No collusion patterns detected")
    
    def generate_report(self):
        """Generate final validation report"""
        print("\n" + "=" * 70)
        print("VALIDATION REPORT SUMMARY")
        print("=" * 70)
        
        print(f"\n✓ Successes: {len([m for m in [self.errors, self.warnings] if not m])}")
        print(f"⚠️  Warnings: {len(self.warnings)}")
        print(f"❌ Errors: {len(self.errors)}")
        
        if self.errors:
            print("\nCRITICAL ERRORS:")
            for error in self.errors:
                print(f"  {error}")
        
        if self.warnings:
            print("\nWARNINGS:")
            for warning in self.warnings:
                print(f"  {warning}")
        
        if not self.errors:
            print("\n" + "=" * 70)
            print("✓ ✓ ✓  ALL VALIDATIONS PASSED  ✓ ✓ ✓")
            print("=" * 70)
            return True
        else:
            print("\n" + "=" * 70)
            print("❌ VALIDATION FAILED - FIX ERRORS ABOVE")
            print("=" * 70)
            return False
    
    def run_all_validations(self):
        """Run all validation checks"""
        print("=" * 70)
        print("DATA QUALITY VALIDATION")
        print("Phase 5 - Step 19")
        print("=" * 70)
        
        self.validate_volume()
        self.validate_null_values()
        self.validate_foreign_keys()
        self.validate_distributions()
        self.validate_data_types()
        self.validate_collusion_patterns()
        
        return self.generate_report()


def main():
    validator = DataValidator()
    success = validator.run_all_validations()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
