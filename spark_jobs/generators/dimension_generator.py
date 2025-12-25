"""
Dimension Data Generator
Phase 4 - Step 14: Generate master data (students, courses, exams, questions)
"""

import sys
import subprocess

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

try:
    from faker import Faker
except ImportError:
    print("Installing faker...")
    install_package('faker')
    from faker import Faker

try:
    from pymongo import MongoClient
except ImportError:
    print("Installing pymongo...")
    install_package('pymongo')
    from pymongo import MongoClient

import random
import numpy as np
from datetime import datetime, timedelta
import json

# Initialize Faker
fake = Faker()

# MongoDB connection
MONGO_URI = 'mongodb://admin:admin123@mongodb:27017/'
MONGO_DB = 'academic_integrity'

# Configuration
CONFIG = {
    'students': 5000,
    'courses': 50,
    'exams': 100,
    'questions': 5000  # 50 questions per exam
}

PROGRAMS = [
    'Computer Science',
    'Data Science', 
    'Software Engineering',
    'Information Systems',
    'Business Analytics'
]

REGIONS = [
    'North America',
    'Europe',
    'Asia',
    'South America',
    'Africa',
    'Oceania'
]

DEPARTMENTS = [
    'Computer Science',
    'Mathematics',
    'Engineering',
    'Business',
    'Statistics'
]

COURSE_TEMPLATES = [
    ('Introduction to {}', 1),
    ('Intermediate {}', 2),
    ('Advanced {}', 3),
    ('{} Fundamentals', 2),
    ('{} Applications', 3),
    ('Modern {}', 3),
    ('{} Theory', 4),
    ('{} Systems', 4),
    ('Applied {}', 3),
    ('{}Engineering', 4)
]

TOPICS_BY_DEPT = {
    'Computer Science': ['Algorithms', 'Data Structures', 'AI', 'Machine Learning', 'Databases', 
                         'Networks', 'Security', 'Software Engineering', 'Operating Systems'],
    'Mathematics': ['Calculus', 'Linear Algebra', 'Statistics', 'Probability', 'Discrete Math',
                    'Number Theory', 'Graph Theory', 'Optimization'],
    'Engineering': ['Circuits', 'Systems', 'Control Theory', 'Signal Processing', 'Electronics',
                   'Embedded Systems', 'Digital Design'],
    'Business': ['Finance', 'Marketing', 'Strategy', 'Operations', 'Analytics', 'Management'],
    'Statistics': ['Regression', 'Time Series', 'Bayesian Analysis', 'Experimental Design',
                  'Multivariate Analysis', 'Data Mining']
}

QUESTION_TYPES = ['MCQ', 'short-answer', 'essay', 'coding', 'true-false']

EXAM_TYPES = ['midterm', 'final', 'quiz', 'assignment']


class DimensionGenerator:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        
    def generate_students(self, count=5000):
        """Generate student dimension data"""
        print(f"\nGenerating {count} students...")
        
        students = []
        for i in range(count):
            student = {
                'student_id': f'STU{str(i+1).zfill(6)}',
                'name': fake.name(),
                'email': fake.email(),
                'program': random.choice(PROGRAMS),
                'academic_year': random.randint(1, 5),
                'region': random.choice(REGIONS),
                'skill_level': float(np.random.beta(5, 2)),  # Skewed toward 0.7-0.9
                'enrollment_date': fake.date_time_between(start_date='-4y', end_date='now')
            }
            students.append(student)
            
            if (i + 1) % 1000 == 0:
                print(f"  Generated {i+1}/{count} students...")
        
        # Insert into MongoDB
        result = self.db.students.insert_many(students)
        print(f"✓ Inserted {len(result.inserted_ids)} students into MongoDB")

        return students

    def generate_courses(self, count=50):
        """Generate course dimension data"""
        print(f"\nGenerating {count} courses...")
        
        courses = []
        for i in range(count):
            dept = random.choice(DEPARTMENTS)
            topics = TOPICS_BY_DEPT[dept]
            topic = random.choice(topics)
            template, base_difficulty = random.choice(COURSE_TEMPLATES)
            
            course_name = template.format(topic)
            
            course = {
                'course_id': f'{dept[:2].upper()}{str((i % 10) * 100 + random.randint(1, 99))}',
                'course_name': course_name,
                'instructor_id': f'INST{str(random.randint(1, 50)).zfill(3)}',
                'instructor_name': f'Dr. {fake.last_name()}',
                'difficulty_level': min(5, max(1, base_difficulty + random.randint(-1, 1))),
                'department': dept,
                'credits': random.choice([3, 4])
            }
            courses.append(course)
        
        # Insert into MongoDB
        result = self.db.courses.insert_many(courses)
        print(f"✓ Inserted {len(result.inserted_ids)} courses into MongoDB")
        
        return courses

    def generate_exams(self, courses, count=100):
        """Generate exam dimension data"""
        print(f"\nGenerating {count} exams...")
        
        exams = []
        exam_dates_start = datetime.now() - timedelta(days=60)  # Start 60 days ago
        
        for i in range(count):
            course = random.choice(courses)
            exam_type = random.choice(EXAM_TYPES)
            
            # Exam date in past 60 days or next 30 days
            days_offset = random.randint(-60, 30)
            exam_date = datetime.now() + timedelta(days=days_offset)
            
            # Duration based on exam type
            if exam_type == 'quiz':
                duration = random.choice([30, 45, 60])
                total_marks = random.choice([20, 25, 30])
            elif exam_type == 'midterm':
                duration = random.choice([90, 120])
                total_marks = random.choice([50, 75, 100])
            elif exam_type == 'final':
                duration = random.choice([120, 150, 180])
                total_marks = 100
            else:  # assignment
                duration = random.choice([60, 90, 120])
                total_marks = random.choice([50, 75, 100])
            
            exam = {
                'exam_id': f'EXM{str(i+1).zfill(3)}',
                'course_id': course['course_id'],
                'exam_name': f"{exam_type.title()} - {course['course_name']}",
                'exam_date': exam_date,
                'duration_minutes': duration,
                'total_marks': total_marks,
                'exam_type': exam_type,
                'proctored': exam_type in ['midterm', 'final']
            }
            exams.append(exam)
        
        # Insert into MongoDB
        result = self.db.exams.insert_many(exams)
        print(f"✓ Inserted {len(result.inserted_ids)} exams into MongoDB")
        
        return exams

    def generate_questions(self, exams, count=5000):
        """Generate question dimension data"""
        print(f"\nGenerating {count} questions...")
        
        questions = []
        questions_per_exam = count // len(exams)
        
        question_id_counter = 1
        
        for exam in exams:
            num_questions = questions_per_exam if exam['exam_type'] != 'quiz' else min(20, questions_per_exam)
            
            for q_num in range(1, num_questions + 1):
                # Question type distribution
                if exam['exam_type'] == 'quiz':
                    q_type_weights = [0.7, 0.2, 0.0, 0.0, 0.1]  # Mostly MCQ
                elif exam['exam_type'] == 'coding':
                    q_type_weights = [0.1, 0.2, 0.0, 0.7, 0.0]  # Mostly coding
                else:
                    q_type_weights = [0.4, 0.3, 0.2, 0.1, 0.0]  # Mixed
                
                q_type = random.choices(QUESTION_TYPES, weights=q_type_weights)[0]
                
                # Difficulty distribution (normal around 3)
                difficulty = int(np.clip(np.random.normal(3, 1), 1, 5))
                
                # Max marks based on type
                if q_type == 'MCQ' or q_type == 'true-false':
                    max_marks = random.choice([1, 2, 3])
                    expected_time = random.randint(30, 120)
                elif q_type == 'short-answer':
                    max_marks = random.choice([5, 10])
                    expected_time = random.randint(180, 300)
                elif q_type == 'essay':
                    max_marks = random.choice([15, 20, 25])
                    expected_time = random.randint(600, 900)
                else:  # coding
                    max_marks = random.choice([10, 15, 20])
                    expected_time = random.randint(300, 600)
                
                question = {
                    'question_id': f'QST{str(question_id_counter).zfill(4)}',
                    'exam_id': exam['exam_id'],
                    'question_number': q_num,
                    'difficulty_level': difficulty,
                    'question_type': q_type,
                    'max_marks': max_marks,
                    'expected_time_seconds': expected_time,
                    'topic': random.choice(TOPICS_BY_DEPT[exam['course_id'][:2] == 'CS' and 'Computer Science' or 'Mathematics'])
                }
                questions.append(question)
                question_id_counter += 1
                
            if question_id_counter % 1000 == 0:
                print(f"  Generated {question_id_counter}/{count} questions...")
        
        # Insert into MongoDB in batches
        batch_size = 1000
        for i in range(0, len(questions), batch_size):
            batch = questions[i:i+batch_size]
            self.db.questions.insert_many(batch)
            print(f"  Inserted batch {i//batch_size + 1}/{len(questions)//batch_size + 1}...")
        
        print(f"✓ Inserted {len(questions)} questions into MongoDB")
        
        return questions

    def generate_all_dimensions(self):
        """Generate all dimension data"""
        print("=" * 70)
        print("DIMENSION DATA GENERATION")
        print("=" * 70)
        
        # Clear existing data
        print("\nClearing existing dimension data...")
        self.db.students.delete_many({})
        self.db.courses.delete_many({})
        self.db.exams.delete_many({})
        self.db.questions.delete_many({})
        print("✓ Cleared")
        
        # Generate dimensions
        students = self.generate_students(CONFIG['students'])
        courses = self.generate_courses(CONFIG['courses'])
        exams = self.generate_exams(courses, CONFIG['exams'])
        questions = self.generate_questions(exams, CONFIG['questions'])
        
        # Summary
        print("\n" + "=" * 70)
        print("GENERATION COMPLETE")
        print("=" * 70)
        print(f"\nRecords created:")
        print(f"  Students: {len(students)}")
        print(f"  Courses: {len(courses)}")
        print(f"  Exams: {len(exams)}")
        print(f"  Questions: {len(questions)}")
        
        # Verify in MongoDB
        print(f"\nMongoDB verification:")
        print(f"  Students: {self.db.students.count_documents({})}")
        print(f"  Courses: {self.db.courses.count_documents({})}")
        print(f"  Exams: {self.db.exams.count_documents({})}")
        print(f"  Questions: {self.db.questions.count_documents({})}")
        
        print("\n✓ All dimensions generated successfully!")
        
        return {
            'students': students,
            'courses': courses,
            'exams': exams,
            'questions': questions
        }

if __name__ == "__main__":

    generator = DimensionGenerator()
    generator.generate_all_dimensions()