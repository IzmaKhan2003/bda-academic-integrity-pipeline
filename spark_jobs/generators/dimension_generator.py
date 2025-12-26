"""
Dimension Data Generators
Phase 4 - Step 14: Generate master data (students, courses, exams, questions)

File: spark_jobs/generators/dimension_generator.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from faker import Faker
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
from utils.distributions import DistributionGenerator, TimeSeriesGenerator


fake = Faker()
Faker.seed(42)  # Reproducible data
np.random.seed(42)


def convert_to_python_types(obj):
    """Convert numpy types to native Python types for MongoDB"""
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


class DimensionDataGenerator:
    """Generate all dimension table data"""
    
    def __init__(self, mongo_uri='mongodb://admin:admin123@mongodb:27017/'):
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client['academic_integrity']
        
        # Define data parameters
        self.n_students = 5000
        self.n_courses = 50
        self.n_exams_per_course = 2  # Midterm + Final
        self.n_questions_per_exam = 50
        
        # Student profiles distribution
        self.profile_distribution = {
            'normal': 0.75,  # 75% normal students
            'ai_cheater': 0.15,  # 15% AI cheaters
            'colluding': 0.10  # 10% colluding (in pairs)
        }
    
    def generate_students(self):
        """
        Generate 5000 students with realistic attributes
        """
        print(f"Generating {self.n_students} students...")
        
        students = []
        
        programs = ["Computer Science", "Data Science", "Software Engineering", 
                   "Information Systems", "Business Analytics"]
        program_weights = [0.35, 0.25, 0.20, 0.15, 0.05]
        
        regions = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]
        region_weights = [0.40, 0.25, 0.20, 0.08, 0.05, 0.02]
        
        for i in range(1, self.n_students + 1):
            student_id = f"STU{i:06d}"
            
            # Generate attributes
            name = fake.name()
            email = f"{name.lower().replace(' ', '.')}.{i}@university.edu"
            program = str(np.random.choice(programs, p=program_weights))
            academic_year = int(np.random.choice([1, 2, 3, 4, 5], p=[0.25, 0.25, 0.25, 0.20, 0.05]))
            region = str(np.random.choice(regions, p=region_weights))
            
            # Skill level: Beta distribution (most students average)
            skill_level = float(DistributionGenerator.beta_skill_distribution(alpha=5, beta=5))
            
            # Enrollment date: within last 5 years
            days_ago = int(np.random.randint(0, 365 * 5))
            enrollment_date = datetime.now() - timedelta(days=days_ago)
            
            student = {
                'student_id': student_id,
                'name': name,
                'email': email,
                'program': program,
                'academic_year': academic_year,
                'region': region,
                'skill_level': round(skill_level, 2),
                'enrollment_date': enrollment_date
            }
            
            students.append(student)
        
        # Insert into MongoDB
        self.db.students.insert_many(students)
        print(f"✓ Inserted {len(students)} students")
        
        return students
    
    def generate_courses(self):
        """
        Generate 50 courses across departments
        """
        print(f"Generating {self.n_courses} courses...")
        
        courses = []
        
        departments = ["Computer Science", "Mathematics", "Engineering", 
                      "Business", "Statistics"]
        
        # Course templates by department
        cs_courses = [
            "Data Structures", "Algorithms", "Operating Systems", "Databases",
            "Computer Networks", "Software Engineering", "AI & Machine Learning",
            "Web Development", "Mobile App Development", "Cybersecurity"
        ]
        
        math_courses = [
            "Calculus I", "Calculus II", "Linear Algebra", "Discrete Mathematics",
            "Probability & Statistics", "Differential Equations", "Abstract Algebra"
        ]
        
        eng_courses = [
            "Systems Design", "Signal Processing", "Control Systems",
            "Embedded Systems", "VLSI Design"
        ]
        
        bus_courses = [
            "Business Analytics", "Operations Research", "Project Management",
            "Data Mining", "Decision Support Systems"
        ]
        
        stat_courses = [
            "Statistical Methods", "Regression Analysis", "Time Series",
            "Bayesian Statistics", "Experimental Design"
        ]
        
        all_course_names = (cs_courses * 2 + math_courses + 
                           eng_courses + bus_courses + stat_courses)[:self.n_courses]
        
        for i, course_name in enumerate(all_course_names, 1):
            course_id = f"CS{100 + i}" if i <= 30 else f"MATH{100 + (i - 30)}"
            
            # Determine department
            if i <= 20:
                department = "Computer Science"
                difficulty = int(np.random.choice([3, 4, 5], p=[0.3, 0.5, 0.2]))
            elif i <= 30:
                department = "Mathematics"
                difficulty = int(np.random.choice([3, 4, 5], p=[0.4, 0.4, 0.2]))
            elif i <= 38:
                department = "Engineering"
                difficulty = int(np.random.choice([4, 5], p=[0.6, 0.4]))
            elif i <= 44:
                department = "Business"
                difficulty = int(np.random.choice([2, 3, 4], p=[0.3, 0.5, 0.2]))
            else:
                department = "Statistics"
                difficulty = int(np.random.choice([3, 4], p=[0.6, 0.4]))
            
            instructor_id = f"INST{int(np.random.randint(100, 200)):03d}"
            instructor_name = f"Dr. {fake.name()}"
            credits = int(np.random.choice([3, 4], p=[0.8, 0.2]))
            
            course = {
                'course_id': course_id,
                'course_name': course_name,
                'instructor_id': instructor_id,
                'instructor_name': instructor_name,
                'difficulty_level': difficulty,
                'department': department,
                'credits': credits
            }
            
            courses.append(course)
        
        # Insert into MongoDB
        self.db.courses.insert_many(courses)
        print(f"✓ Inserted {len(courses)} courses")
        
        return courses
    
    def generate_exams(self, courses):
        """
        Generate exams for each course (midterm + final)
        """
        print(f"Generating exams...")
        
        exams = []
        exam_types = ["midterm", "final"]
        
        # Generate exam schedule starting 3 months ago
        start_date = datetime.now() - timedelta(days=90)
        
        for course in courses:
            for exam_type in exam_types:
                exam_id = f"EXM{len(exams) + 1:05d}"
                
                # Midterms in first half, finals in second half
                if exam_type == "midterm":
                    days_offset = int(np.random.randint(0, 45))
                else:
                    days_offset = int(np.random.randint(45, 90))
                
                exam_date = start_date + timedelta(days=days_offset)
                
                # Set exam time (9 AM to 5 PM)
                hour = int(np.random.choice([9, 10, 11, 14, 15, 16, 17]))
                exam_date = exam_date.replace(hour=hour, minute=0, second=0, microsecond=0)
                
                # Duration based on exam type
                if exam_type == "quiz":
                    duration = 60
                    total_marks = 50
                elif exam_type == "midterm":
                    duration = 120
                    total_marks = 100
                else:  # final
                    duration = 180
                    total_marks = 150
                
                exam_name = f"{course['course_name']} - {exam_type.title()} {exam_date.year}"
                
                exam = {
                    'exam_id': exam_id,
                    'course_id': course['course_id'],
                    'exam_name': exam_name,
                    'exam_date': exam_date,
                    'duration_minutes': duration,
                    'total_marks': total_marks,
                    'exam_type': exam_type,
                    'proctored': bool(np.random.choice([True, False], p=[0.7, 0.3]))
                }
                
                exams.append(exam)
        
        # Insert into MongoDB
        self.db.exams.insert_many(exams)
        print(f"✓ Inserted {len(exams)} exams")
        
        return exams
    
    def generate_questions(self, exams):
        """
        Generate questions for each exam
        """
        print(f"Generating questions...")
        
        questions = []
        
        question_types = ["MCQ", "short-answer", "essay", "coding", "true-false"]
        type_weights = [0.40, 0.25, 0.15, 0.15, 0.05]
        
        topics = [
            "Binary Trees", "Graphs", "Dynamic Programming", "Sorting Algorithms",
            "Hash Tables", "Recursion", "Greedy Algorithms", "Backtracking",
            "Divide and Conquer", "String Manipulation", "Bit Manipulation",
            "Linked Lists", "Stacks and Queues", "Heaps", "Tries"
        ]
        
        for exam in exams:
            for q_num in range(1, self.n_questions_per_exam + 1):
                question_id = f"QST{len(questions) + 1:06d}"
                
                # Question difficulty: normally distributed around exam difficulty
                difficulty = int(np.random.choice([1, 2, 3, 4, 5], 
                                             p=[0.05, 0.20, 0.50, 0.20, 0.05]))
                
                q_type = str(np.random.choice(question_types, p=type_weights))
                
                # Marks based on question type
                if q_type == "MCQ" or q_type == "true-false":
                    max_marks = int(np.random.choice([1, 2], p=[0.7, 0.3]))
                    expected_time = int(np.random.randint(30, 90))
                elif q_type == "short-answer":
                    max_marks = int(np.random.choice([5, 10], p=[0.6, 0.4]))
                    expected_time = int(np.random.randint(120, 300))
                elif q_type == "essay":
                    max_marks = int(np.random.choice([15, 20, 25], p=[0.5, 0.3, 0.2]))
                    expected_time = int(np.random.randint(600, 1200))
                else:  # coding
                    max_marks = int(np.random.choice([10, 15, 20], p=[0.4, 0.4, 0.2]))
                    expected_time = int(np.random.randint(300, 900))
                
                topic = str(np.random.choice(topics))
                
                question = {
                    'question_id': question_id,
                    'exam_id': exam['exam_id'],
                    'question_number': q_num,
                    'difficulty_level': difficulty,
                    'question_type': q_type,
                    'max_marks': max_marks,
                    'expected_time_seconds': expected_time,
                    'topic': topic
                }
                
                questions.append(question)
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(questions), batch_size):
            batch = questions[i:i + batch_size]
            self.db.questions.insert_many(batch)
            print(f"  Inserted batch {i // batch_size + 1}: {len(batch)} questions")
        
        print(f"✓ Inserted {len(questions)} total questions")
        
        return questions
    
    def generate_all_dimensions(self):
        """
        Generate all dimension tables in correct order
        """
        print("\n" + "=" * 70)
        print("DIMENSION DATA GENERATION - Phase 4, Step 14")
        print("=" * 70 + "\n")
        
        # Clear existing data
        print("Clearing existing dimension data...")
        self.db.students.delete_many({})
        self.db.courses.delete_many({})
        self.db.exams.delete_many({})
        self.db.questions.delete_many({})
        print("✓ Cleared\n")
        
        # Generate in dependency order
        students = self.generate_students()
        print()
        
        courses = self.generate_courses()
        print()
        
        exams = self.generate_exams(courses)
        print()
        
        questions = self.generate_questions(exams)
        print()
        
        # Summary
        print("=" * 70)
        print("DIMENSION GENERATION COMPLETE")
        print("=" * 70)
        print(f"Students: {len(students)}")
        print(f"Courses: {len(courses)}")
        print(f"Exams: {len(exams)}")
        print(f"Questions: {len(questions)}")
        print("\nData ready for fact table generation!")
        print("=" * 70)
        
        return {
            'students': students,
            'courses': courses,
            'exams': exams,
            'questions': questions
        }


if __name__ == "__main__":
    generator = DimensionDataGenerator()
    dimensions = generator.generate_all_dimensions()
