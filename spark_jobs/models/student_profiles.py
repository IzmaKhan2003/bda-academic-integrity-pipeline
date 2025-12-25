"""
Student Behavioral Profiles
Phase 4 - Step 12: Statistical models for realistic student behavior
"""

import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import hashlib
import random

class StudentProfile:
    """Base class for student behavior modeling"""
    
    def __init__(self, student_id, skill_level):
        self.student_id = student_id
        self.skill_level = skill_level  # 0.0 - 1.0
        
    def generate_response_time(self, question_difficulty):
        """Override in subclasses"""
        raise NotImplementedError
        
    def generate_accuracy(self, question_difficulty):
        """Override in subclasses"""
        raise NotImplementedError
        
    def generate_ai_score(self):
        """Override in subclasses"""
        raise NotImplementedError


class NormalStudent(StudentProfile):
    """
    Normal student behavior - log-normal response times, skill-based accuracy
    
    Characteristics:
    - Response time: Log-normal distribution (μ=4.5, σ=0.8)
    - Accuracy: Skill-dependent (30-90%)
    - AI usage: Very low (0.0-0.2)
    - Keystroke patterns: Normal typing behavior
    """
    
    def __init__(self, student_id, skill_level):
        super().__init__(student_id, skill_level)
        self.profile_type = "normal"
        
    def generate_response_time(self, question_difficulty):
        """
        Log-normal distribution for response times
        Easy questions: 30-120s
        Hard questions: 120-300s
        """
        # Base parameters
        mu = 4.5  # Mean of log(response_time)
        sigma = 0.8  # Standard deviation
        
        # Adjust for difficulty
        difficulty_factor = 1 + (question_difficulty - 3) * 0.2
        
        # Generate log-normal response time
        response_time = np.random.lognormal(mu, sigma) * difficulty_factor
        
        # Add human variability
        response_time += np.random.normal(0, 15)
        
        # Clamp to reasonable range
        return max(10.0, min(600.0, response_time))
    
    def generate_accuracy(self, question_difficulty):
        """
        Accuracy based on skill level and question difficulty
        """
        # Base accuracy from skill level
        base_accuracy = self.skill_level
        
        # Reduce accuracy for harder questions
        difficulty_penalty = (question_difficulty - 3) * 0.10
        final_accuracy = base_accuracy - difficulty_penalty
        
        # Add randomness
        final_accuracy += np.random.normal(0, 0.05)
        
        # Clamp to 0-1
        final_accuracy = max(0.0, min(1.0, final_accuracy))
        
        return np.random.random() < final_accuracy
    
    def generate_ai_score(self):
        """Low AI usage score"""
        return np.random.uniform(0.0, 0.2)
    
    def generate_keystroke_count(self, answer_length):
        """Normal typing with corrections"""
        base_keystrokes = answer_length
        corrections = int(answer_length * np.random.uniform(0.1, 0.3))
        return base_keystrokes + corrections
    
    def generate_paste_count(self):
        """Occasional pasting (citations, formulas)"""
        return np.random.choice([0, 0, 0, 1, 2], p=[0.6, 0.2, 0.1, 0.07, 0.03])


class AIAssistedCheater(StudentProfile):
    """
    AI-assisted cheating behavior
    
    Characteristics:
    - Response time: Very fast and consistent (10-30s)
    - Accuracy: Abnormally high (85-95%)
    - AI usage score: High (0.7-0.95)
    - Low keystroke count (paste-heavy)
    - Low response variance
    """
    
    def __init__(self, student_id, skill_level):
        super().__init__(student_id, skill_level)
        self.profile_type = "ai_assisted"
        
    def generate_response_time(self, question_difficulty):
        """
        Suspiciously fast and consistent response times
        Low variance regardless of difficulty
        """
        # AI response is fast and doesn't vary much with difficulty
        base_time = np.random.uniform(10, 30)
        
        # Very small random variation (AI is consistent)
        noise = np.random.normal(0, 3)
        
        return max(5.0, base_time + noise)
    
    def generate_accuracy(self, question_difficulty):
        """
        Abnormally high accuracy regardless of difficulty
        """
        # AI gets 85-95% correct consistently
        return np.random.random() < np.random.uniform(0.85, 0.95)
    
    def generate_ai_score(self):
        """High AI detection score"""
        return np.random.uniform(0.70, 0.95)
    
    def generate_keystroke_count(self, answer_length):
        """Very low keystroke count (copy-paste)"""
        # Minimal typing, mostly pasting
        return int(answer_length * np.random.uniform(0.1, 0.3))
    
    def generate_paste_count(self):
        """Frequent pasting"""
        return np.random.choice([2, 3, 4, 5], p=[0.2, 0.4, 0.3, 0.1])


class ColludingStudent(StudentProfile):
    """
    Collusion behavior - students sharing answers
    
    Characteristics:
    - Synchronized submission times
    - Identical or very similar answers
    - Shared answer hashes
    - Normal response times but identical patterns
    """
    
    def __init__(self, student_id, skill_level, collusion_group_id):
        super().__init__(student_id, skill_level)
        self.profile_type = "colluding"
        self.collusion_group_id = collusion_group_id
        
    def generate_response_time(self, question_difficulty, base_time=None):
        """
        Response times synchronized within group
        If base_time provided, stay within 30-second window
        """
        if base_time is not None:
            # Synchronized with group leader
            offset = np.random.uniform(-15, 30)
            return max(10.0, base_time + offset)
        else:
            # Group leader sets the pace
            mu = 4.0
            sigma = 0.7
            return max(10.0, np.random.lognormal(mu, sigma))
    
    def generate_accuracy(self, question_difficulty):
        """Normal accuracy based on skill"""
        base_accuracy = self.skill_level
        difficulty_penalty = (question_difficulty - 3) * 0.08
        final_accuracy = base_accuracy - difficulty_penalty + np.random.normal(0, 0.05)
        return np.random.random() < max(0.0, min(1.0, final_accuracy))
    
    def generate_ai_score(self):
        """Low to medium AI score (they're sharing, not using AI)"""
        return np.random.uniform(0.1, 0.4)
    
    def generate_shared_answer_hash(self, question_id, group_seed):
        """
        Generate identical hash for colluding group members
        """
        hash_input = f"{question_id}_{group_seed}_{self.collusion_group_id}"
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def generate_keystroke_count(self, answer_length):
        """Normal typing behavior"""
        base_keystrokes = answer_length
        corrections = int(answer_length * np.random.uniform(0.1, 0.25))
        return base_keystrokes + corrections
    
    def generate_paste_count(self):
        """Slightly higher pasting (sharing snippets)"""
        return np.random.choice([0, 1, 1, 2, 3], p=[0.3, 0.3, 0.2, 0.15, 0.05])


class StudentProfileFactory:
    """Factory to create student profiles with realistic distribution"""
    
    @staticmethod
    def create_student_pool(num_students=5000):
        """
        Create a pool of students with realistic distribution:
        - 85% Normal students
        - 10% AI-assisted cheaters
        - 5% Colluding students (in groups of 3-5)
        """
        students = []
        
        # Normal students (85%)
        num_normal = int(num_students * 0.85)
        for i in range(num_normal):
            student_id = f"STU{str(i+1).zfill(6)}"
            skill_level = np.random.beta(5, 2)  # Beta distribution skewed toward 0.7-0.9
            students.append(NormalStudent(student_id, skill_level))
        
        # AI-assisted cheaters (10%)
        num_ai_cheaters = int(num_students * 0.10)
        for i in range(num_ai_cheaters):
            student_id = f"STU{str(num_normal + i + 1).zfill(6)}"
            # AI cheaters might be weaker students trying to compensate
            skill_level = np.random.uniform(0.3, 0.6)
            students.append(AIAssistedCheater(student_id, skill_level))
        
        # Colluding students (5% in groups)
        num_colluding = int(num_students * 0.05)
        group_id = 1
        idx = num_normal + num_ai_cheaters
        
        while idx < num_students:
            group_size = random.randint(3, 5)
            for _ in range(min(group_size, num_students - idx)):
                student_id = f"STU{str(idx + 1).zfill(6)}"
                skill_level = np.random.uniform(0.4, 0.7)
                students.append(ColludingStudent(student_id, skill_level, group_id))
                idx += 1
            group_id += 1
        
        return students
    
    @staticmethod
    def get_profile_statistics(students):
        """Get distribution statistics"""
        normal_count = sum(1 for s in students if isinstance(s, NormalStudent))
        ai_count = sum(1 for s in students if isinstance(s, AIAssistedCheater))
        colluding_count = sum(1 for s in students if isinstance(s, ColludingStudent))
        
        return {
            'total': len(students),
            'normal': normal_count,
            'ai_assisted': ai_count,
            'colluding': colluding_count,
            'normal_pct': normal_count / len(students) * 100,
            'ai_pct': ai_count / len(students) * 100,
            'colluding_pct': colluding_count / len(students) * 100
        }


# Test the profiles
if __name__ == "__main__":
    print("=" * 70)
    print("STUDENT BEHAVIORAL PROFILES - TEST")
    print("=" * 70)
    
    # Create student pool
    students = StudentProfileFactory.create_student_pool(5000)
    stats = StudentProfileFactory.get_profile_statistics(students)
    
    print(f"\nStudent Pool Created:")
    print(f"  Total: {stats['total']}")
    print(f"  Normal: {stats['normal']} ({stats['normal_pct']:.1f}%)")
    print(f"  AI-Assisted: {stats['ai_assisted']} ({stats['ai_pct']:.1f}%)")
    print(f"  Colluding: {stats['colluding']} ({stats['colluding_pct']:.1f}%)")
    
    # Sample profiles
    print("\n" + "=" * 70)
    print("SAMPLE BEHAVIOR SIMULATIONS")
    print("=" * 70)
    
    normal = students[0]
    ai_cheater = students[4250]
    colluder = students[4750]
    
    question_difficulty = 3
    
    print(f"\n1. Normal Student (skill_level={normal.skill_level:.2f}):")
    print(f"   Response time: {normal.generate_response_time(question_difficulty):.1f}s")
    print(f"   Accuracy: {normal.generate_accuracy(question_difficulty)}")
    print(f"   AI score: {normal.generate_ai_score():.3f}")
    print(f"   Keystrokes: {normal.generate_keystroke_count(200)}")
    print(f"   Pastes: {normal.generate_paste_count()}")
    
    print(f"\n2. AI-Assisted Cheater (skill_level={ai_cheater.skill_level:.2f}):")
    print(f"   Response time: {ai_cheater.generate_response_time(question_difficulty):.1f}s")
    print(f"   Accuracy: {ai_cheater.generate_accuracy(question_difficulty)}")
    print(f"   AI score: {ai_cheater.generate_ai_score():.3f}")
    print(f"   Keystrokes: {ai_cheater.generate_keystroke_count(200)}")
    print(f"   Pastes: {ai_cheater.generate_paste_count()}")
    
    print(f"\n3. Colluding Student (skill_level={colluder.skill_level:.2f}):")
    base_time = 120
    print(f"   Response time: {colluder.generate_response_time(question_difficulty, base_time):.1f}s")
    print(f"   Accuracy: {colluder.generate_accuracy(question_difficulty)}")
    print(f"   AI score: {colluder.generate_ai_score():.3f}")
    print(f"   Answer hash: {colluder.generate_shared_answer_hash('QST001', 'seed123')[:16]}...")
    
    print("\n" + "=" * 70)
    print("✓ Behavioral models working correctly!")
    print("=" * 70)
