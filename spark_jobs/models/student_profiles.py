"""
Student Behavior Profile Classes
Phase 4 - Step 12: Statistical modeling of student behaviors

File: spark_jobs/models/student_profiles.py
"""

import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import hashlib


class StudentProfile:
    """Base class for all student behavioral profiles"""
    
    def __init__(self, student_id, skill_level):
        self.student_id = student_id
        self.skill_level = skill_level  # 0.0 - 1.0
        self.profile_type = "base"
    
    def generate_response_time(self, question_difficulty):
        """Generate response time based on question difficulty"""
        raise NotImplementedError
    
    def generate_accuracy(self, question_difficulty):
        """Determine if answer is correct"""
        raise NotImplementedError
    
    def generate_ai_score(self):
        """Generate AI usage probability"""
        raise NotImplementedError


class NormalStudent(StudentProfile):
    """
    Normal student with realistic human behavior
    - Response time: Log-normal distribution
    - Accuracy: Skill-based with variability
    - Low AI usage
    """
    
    def __init__(self, student_id, skill_level):
        super().__init__(student_id, skill_level)
        self.profile_type = "normal"
        
        # Log-normal parameters for response time
        self.response_mu = 3.5  # Mean on log scale
        self.response_sigma = 0.8  # Std dev on log scale
    
    def generate_response_time(self, question_difficulty):
        """
        Response time follows log-normal distribution
        Harder questions take longer
        """
        # Adjust mean based on difficulty (1-5)
        adjusted_mu = self.response_mu + (question_difficulty - 3) * 0.3
        
        # Generate log-normal response time
        response_time = np.random.lognormal(adjusted_mu, self.response_sigma)
        
        # Clip to reasonable bounds (10s - 600s)
        return np.clip(response_time, 10, 600)
    
    def generate_accuracy(self, question_difficulty):
        """
        Probability of correct answer based on skill vs difficulty
        """
        # Base probability from skill level
        base_prob = self.skill_level
        
        # Adjust for question difficulty (1=easy, 5=hard)
        difficulty_penalty = (question_difficulty - 1) * 0.15
        prob_correct = base_prob - difficulty_penalty
        
        # Add some randomness (±10%)
        prob_correct += np.random.uniform(-0.1, 0.1)
        
        # Clip to valid range
        prob_correct = np.clip(prob_correct, 0.1, 0.95)
        
        return np.random.random() < prob_correct
    
    def generate_ai_score(self):
        """Low AI usage for normal students"""
        return np.random.uniform(0.0, 0.3)
    
    def generate_keystrokes(self, answer_length):
        """Realistic keystroke patterns"""
        # Normal typing with some backspaces
        base_keystrokes = answer_length * np.random.uniform(1.2, 1.8)
        return int(base_keystrokes)
    
    def generate_paste_count(self):
        """Occasional paste operations"""
        return np.random.choice([0, 1, 2], p=[0.7, 0.25, 0.05])


class AIAssistedCheater(StudentProfile):
    """
    Student using AI tools (ChatGPT, etc.)
    - Very fast response times with low variance
    - High accuracy regardless of skill
    - High AI detection score
    - Minimal keystrokes (copy-paste heavy)
    """
    
    def __init__(self, student_id, skill_level):
        super().__init__(student_id, skill_level)
        self.profile_type = "ai_cheater"
        
        # Much faster and more consistent
        self.response_mu = 1.2  # Very fast on log scale
        self.response_sigma = 0.2  # Very low variance
    
    def generate_response_time(self, question_difficulty):
        """
        Suspiciously fast and consistent response times
        AI doesn't care about difficulty
        """
        # Minimal difficulty adjustment (AI is fast regardless)
        adjusted_mu = self.response_mu + (question_difficulty - 3) * 0.05
        
        response_time = np.random.lognormal(adjusted_mu, self.response_sigma)
        
        # AI cheaters: 15-60 seconds typically
        return np.clip(response_time, 15, 60)
    
    def generate_accuracy(self, question_difficulty):
        """
        High accuracy regardless of student skill or difficulty
        """
        # AI tools have high base accuracy
        prob_correct = np.random.uniform(0.85, 0.98)
        
        return np.random.random() < prob_correct
    
    def generate_ai_score(self):
        """High AI usage detection"""
        return np.random.uniform(0.75, 0.98)
    
    def generate_keystrokes(self, answer_length):
        """Very few keystrokes (mostly paste)"""
        # Just a few edits after pasting
        return int(answer_length * np.random.uniform(0.1, 0.3))
    
    def generate_paste_count(self):
        """Heavy paste usage"""
        return np.random.choice([2, 3, 4, 5], p=[0.3, 0.4, 0.2, 0.1])


class ColludingStudent(StudentProfile):
    """
    Students coordinating answers with others
    - Response times synchronized with partner
    - Identical answer hashes
    - Shared submission timing patterns
    """
    
    def __init__(self, student_id, skill_level, partner_id=None):
        super().__init__(student_id, skill_level)
        self.profile_type = "colluding"
        self.partner_id = partner_id
        
        # Normal-ish response times
        self.response_mu = 3.2
        self.response_sigma = 0.6
        
        # Shared seed for synchronized behavior
        self.shared_seed = int(hashlib.md5(
            f"{student_id}{partner_id}".encode()
        ).hexdigest()[:8], 16)
    
    def generate_response_time(self, question_difficulty, partner_time=None):
        """
        If partner_time provided, synchronize within 30-60 seconds
        """
        if partner_time is not None:
            # Coordinate timing with partner
            time_diff = np.random.uniform(-30, 30)
            return max(10, partner_time + time_diff)
        
        # Otherwise, normal timing
        adjusted_mu = self.response_mu + (question_difficulty - 3) * 0.25
        response_time = np.random.lognormal(adjusted_mu, self.response_sigma)
        return np.clip(response_time, 10, 500)
    
    def generate_accuracy(self, question_difficulty):
        """Moderate accuracy (shared answers may have errors)"""
        prob_correct = self.skill_level - (question_difficulty - 1) * 0.12
        prob_correct = np.clip(prob_correct, 0.2, 0.85)
        return np.random.random() < prob_correct
    
    def generate_ai_score(self):
        """Moderate AI usage (may use tools to share answers)"""
        return np.random.uniform(0.3, 0.6)
    
    def generate_shared_answer_hash(self, question_id, base_hash):
        """
        Generate answer hash that matches partner's
        Used for collusion detection
        """
        # Use shared seed to generate consistent hash
        np.random.seed(self.shared_seed + int(question_id[3:]))
        
        # 80% chance of identical answer hash
        if np.random.random() < 0.8:
            return base_hash
        else:
            # Slight variation
            return hashlib.sha256(
                f"{base_hash}{np.random.randint(0, 10)}".encode()
            ).hexdigest()
    
    def generate_keystrokes(self, answer_length):
        """Normal keystroke patterns"""
        return int(answer_length * np.random.uniform(1.0, 1.5))
    
    def generate_paste_count(self):
        """Some paste usage (copying from chat)"""
        return np.random.choice([0, 1, 2], p=[0.4, 0.4, 0.2])


class SessionBehaviorGenerator:
    """
    Generates session-level behavioral data
    Corresponds to session_logs collection
    """
    
    @staticmethod
    def generate_tab_switches(profile_type):
        """Number of tab switches during exam"""
        if profile_type == "normal":
            # Few tab switches (0-5)
            return np.random.poisson(2)
        elif profile_type == "ai_cheater":
            # Many tab switches (looking up ChatGPT)
            return np.random.poisson(12)
        elif profile_type == "colluding":
            # Moderate (messaging partner)
            return np.random.poisson(8)
        else:
            return np.random.poisson(3)
    
    @staticmethod
    def generate_idle_time(exam_duration_minutes, profile_type):
        """Idle time in seconds"""
        total_seconds = exam_duration_minutes * 60
        
        if profile_type == "normal":
            # 5-15% idle time (thinking)
            idle_ratio = np.random.uniform(0.05, 0.15)
        elif profile_type == "ai_cheater":
            # Very little idle time (fast responses)
            idle_ratio = np.random.uniform(0.01, 0.05)
        elif profile_type == "colluding":
            # Moderate idle time (waiting for partner)
            idle_ratio = np.random.uniform(0.08, 0.20)
        else:
            idle_ratio = np.random.uniform(0.05, 0.15)
        
        return total_seconds * idle_ratio
    
    @staticmethod
    def generate_focus_loss(profile_type):
        """Times window lost focus"""
        if profile_type == "normal":
            return np.random.poisson(3)
        elif profile_type == "ai_cheater":
            return np.random.poisson(10)
        elif profile_type == "colluding":
            return np.random.poisson(7)
        else:
            return np.random.poisson(4)
    
    @staticmethod
    def generate_device_browser():
        """Random device and browser"""
        devices = ["Desktop", "Laptop", "Tablet", "Mobile"]
        device_probs = [0.5, 0.35, 0.10, 0.05]
        
        browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
        browser_probs = [0.50, 0.20, 0.15, 0.10, 0.05]
        
        device = np.random.choice(devices, p=device_probs)
        browser = np.random.choice(browsers, p=browser_probs)
        
        return device, browser


# Factory function to create student profiles
def create_student_profile(student_id, skill_level, profile_type="normal", partner_id=None):
    """
    Factory function to create appropriate student profile
    
    Args:
        student_id: Unique student identifier
        skill_level: Academic skill (0.0-1.0)
        profile_type: "normal", "ai_cheater", or "colluding"
        partner_id: For colluding students, their partner's ID
    
    Returns:
        StudentProfile subclass instance
    """
    if profile_type == "normal":
        return NormalStudent(student_id, skill_level)
    elif profile_type == "ai_cheater":
        return AIAssistedCheater(student_id, skill_level)
    elif profile_type == "colluding":
        return ColludingStudent(student_id, skill_level, partner_id)
    else:
        return NormalStudent(student_id, skill_level)


if __name__ == "__main__":
    # Test the profiles
    print("Testing Student Behavior Profiles\n")
    
    # Create test students
    normal = NormalStudent("STU001", 0.7)
    cheater = AIAssistedCheater("STU002", 0.5)
    colluder = ColludingStudent("STU003", 0.6, partner_id="STU004")
    
    question_difficulty = 3
    
    print("Normal Student:")
    print(f"  Response time: {normal.generate_response_time(question_difficulty):.1f}s")
    print(f"  Accuracy: {normal.generate_accuracy(question_difficulty)}")
    print(f"  AI score: {normal.generate_ai_score():.2f}\n")
    
    print("AI Cheater:")
    print(f"  Response time: {cheater.generate_response_time(question_difficulty):.1f}s")
    print(f"  Accuracy: {cheater.generate_accuracy(question_difficulty)}")
    print(f"  AI score: {cheater.generate_ai_score():.2f}\n")
    
    print("Colluding Student:")
    print(f"  Response time: {colluder.generate_response_time(question_difficulty):.1f}s")
    print(f"  Accuracy: {colluder.generate_accuracy(question_difficulty)}")
    print(f"  AI score: {colluder.generate_ai_score():.2f}\n")