"""
Statistical Distribution Generators
Phase 4 - Step 13: Realistic probability distributions

File: spark_jobs/utils/distributions.py
"""

import numpy as np
from scipy import stats
from datetime import datetime, timedelta


class DistributionGenerator:
    """Statistical distribution generators for realistic data"""
    
    @staticmethod
    def log_normal_response_time(mu=3.5, sigma=0.8, difficulty_factor=1.0):
        """
        Generate response time using log-normal distribution
        
        Args:
            mu: Mean on log scale (3.5 ≈ 33 seconds)
            sigma: Std dev on log scale (0.8 = moderate variance)
            difficulty_factor: Multiplier for difficulty (1-5)
        
        Returns:
            Response time in seconds (10-600s range)
        """
        adjusted_mu = mu + (difficulty_factor - 3) * 0.3
        response_time = np.random.lognormal(adjusted_mu, sigma)
        return np.clip(response_time, 10, 600)
    
    @staticmethod
    def exponential_idle_time(lambda_param=0.05, max_time=600):
        """
        Generate idle time using exponential distribution
        
        Args:
            lambda_param: Rate parameter (0.05 = mean 20s)
            max_time: Maximum idle time in seconds
        
        Returns:
            Idle time in seconds
        """
        idle_time = np.random.exponential(1 / lambda_param)
        return min(idle_time, max_time)
    
    @staticmethod
    def poisson_discrete_events(lambda_param=2):
        """
        Generate discrete event counts (tab switches, copy operations)
        
        Args:
            lambda_param: Average number of events
        
        Returns:
            Count of events (non-negative integer)
        """
        return np.random.poisson(lambda_param)
    
    @staticmethod
    def uniform_ai_score(min_val=0.0, max_val=1.0):
        """
        Generate AI usage score uniformly
        
        Args:
            min_val: Minimum score
            max_val: Maximum score
        
        Returns:
            AI usage probability (0.0-1.0)
        """
        return np.random.uniform(min_val, max_val)
    
    @staticmethod
    def beta_skill_distribution(alpha=2, beta=2):
        """
        Generate student skill levels using beta distribution
        
        Args:
            alpha: Shape parameter (2 = moderate skills)
            beta: Shape parameter (2 = symmetric)
        
        Returns:
            Skill level (0.0-1.0)
        """
        return np.random.beta(alpha, beta)
    
    @staticmethod
    def normal_marks(mean, std_dev, max_marks):
        """
        Generate marks obtained using normal distribution
        
        Args:
            mean: Average marks
            std_dev: Standard deviation
            max_marks: Maximum possible marks
        
        Returns:
            Marks obtained (0 to max_marks)
        """
        marks = np.random.normal(mean, std_dev)
        return int(np.clip(marks, 0, max_marks))
    
    @staticmethod
    def binomial_accuracy(n_questions, p_correct):
        """
        Generate number of correct answers
        
        Args:
            n_questions: Total questions attempted
            p_correct: Probability of correct answer
        
        Returns:
            Number of correct answers
        """
        return np.random.binomial(n_questions, p_correct)
    
    @staticmethod
    def generate_timestamp_distribution(start_time, duration_minutes, n_events):
        """
        Generate realistic submission timestamps
        Events cluster at start, middle, and end of exam
        
        Args:
            start_time: Exam start datetime
            duration_minutes: Exam duration
            n_events: Number of events to generate
        
        Returns:
            List of datetime objects
        """
        # Convert to seconds
        duration_seconds = duration_minutes * 60
        
        # Use beta distribution to cluster submissions
        # Most at start, some throughout, many at end
        beta_samples = np.random.beta(2, 5, size=n_events)
        
        # Convert to actual timestamps
        timestamps = []
        for sample in beta_samples:
            offset_seconds = sample * duration_seconds
            timestamp = start_time + timedelta(seconds=offset_seconds)
            timestamps.append(timestamp)
        
        return sorted(timestamps)
    
    @staticmethod
    def generate_correlated_timing(base_time, correlation=0.8, std_dev=30):
        """
        Generate correlated timing for colluding students
        
        Args:
            base_time: Partner's submission time
            correlation: How closely they coordinate (0-1)
            std_dev: Time difference std dev (seconds)
        
        Returns:
            Correlated submission time
        """
        if np.random.random() < correlation:
            # Coordinated timing
            time_diff = np.random.normal(0, std_dev)
            return base_time + timedelta(seconds=time_diff)
        else:
            # Independent timing
            return base_time + timedelta(seconds=np.random.uniform(-300, 300))


class DataQualityGenerator:
    """Generate realistic data quality issues"""
    
    @staticmethod
    def add_missing_values(data, missing_rate=0.02):
        """
        Introduce missing values realistically
        
        Args:
            data: List or array of values
            missing_rate: Probability of missing (0-1)
        
        Returns:
            Data with some None/NaN values
        """
        n = len(data)
        mask = np.random.random(n) < missing_rate
        result = data.copy() if isinstance(data, list) else data
        result[mask] = None
        return result
    
    @staticmethod
    def add_outliers(data, outlier_rate=0.01, multiplier=3):
        """
        Add realistic outliers
        
        Args:
            data: Numeric array
            outlier_rate: Probability of outlier
            multiplier: How extreme (std devs from mean)
        
        Returns:
            Data with outliers
        """
        data = np.array(data)
        mean = np.mean(data)
        std = np.std(data)
        
        n = len(data)
        outlier_mask = np.random.random(n) < outlier_rate
        
        result = data.copy()
        result[outlier_mask] = mean + multiplier * std * np.random.choice([-1, 1], size=sum(outlier_mask))
        
        return result


class TimeSeriesGenerator:
    """Generate time-series patterns"""
    
    @staticmethod
    def generate_exam_schedule(start_date, n_exams, spacing_days=7):
        """
        Generate exam schedule with realistic spacing
        
        Args:
            start_date: First exam date
            n_exams: Number of exams to generate
            spacing_days: Average days between exams
        
        Returns:
            List of exam datetime objects
        """
        exam_dates = []
        current_date = start_date
        
        for i in range(n_exams):
            # Add some randomness (±2 days)
            offset_days = spacing_days + np.random.randint(-2, 3)
            current_date += timedelta(days=offset_days)
            
            # Set time between 9 AM and 5 PM
            hour = np.random.choice([9, 10, 11, 14, 15, 16, 17])
            exam_time = current_date.replace(hour=hour, minute=0, second=0)
            
            exam_dates.append(exam_time)
        
        return exam_dates
    
    @staticmethod
    def generate_trend_with_seasonality(n_points, base_value=0.5, trend=0.01, seasonality=0.1):
        """
        Generate time series with trend and seasonality
        
        Args:
            n_points: Number of data points
            base_value: Starting value
            trend: Linear trend coefficient
            seasonality: Amplitude of seasonal variation
        
        Returns:
            Array of values with trend and seasonality
        """
        t = np.arange(n_points)
        
        # Linear trend
        trend_component = base_value + trend * t
        
        # Seasonal component (weekly pattern)
        seasonal_component = seasonality * np.sin(2 * np.pi * t / 7)
        
        # Random noise
        noise = np.random.normal(0, 0.05, n_points)
        
        series = trend_component + seasonal_component + noise
        
        return np.clip(series, 0, 1)


# Utility functions for specific use cases
def generate_realistic_answer_length():
    """Generate realistic answer text length (characters)"""
    # Most answers: 50-500 characters
    # Distribution: skewed right (some very long essays)
    length = int(np.random.gamma(shape=2, scale=100))
    return np.clip(length, 20, 5000)


def generate_keystroke_pattern(answer_length, profile_type="normal"):
    """
    Generate realistic keystroke count based on answer length
    
    Args:
        answer_length: Number of characters in answer
        profile_type: "normal", "ai_cheater", or "colluding"
    
    Returns:
        (keystroke_count, paste_count)
    """
    if profile_type == "normal":
        # Normal typing: 1.2-1.8x length (with backspaces)
        keystrokes = int(answer_length * np.random.uniform(1.2, 1.8))
        pastes = np.random.choice([0, 1, 2], p=[0.7, 0.25, 0.05])
    
    elif profile_type == "ai_cheater":
        # Minimal typing (mostly paste)
        keystrokes = int(answer_length * np.random.uniform(0.1, 0.3))
        pastes = np.random.choice([2, 3, 4, 5], p=[0.3, 0.4, 0.2, 0.1])
    
    elif profile_type == "colluding":
        # Normal-ish with some paste
        keystrokes = int(answer_length * np.random.uniform(1.0, 1.5))
        pastes = np.random.choice([0, 1, 2], p=[0.4, 0.4, 0.2])
    
    else:
        keystrokes = int(answer_length * np.random.uniform(1.0, 1.5))
        pastes = 0
    
    return keystrokes, pastes


def generate_ip_address(region="North America"):
    """Generate realistic IP address based on region"""
    # Simplified IP generation by region
    region_prefixes = {
        "North America": ["192.168", "10.0", "172.16"],
        "Europe": ["193.0", "194.0", "195.0"],
        "Asia": ["202.0", "203.0", "210.0"],
        "South America": ["200.0", "201.0"],
        "Africa": "196.0",
        "Oceania": ["203.0", "210.0"]
    }
    
    prefix = np.random.choice(region_prefixes.get(region, ["192.168"]))
    suffix1 = np.random.randint(0, 256)
    suffix2 = np.random.randint(1, 255)
    
    return f"{prefix}.{suffix1}.{suffix2}"


if __name__ == "__main__":
    print("Testing Statistical Distributions\n")
    
    # Test response time distribution
    print("Response Time Distribution (Normal Student):")
    times = [DistributionGenerator.log_normal_response_time() for _ in range(1000)]
    print(f"  Mean: {np.mean(times):.1f}s")
    print(f"  Std Dev: {np.std(times):.1f}s")
    print(f"  Min: {np.min(times):.1f}s, Max: {np.max(times):.1f}s\n")
    
    # Test AI cheater (fast)
    print("Response Time Distribution (AI Cheater):")
    times_ai = [DistributionGenerator.log_normal_response_time(mu=1.2, sigma=0.2) for _ in range(1000)]
    print(f"  Mean: {np.mean(times_ai):.1f}s")
    print(f"  Std Dev: {np.std(times_ai):.1f}s (very low variance!)")
    print(f"  Min: {np.min(times_ai):.1f}s, Max: {np.max(times_ai):.1f}s\n")
    
    # Test other distributions
    print("Other Distributions:")
    print(f"  Idle time: {DistributionGenerator.exponential_idle_time():.1f}s")
    print(f"  Tab switches: {DistributionGenerator.poisson_discrete_events(lambda_param=5)}")
    print(f"  AI score: {DistributionGenerator.uniform_ai_score(0.7, 0.95):.2f}")
    print(f"  Skill level: {DistributionGenerator.beta_skill_distribution():.2f}")
    
    # Test timestamp generation
    print("\nTimestamp Distribution:")
    start = datetime.now()
    timestamps = DistributionGenerator.generate_timestamp_distribution(start, 120, 50)
    print(f"  Generated {len(timestamps)} timestamps over 120 minutes")
    print(f"  First: {timestamps[0].strftime('%H:%M:%S')}")
    print(f"  Last: {timestamps[-1].strftime('%H:%M:%S')}")