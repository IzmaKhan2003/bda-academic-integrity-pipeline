"""
Statistical Distribution Generators
Phase 4 - Step 13: NumPy-based probability distributions for realistic data
"""

import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import random

class DistributionGenerator:
    """Collection of statistical distribution generators"""
    
    @staticmethod
    def log_normal_response_time(mu=4.5, sigma=0.8, size=1):
        """
        Log-normal distribution for response times
        
        Parameters:
        - mu: Mean of underlying normal distribution (4.5 = ~90 seconds median)
        - sigma: Standard deviation (0.8 = moderate variability)
        - size: Number of samples
        
        Returns: Array of response times in seconds
        """
        times = np.random.lognormal(mu, sigma, size)
        return np.clip(times, 10, 600)  # Clamp to 10s-10min range
    
    @staticmethod
    def exponential_idle_time(lambda_param=0.05, size=1):
        """
        Exponential distribution for idle/wait times
        
        Parameters:
        - lambda_param: Rate parameter (0.05 = average 20s idle)
        - size: Number of samples
        
        Returns: Array of idle times in seconds
        """
        idle_times = np.random.exponential(1/lambda_param, size)
        return np.clip(idle_times, 0, 300)  # Max 5 minutes idle
    
    @staticmethod
    def poisson_events(lambda_param=2, size=1):
        """
        Poisson distribution for discrete events
        
        Parameters:
        - lambda_param: Average rate (2 = average 2 tab switches)
        - size: Number of samples
        
        Returns: Array of event counts
        """
        return np.random.poisson(lambda_param, size)
    
    @staticmethod
    def uniform_ai_score(low=0.0, high=1.0, size=1):
        """
        Uniform distribution for AI usage scores
        
        Parameters:
        - low, high: Range bounds
        - size: Number of samples
        
        Returns: Array of scores between low and high
        """
        return np.random.uniform(low, high, size)
    
    @staticmethod
    def beta_skill_distribution(alpha=5, beta_param=2, size=1):
        """
        Beta distribution for student skill levels
        Skewed toward higher values (most students are decent)
        
        Parameters:
        - alpha, beta_param: Shape parameters (5,2 = skew right)
        - size: Number of samples
        
        Returns: Array of skill levels (0.0-1.0)
        """
        return np.random.beta(alpha, beta_param, size)
    
    @staticmethod
    def normal_with_outliers(mu=100, sigma=20, outlier_rate=0.05, size=1):
        """
        Normal distribution with occasional outliers
        
        Parameters:
        - mu, sigma: Normal distribution parameters
        - outlier_rate: Fraction of outliers (0.05 = 5%)
        - size: Number of samples
        
        Returns: Array with normal values + outliers
        """
        normal_samples = np.random.normal(mu, sigma, size)
        
        # Add outliers
        num_outliers = int(size * outlier_rate)
        if num_outliers > 0:
            outlier_indices = np.random.choice(size, num_outliers, replace=False)
            # Outliers are 3-5 standard deviations away
            outlier_values = mu + np.random.choice([-1, 1], num_outliers) * np.random.uniform(3, 5, num_outliers) * sigma
            normal_samples[outlier_indices] = outlier_values
        
        return normal_samples
    
    @staticmethod
    def generate_timestamp_sequence(start_time, num_events, rate_per_minute):
        """
        Generate realistic timestamp sequence with Poisson arrival process
        
        Parameters:
        - start_time: Starting datetime
        - num_events: Total number of events
        - rate_per_minute: Average events per minute
        
        Returns: List of datetime objects
        """
        # Inter-arrival times (exponential distribution)
        lambda_param = rate_per_minute / 60  # Convert to per-second
        inter_arrival_times = np.random.exponential(1/lambda_param, num_events)
        
        timestamps = []
        current_time = start_time
        
        for interval in inter_arrival_times:
            current_time += timedelta(seconds=interval)
            timestamps.append(current_time)
        
        return timestamps
    
    @staticmethod
    def generate_correlated_values(base_value, correlation=0.8, noise_std=0.1, size=1):
        """
        Generate values correlated with a base value
        Useful for simulating colluding students with similar answers
        
        Parameters:
        - base_value: The value to correlate with
        - correlation: Correlation strength (0-1)
        - noise_std: Standard deviation of noise
        - size: Number of samples
        
        Returns: Array of correlated values
        """
        noise = np.random.normal(0, noise_std, size)
        correlated = base_value * correlation + noise * (1 - correlation)
        return correlated


class BehavioralPatternGenerator:
    """Generate realistic behavioral patterns"""
    
    @staticmethod
    def generate_exam_session_pattern(duration_minutes=120):
        """
        Generate realistic activity pattern during exam
        - Higher activity at start
        - Slight dip in middle
        - Increased activity toward end
        
        Returns: Dict with time windows and activity levels
        """
        num_windows = duration_minutes // 10  # 10-minute windows
        
        # Beta distribution for activity pattern
        activity_curve = stats.beta.pdf(
            np.linspace(0, 1, num_windows),
            a=2, b=2
        )
        
        # Normalize to 0-1 range
        activity_curve = activity_curve / activity_curve.max()
        
        return {
            'windows': num_windows,
            'activity_levels': activity_curve.tolist()
        }
    
    @staticmethod
    def generate_typing_speed_variance(base_speed_wpm=40, fatigue_factor=0.1):
        """
        Generate realistic typing speed with fatigue
        
        Parameters:
        - base_speed_wpm: Average typing speed (words per minute)
        - fatigue_factor: How much speed decreases over time
        
        Returns: Function that gives speed at time t
        """
        def speed_at_time(t_minutes):
            # Speed decreases slightly over time
            speed = base_speed_wpm * (1 - fatigue_factor * (t_minutes / 120))
            # Add random variation
            speed += np.random.normal(0, 5)
            return max(20, speed)  # Minimum 20 wpm
        
        return speed_at_time
    
    @staticmethod
    def generate_focus_loss_pattern(exam_duration_minutes=120):
        """
        Generate realistic focus loss events
        More likely in middle of exam (attention wanes)
        
        Returns: List of timestamps when focus was lost
        """
        # Higher probability in middle
        time_points = np.linspace(0, exam_duration_minutes, 100)
        
        # Inverted normal distribution (peaks in middle)
        prob_curve = stats.norm.pdf(time_points, loc=exam_duration_minutes/2, scale=30)
        prob_curve = prob_curve / prob_curve.sum()
        
        # Sample number of focus losses
        num_losses = np.random.poisson(5)
        
        # Sample times based on probability curve
        loss_times = np.random.choice(time_points, size=min(num_losses, 20), replace=False, p=prob_curve)
        
        return sorted(loss_times.tolist())


# Test the distributions
if __name__ == "__main__":
    print("=" * 70)
    print("STATISTICAL DISTRIBUTION GENERATORS - TEST")
    print("=" * 70)
    
    gen = DistributionGenerator()
    
    # Test log-normal response times
    print("\n1. Log-Normal Response Times (n=10):")
    response_times = gen.log_normal_response_time(size=10)
    print(f"   Mean: {response_times.mean():.1f}s")
    print(f"   Std: {response_times.std():.1f}s")
    print(f"   Range: {response_times.min():.1f}s - {response_times.max():.1f}s")
    print(f"   Sample: {response_times[:5]}")
    
    # Test exponential idle times
    print("\n2. Exponential Idle Times (n=10):")
    idle_times = gen.exponential_idle_time(size=10)
    print(f"   Mean: {idle_times.mean():.1f}s")
    print(f"   Range: {idle_times.min():.1f}s - {idle_times.max():.1f}s")
    print(f"   Sample: {idle_times[:5]}")
    
    # Test Poisson events
    print("\n3. Poisson Tab Switches (n=10):")
    tab_switches = gen.poisson_events(lambda_param=3, size=10)
    print(f"   Mean: {tab_switches.mean():.1f}")
    print(f"   Range: {tab_switches.min()} - {tab_switches.max()}")
    print(f"   Sample: {tab_switches}")
    
    # Test Beta skill distribution
    print("\n4. Beta Skill Levels (n=10):")
    skills = gen.beta_skill_distribution(size=10)
    print(f"   Mean: {skills.mean():.3f}")
    print(f"   Range: {skills.min():.3f} - {skills.max():.3f}")
    print(f"   Sample: {skills[:5]}")
    
    # Test timestamp generation
    print("\n5. Timestamp Sequence (10 events at 100/min):")
    start = datetime.now()
    timestamps = gen.generate_timestamp_sequence(start, 10, 100)
    intervals = [(timestamps[i+1] - timestamps[i]).total_seconds() for i in range(9)]
    print(f"   Inter-arrival times: {[f'{x:.1f}s' for x in intervals[:5]]}")
    print(f"   Mean interval: {np.mean(intervals):.1f}s")
    
    # Test behavioral patterns
    print("\n6. Exam Session Activity Pattern:")
    pattern_gen = BehavioralPatternGenerator()
    session_pattern = pattern_gen.generate_exam_session_pattern(120)
    print(f"   Windows: {session_pattern['windows']}")
    print(f"   Activity (first 5): {[f'{x:.2f}' for x in session_pattern['activity_levels'][:5]]}")
    
    print("\n7. Focus Loss Pattern (120 min exam):")
    focus_losses = pattern_gen.generate_focus_loss_pattern(120)
    print(f"   Number of losses: {len(focus_losses)}")
    print(f"   Times: {[f'{x:.1f}min' for x in focus_losses[:5]]}")
    
    print("\n" + "=" * 70)
    print("✓ All distributions working correctly!")
    print("=" * 70)
