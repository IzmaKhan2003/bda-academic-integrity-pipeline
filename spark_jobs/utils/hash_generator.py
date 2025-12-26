"""
Answer Hash Generation for Collusion Detection
Phase 4 - Step 15: Consistent hashing for identical answers

File: spark_jobs/utils/hash_generator.py
"""

import hashlib
import re
from typing import List, Tuple


class AnswerHashGenerator:
    """
    Generate cryptographic hashes of answers for collusion detection
    Identical or highly similar answers will have matching hashes
    """
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize answer text before hashing
        - Remove extra whitespace
        - Convert to lowercase
        - Remove punctuation (except essential chars)
        - Standardize format
        
        This ensures minor formatting differences don't affect hash
        """
        # Convert to lowercase
        text = text.lower()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove most punctuation, keep essential chars
        text = re.sub(r'[^\w\s\-+=/*()[\]{}]', '', text)
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text
    
    @staticmethod
    def generate_hash(answer_text: str, algorithm='sha256') -> str:
        """
        Generate SHA256 hash of normalized answer
        
        Args:
            answer_text: Raw answer text
            algorithm: Hash algorithm (default: sha256)
        
        Returns:
            Hexadecimal hash string
        """
        # Normalize first
        normalized = AnswerHashGenerator.normalize_text(answer_text)
        
        # Generate hash
        if algorithm == 'sha256':
            hash_obj = hashlib.sha256(normalized.encode('utf-8'))
        elif algorithm == 'md5':
            hash_obj = hashlib.md5(normalized.encode('utf-8'))
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        return hash_obj.hexdigest()
    
    @staticmethod
    def generate_fuzzy_hash(answer_text: str, chunk_size=10) -> str:
        """
        Generate fuzzy hash for detecting similar (not identical) answers
        Breaks text into chunks and hashes key parts
        
        Args:
            answer_text: Raw answer text
            chunk_size: Number of words per chunk
        
        Returns:
            Fuzzy hash (shorter than normal hash)
        """
        normalized = AnswerHashGenerator.normalize_text(answer_text)
        words = normalized.split()
        
        # Take chunks of words
        chunks = []
        for i in range(0, len(words), chunk_size):
            chunk = ' '.join(words[i:i + chunk_size])
            chunks.append(chunk)
        
        # Hash each chunk and combine
        chunk_hashes = []
        for chunk in chunks[:5]:  # Only first 5 chunks for speed
            h = hashlib.md5(chunk.encode('utf-8')).hexdigest()[:8]
            chunk_hashes.append(h)
        
        return '-'.join(chunk_hashes)
    
    @staticmethod
    def calculate_jaccard_similarity(hash1: str, hash2: str) -> float:
        """
        Calculate Jaccard similarity between two fuzzy hashes
        
        Args:
            hash1: First fuzzy hash
            hash2: Second fuzzy hash
        
        Returns:
            Similarity score (0.0-1.0)
        """
        set1 = set(hash1.split('-'))
        set2 = set(hash2.split('-'))
        
        if not set1 and not set2:
            return 0.0
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    @staticmethod
    def generate_structural_hash(answer_text: str) -> str:
        """
        Generate hash based on answer structure (length, format)
        Useful for detecting copy-paste with minor modifications
        
        Args:
            answer_text: Raw answer text
        
        Returns:
            Structural hash
        """
        normalized = AnswerHashGenerator.normalize_text(answer_text)
        
        # Extract structural features
        features = {
            'length': len(normalized),
            'word_count': len(normalized.split()),
            'digit_count': sum(c.isdigit() for c in normalized),
            'upper_count': sum(c.isupper() for c in answer_text),  # Use original
            'has_code': 'def ' in normalized or 'function' in normalized or 'return' in normalized
        }
        
        # Create feature string
        feature_str = f"{features['length']}-{features['word_count']}-{features['digit_count']}"
        
        # Hash it
        return hashlib.md5(feature_str.encode('utf-8')).hexdigest()[:16]


class CollusionDetector:
    """
    Detect collusion using answer hashes
    """
    
    @staticmethod
    def find_matching_pairs(answers: List[Tuple[str, str, str]]) -> List[dict]:
        """
        Find student pairs with identical answer hashes
        
        Args:
            answers: List of (student_id, question_id, answer_hash) tuples
        
        Returns:
            List of suspicious pairs with similarity scores
        """
        # Group by question_id and answer_hash
        hash_groups = {}
        
        for student_id, question_id, answer_hash in answers:
            key = (question_id, answer_hash)
            if key not in hash_groups:
                hash_groups[key] = []
            hash_groups[key].append(student_id)
        
        # Find pairs with matching hashes
        suspicious_pairs = []
        
        for (question_id, answer_hash), students in hash_groups.items():
            if len(students) >= 2:
                # All pairs of students with same hash
                for i in range(len(students)):
                    for j in range(i + 1, len(students)):
                        suspicious_pairs.append({
                            'student_1': students[i],
                            'student_2': students[j],
                            'question_id': question_id,
                            'answer_hash': answer_hash,
                            'similarity': 1.0  # Identical
                        })
        
        return suspicious_pairs
    
    @staticmethod
    def calculate_pair_suspicion_score(
        pairs: List[dict],
        threshold_questions=5
    ) -> List[dict]:
        """
        Calculate overall suspicion score for student pairs
        
        Args:
            pairs: List of suspicious pairs from find_matching_pairs
            threshold_questions: Minimum matching questions for high suspicion
        
        Returns:
            List of pairs with suspicion scores
        """
        # Group by student pair
        pair_counts = {}
        
        for pair in pairs:
            key = tuple(sorted([pair['student_1'], pair['student_2']]))
            if key not in pair_counts:
                pair_counts[key] = {
                    'student_1': key[0],
                    'student_2': key[1],
                    'matching_questions': 0,
                    'questions': []
                }
            pair_counts[key]['matching_questions'] += 1
            pair_counts[key]['questions'].append(pair['question_id'])
        
        # Calculate suspicion score
        scored_pairs = []
        for key, data in pair_counts.items():
            # Score based on number of matching questions
            suspicion_score = min(data['matching_questions'] / threshold_questions, 1.0)
            
            data['suspicion_score'] = suspicion_score
            data['risk_level'] = (
                'High' if suspicion_score >= 0.7 else
                'Medium' if suspicion_score >= 0.4 else
                'Low'
            )
            
            scored_pairs.append(data)
        
        # Sort by suspicion score
        scored_pairs.sort(key=lambda x: x['suspicion_score'], reverse=True)
        
        return scored_pairs


def generate_answer_text_samples():
    """Generate sample answer texts for testing"""
    samples = {
        'original': """
        The time complexity of binary search is O(log n) because we divide 
        the search space in half with each iteration. This makes it much more 
        efficient than linear search which has O(n) complexity.
        """,
        
        'identical': """
        The time complexity of binary search is O(log n) because we divide 
        the search space in half with each iteration. This makes it much more 
        efficient than linear search which has O(n) complexity.
        """,
        
        'slightly_modified': """
        The time complexity of binary search is O(log n) as we divide 
        the search space by half with each iteration. This is much more 
        efficient than linear search which is O(n) complexity.
        """,
        
        'different': """
        Binary search is fast. It works by looking at the middle element
        and deciding which half to search next. This process continues
        until the element is found.
        """
    }
    
    return samples


if __name__ == "__main__":
    print("=" * 70)
    print("ANSWER HASH GENERATOR - Testing")
    print("=" * 70 + "\n")
    
    # Get sample answers
    samples = generate_answer_text_samples()
    
    # Test hash generation
    print("1. Hash Generation:\n")
    for name, text in samples.items():
        hash_val = AnswerHashGenerator.generate_hash(text)
        print(f"{name}:")
        print(f"  Hash: {hash_val[:32]}...")
        print()
    
    # Test collusion detection
    print("\n2. Collusion Detection:\n")
    
    # Simulate answers from 3 students
    answers = [
        ('STU001', 'QST001', AnswerHashGenerator.generate_hash(samples['original'])),
        ('STU002', 'QST001', AnswerHashGenerator.generate_hash(samples['identical'])),
        ('STU003', 'QST001', AnswerHashGenerator.generate_hash(samples['different'])),
        
        ('STU001', 'QST002', AnswerHashGenerator.generate_hash(samples['original'])),
        ('STU002', 'QST002', AnswerHashGenerator.generate_hash(samples['identical'])),
        
        ('STU001', 'QST003', AnswerHashGenerator.generate_hash(samples['original'])),
        ('STU002', 'QST003', AnswerHashGenerator.generate_hash(samples['identical'])),
    ]
    
    # Detect collusion
    detector = CollusionDetector()
    pairs = detector.find_matching_pairs(answers)
    
    print(f"Found {len(pairs)} matching answer instances\n")
    
    # Calculate suspicion scores
    scored_pairs = detector.calculate_pair_suspicion_score(pairs, threshold_questions=3)
    
    print("Student Pairs with Suspicion Scores:\n")
    for pair in scored_pairs:
        print(f"Students: {pair['student_1']} & {pair['student_2']}")
        print(f"  Matching Questions: {pair['matching_questions']}")
        print(f"  Suspicion Score: {pair['suspicion_score']:.2f}")
        print(f"  Risk Level: {pair['risk_level']}")
        print(f"  Questions: {', '.join(pair['questions'])}")
        print()
    
    # Test fuzzy hashing
    print("\n3. Fuzzy Hash (for similarity detection):\n")
    fuzzy1 = AnswerHashGenerator.generate_fuzzy_hash(samples['original'])
    fuzzy2 = AnswerHashGenerator.generate_fuzzy_hash(samples['slightly_modified'])
    fuzzy3 = AnswerHashGenerator.generate_fuzzy_hash(samples['different'])
    
    print(f"Original fuzzy hash: {fuzzy1}")
    print(f"Modified fuzzy hash: {fuzzy2}")
    print(f"Different fuzzy hash: {fuzzy3}\n")
    
    sim_1_2 = AnswerHashGenerator.calculate_jaccard_similarity(fuzzy1, fuzzy2)
    sim_1_3 = AnswerHashGenerator.calculate_jaccard_similarity(fuzzy1, fuzzy3)
    
    print(f"Similarity (original vs modified): {sim_1_2:.2f}")
    print(f"Similarity (original vs different): {sim_1_3:.2f}")
    
    print("\n" + "=" * 70)
    print("Testing complete!")
    print("=" * 70)
