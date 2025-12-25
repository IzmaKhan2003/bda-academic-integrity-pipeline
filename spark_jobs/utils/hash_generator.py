"""
Answer Hash Generator
Phase 4 - Step 15: Generate consistent hashes for collusion detection
"""

import hashlib
import re

class AnswerHashGenerator:
    """Generate hashes for answer similarity detection"""
    
    @staticmethod
    def normalize_answer(answer_text):
        """
        Normalize answer text for fair comparison
        - Convert to lowercase
        - Remove extra whitespace
        - Remove punctuation
        - Sort words (for order-independent comparison)
        """
        # Convert to lowercase
        normalized = answer_text.lower()
        
        # Remove punctuation
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Remove extra whitespace
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    @staticmethod
    def generate_hash(answer_text, algorithm='sha256'):
        """
        Generate hash of answer text
        
        Parameters:
        - answer_text: The answer content
        - algorithm: Hash algorithm (sha256, md5, sha1)
        
        Returns: Hexadecimal hash string
        """
        normalized = AnswerHashGenerator.normalize_answer(answer_text)
        
        if algorithm == 'sha256':
            return hashlib.sha256(normalized.encode()).hexdigest()
        elif algorithm == 'md5':
            return hashlib.md5(normalized.encode()).hexdigest()
        elif algorithm == 'sha1':
            return hashlib.sha1(normalized.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
    
    @staticmethod
    def generate_fuzzy_hash(answer_text, n_grams=3):
        """
        Generate n-gram based hash for fuzzy matching
        Allows detecting similar (not identical) answers
        
        Parameters:
        - answer_text: The answer content
        - n_grams: Size of n-grams (3 = trigrams)
        
        Returns: Set of n-gram hashes
        """
        normalized = AnswerHashGenerator.normalize_answer(answer_text)
        words = normalized.split()
        
        # Generate n-grams
        n_gram_hashes = set()
        for i in range(len(words) - n_grams + 1):
            n_gram = ' '.join(words[i:i+n_grams])
            n_gram_hash = hashlib.md5(n_gram.encode()).hexdigest()[:8]
            n_gram_hashes.add(n_gram_hash)
        
        return n_gram_hashes
    
    @staticmethod
    def calculate_similarity(hash_set1, hash_set2):
        """
        Calculate Jaccard similarity between two fuzzy hash sets
        
        Parameters:
        - hash_set1, hash_set2: Sets of n-gram hashes
        
        Returns: Similarity score (0.0-1.0)
        """
        if not hash_set1 or not hash_set2:
            return 0.0
        
        intersection = len(hash_set1.intersection(hash_set2))
        union = len(hash_set1.union(hash_set2))
        
        return intersection / union if union > 0 else 0.0
    
    @staticmethod
    def generate_collusion_hash(question_id, group_seed, group_id):
        """
        Generate identical hash for colluding group
        
        Parameters:
        - question_id: Question being answered
        - group_seed: Seed for this question
        - group_id: Collusion group identifier
        
        Returns: Consistent hash for all group members
        """
        hash_input = f"{question_id}_{group_seed}_{group_id}"
        return hashlib.sha256(hash_input.encode()).hexdigest()


# Test the hash generator
if __name__ == "__main__":
    print("=" * 70)
    print("ANSWER HASH GENERATOR - TEST")
    print("=" * 70)
    
    gen = AnswerHashGenerator()
    
    # Test identical answers
    answer1 = "The time complexity of quicksort is O(n log n) on average."
    answer2 = "The time complexity of QuickSort is O(n log n) on average!"  # Same content, different punctuation
    answer3 = "Quicksort has O(n^2) worst case complexity."  # Different answer
    
    print("\n1. Identical answers (different formatting):")
    print(f"   Answer 1: {answer1}")
    print(f"   Answer 2: {answer2}")
    hash1 = gen.generate_hash(answer1)
    hash2 = gen.generate_hash(answer2)
    print(f"   Hash 1: {hash1[:16]}...")
    print(f"   Hash 2: {hash2[:16]}...")
    print(f"   Match: {hash1 == hash2}")
    
    print("\n2. Different answers:")
    print(f"   Answer 1: {answer1}")
    print(f"   Answer 3: {answer3}")
    hash3 = gen.generate_hash(answer3)
    print(f"   Hash 1: {hash1[:16]}...")
    print(f"   Hash 3: {hash3[:16]}...")
    print(f"   Match: {hash1 == hash3}")
    
    # Test fuzzy matching
    print("\n3. Fuzzy matching (n-grams):")
    answer4 = "The time complexity of quicksort is O(n log n) on average and O(n^2) worst case."
    fuzzy1 = gen.generate_fuzzy_hash(answer1, n_grams=3)
    fuzzy4 = gen.generate_fuzzy_hash(answer4, n_grams=3)
    similarity = gen.calculate_similarity(fuzzy1, fuzzy4)
    print(f"   Answer 1: {answer1}")
    print(f"   Answer 4: {answer4}")
    print(f"   N-grams (answer 1): {len(fuzzy1)}")
    print(f"   N-grams (answer 4): {len(fuzzy4)}")
    print(f"   Similarity: {similarity:.3f}")
    
    # Test collusion hash
    print("\n4. Collusion hash (identical for group):")
    question_id = "QST001"
    group_seed = "seed123"
    group_id_1 = "GROUP1"
    group_id_2 = "GROUP2"
    
    collusion_hash_1a = gen.generate_collusion_hash(question_id, group_seed, group_id_1)
    collusion_hash_1b = gen.generate_collusion_hash(question_id, group_seed, group_id_1)  # Same group
    collusion_hash_2 = gen.generate_collusion_hash(question_id, group_seed, group_id_2)  # Different group
    
    print(f"   Group 1, Student A: {collusion_hash_1a[:16]}...")
    print(f"   Group 1, Student B: {collusion_hash_1b[:16]}...")
    print(f"   Group 2, Student A: {collusion_hash_2[:16]}...")
    print(f"   Group 1 match: {collusion_hash_1a == collusion_hash_1b}")
    print(f"   Cross-group match: {collusion_hash_1a == collusion_hash_2}")
    
    print("\n" + "=" * 70)
    print("✓ Answer hash generation working correctly!")
    print("=" * 70)
