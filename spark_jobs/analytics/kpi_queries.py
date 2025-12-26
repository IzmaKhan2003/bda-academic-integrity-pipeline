"""
KPI Calculation Queries
Phase 7 - Step 24: Calculate 7 numerical metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# KPI definitions
KPIS = {
    'avg_response_time': 'Average response time (seconds)',
    'accuracy_rate': 'Percentage of correct answers',
    'response_variance': 'Std dev of response times',
    'ai_usage_score': 'Average AI usage probability',
    'answer_similarity_index': 'Collusion detection ratio',
    'suspicion_confidence': 'Composite risk score',
    'session_irregularity_score': 'Session behavior anomaly score'
}

def calculate_all_kpis(spark, mongo_uri='mongodb://mongodb:27017/'):
    """
    Calculate all 7 KPIs from MongoDB data
    
    Returns:
        Dictionary of DataFrames with KPI results
    """
    
    print("=" * 70)
    print("CALCULATING KPIs")
    print("=" * 70)
    
    # Read from MongoDB
    print("\n1. Loading data from MongoDB...")
    
    df_attempts = spark.read \
        .format("mongo") \
        .option("uri", f"{mongo_uri}academic_integrity.exam_attempts") \
        .load()
    
    df_sessions = spark.read \
        .format("mongo") \
        .option("uri", f"{mongo_uri}academic_integrity.session_logs") \
        .load()
    
    print(f"  exam_attempts: {df_attempts.count():,} records")
    print(f"  session_logs: {df_sessions.count():,} records")
    
    kpi_results = {}
    
    # KPI 1: avg_response_time
    print("\n2. Calculating avg_response_time...")
    kpi_results['avg_response_time'] = df_attempts \
        .groupBy('student_id', 'exam_id') \
        .agg(avg('response_time').alias('avg_response_time')) \
        .cache()
    
    print(f"  ✓ Calculated for {kpi_results['avg_response_time'].count():,} student-exam pairs")
    
    # KPI 2: accuracy_rate
    print("\n3. Calculating accuracy_rate...")
    kpi_results['accuracy_rate'] = df_attempts \
        .groupBy('student_id', 'exam_id') \
        .agg(
            (sum(when(col('is_correct'), 1).otherwise(0)) / count('*') * 100).alias('accuracy_rate')
        ) \
        .cache()
    
    # KPI 3: response_variance
    print("\n4. Calculating response_variance...")
    kpi_results['response_variance'] = df_attempts \
        .groupBy('student_id', 'exam_id') \
        .agg(stddev('response_time').alias('response_variance')) \
        .cache()
    
    # KPI 4: ai_usage_score
    print("\n5. Calculating ai_usage_score...")
    kpi_results['ai_usage_score'] = df_attempts \
        .groupBy('student_id', 'exam_id') \
        .agg(avg('ai_usage_score').alias('avg_ai_usage_score')) \
        .cache()
    
    # KPI 5: answer_similarity_index (collusion detection)
    print("\n6. Calculating answer_similarity_index...")
    # Self-join to find matching hashes
    df_hash_pairs = df_attempts.alias('a1') \
        .join(
            df_attempts.alias('a2'),
            (col('a1.exam_id') == col('a2.exam_id')) &
            (col('a1.question_id') == col('a2.question_id')) &
            (col('a1.answer_hash') == col('a2.answer_hash')) &
            (col('a1.student_id') < col('a2.student_id'))  # Avoid duplicates
        ) \
        .select(
            col('a1.student_id').alias('student_1'),
            col('a2.student_id').alias('student_2'),
            col('a1.exam_id'),
            col('a1.question_id')
        )
    
    kpi_results['answer_similarity_index'] = df_hash_pairs \
        .groupBy('student_1', 'student_2', 'exam_id') \
        .agg(count('question_id').alias('matching_questions')) \
        .cache()
    
    print(f"  ✓ Found {kpi_results['answer_similarity_index'].count():,} suspicious pairs")
    
    # KPI 6: session_irregularity_score
    print("\n7. Calculating session_irregularity_score...")
    kpi_results['session_irregularity_score'] = df_sessions \
        .withColumn(
            'irregularity_score',
            (
                col('tab_switches') * 0.3 +
                col('focus_loss_count') * 0.4 +
                (col('idle_time_seconds') / 7200) * 0.3  # Normalize to 2-hour exam
            )
        ) \
        .select('student_id', 'exam_id', 'irregularity_score') \
        .cache()
    
    # KPI 7: suspicion_confidence (composite score)
    print("\n8. Calculating suspicion_confidence...")
    
    # Join all KPIs
    suspicion_df = kpi_results['ai_usage_score'] \
        .join(kpi_results['response_variance'], ['student_id', 'exam_id'], 'left') \
        .join(kpi_results['session_irregularity_score'], ['student_id', 'exam_id'], 'left') \
        .fillna(0)
    
    # Normalize response_variance (invert: lower variance = higher suspicion)
    max_variance = suspicion_df.agg(max('response_variance')).collect()[0][0] or 100
    
    kpi_results['suspicion_confidence'] = suspicion_df \
        .withColumn(
            'normalized_variance',
            1 - (col('response_variance') / max_variance)
        ) \
        .withColumn(
            'suspicion_score',
            (
                col('avg_ai_usage_score') * 0.4 +
                col('normalized_variance') * 0.3 +
                col('irregularity_score') * 0.3
            )
        ) \
        .select('student_id', 'exam_id', 'suspicion_score') \
        .cache()
    
    print("\n" + "=" * 70)
    print("ALL KPIs CALCULATED")
    print("=" * 70)
    
    # Summary statistics
    print("\nSummary Statistics:")
    for kpi_name, df in kpi_results.items():
        if kpi_name != 'answer_similarity_index':
            count = df.count()
            print(f"  {kpi_name}: {count:,} records")
    
    return kpi_results


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KPI Calculations") \
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/academic_integrity") \
        .getOrCreate()
    
    # Calculate KPIs
    results = calculate_all_kpis(spark)
    
    # Show samples
    print("\n" + "=" * 70)
    print("SAMPLE RESULTS")
    print("=" * 70)
    
    for kpi_name, df in results.items():
        print(f"\n{kpi_name}:")
        df.show(5, truncate=False)
    
    spark.stop()
