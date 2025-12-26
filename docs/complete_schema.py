"""
Complete Database Schema for Academic Integrity Detection
Phase 3 - Step 9: Normalized schema with dimensions and facts
"""

SCHEMA_DEFINITION = {
    "database": "academic_integrity",
    
    # ============= DIMENSION TABLES =============
    "dimensions": {
        "students": {
            "description": "Student master data",
            "primary_key": "student_id",
            "fields": {
                "student_id": {
                    "type": "string",
                    "description": "Unique student identifier",
                    "example": "STU001234",
                    "constraints": "NOT NULL, UNIQUE"
                },
                "name": {
                    "type": "string",
                    "description": "Full student name",
                    "example": "John Smith"
                },
                "email": {
                    "type": "string",
                    "description": "Student email",
                    "example": "john.smith@university.edu"
                },
                "program": {
                    "type": "string",
                    "description": "Academic program",
                    "example": "Computer Science",
                    "values": ["Computer Science", "Data Science", "Software Engineering", 
                              "Information Systems", "Business Analytics"]
                },
                "academic_year": {
                    "type": "integer",
                    "description": "Year of study",
                    "example": 3,
                    "constraints": "1-5"
                },
                "region": {
                    "type": "string",
                    "description": "Geographic region",
                    "example": "North America",
                    "values": ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]
                },
                "skill_level": {
                    "type": "float",
                    "description": "Academic skill rating (0.0-1.0)",
                    "example": 0.75,
                    "constraints": "0.0 <= skill_level <= 1.0"
                },
                "enrollment_date": {
                    "type": "datetime",
                    "description": "University enrollment date",
                    "example": "2022-09-01T00:00:00Z"
                }
            },
            "indexes": [
                {"fields": ["student_id"], "unique": True},
                {"fields": ["program"]},
                {"fields": ["region"]},
                {"fields": ["academic_year"]}
            ]
        },
        
        "courses": {
            "description": "Course catalog",
            "primary_key": "course_id",
            "fields": {
                "course_id": {
                    "type": "string",
                    "description": "Unique course identifier",
                    "example": "CS501",
                    "constraints": "NOT NULL, UNIQUE"
                },
                "course_name": {
                    "type": "string",
                    "description": "Course title",
                    "example": "Advanced Data Structures"
                },
                "instructor_id": {
                    "type": "string",
                    "description": "Instructor identifier",
                    "example": "INST123"
                },
                "instructor_name": {
                    "type": "string",
                    "description": "Instructor full name",
                    "example": "Dr. Jane Doe"
                },
                "difficulty_level": {
                    "type": "integer",
                    "description": "Course difficulty (1=Easy, 5=Hard)",
                    "example": 4,
                    "constraints": "1-5"
                },
                "department": {
                    "type": "string",
                    "description": "Academic department",
                    "example": "Computer Science",
                    "values": ["Computer Science", "Mathematics", "Engineering", 
                              "Business", "Statistics"]
                },
                "credits": {
                    "type": "integer",
                    "description": "Credit hours",
                    "example": 3
                }
            },
            "indexes": [
                {"fields": ["course_id"], "unique": True},
                {"fields": ["department"]},
                {"fields": ["difficulty_level"]}
            ]
        },
        
        "exams": {
            "description": "Exam metadata",
            "primary_key": "exam_id",
            "foreign_keys": ["course_id"],
            "fields": {
                "exam_id": {
                    "type": "string",
                    "description": "Unique exam identifier",
                    "example": "EXM001",
                    "constraints": "NOT NULL, UNIQUE"
                },
                "course_id": {
                    "type": "string",
                    "description": "Associated course",
                    "example": "CS501",
                    "foreign_key": "courses.course_id"
                },
                "exam_name": {
                    "type": "string",
                    "description": "Exam title",
                    "example": "Midterm Exam - Fall 2024"
                },
                "exam_date": {
                    "type": "datetime",
                    "description": "Scheduled exam date",
                    "example": "2024-10-15T14:00:00Z"
                },
                "duration_minutes": {
                    "type": "integer",
                    "description": "Exam duration in minutes",
                    "example": 120
                },
                "total_marks": {
                    "type": "integer",
                    "description": "Maximum possible score",
                    "example": 100
                },
                "exam_type": {
                    "type": "string",
                    "description": "Type of examination",
                    "example": "midterm",
                    "values": ["midterm", "final", "quiz", "assignment"]
                },
                "proctored": {
                    "type": "boolean",
                    "description": "Whether exam is proctored",
                    "example": True
                }
            },
            "indexes": [
                {"fields": ["exam_id"], "unique": True},
                {"fields": ["course_id"]},
                {"fields": ["exam_date"]},
                {"fields": ["exam_type"]}
            ]
        },
        
        "questions": {
            "description": "Question bank",
            "primary_key": "question_id",
            "foreign_keys": ["exam_id"],
            "fields": {
                "question_id": {
                    "type": "string",
                    "description": "Unique question identifier",
                    "example": "QST0001",
                    "constraints": "NOT NULL, UNIQUE"
                },
                "exam_id": {
                    "type": "string",
                    "description": "Associated exam",
                    "example": "EXM001",
                    "foreign_key": "exams.exam_id"
                },
                "question_number": {
                    "type": "integer",
                    "description": "Question sequence number",
                    "example": 1
                },
                "difficulty_level": {
                    "type": "integer",
                    "description": "Question difficulty (1=Easy, 5=Hard)",
                    "example": 3,
                    "constraints": "1-5"
                },
                "question_type": {
                    "type": "string",
                    "description": "Question format",
                    "example": "MCQ",
                    "values": ["MCQ", "short-answer", "essay", "coding", "true-false"]
                },
                "max_marks": {
                    "type": "integer",
                    "description": "Maximum points for question",
                    "example": 10
                },
                "expected_time_seconds": {
                    "type": "integer",
                    "description": "Expected time to complete",
                    "example": 180
                },
                "topic": {
                    "type": "string",
                    "description": "Question topic/category",
                    "example": "Binary Trees"
                }
            },
            "indexes": [
                {"fields": ["question_id"], "unique": True},
                {"fields": ["exam_id", "question_number"]},
                {"fields": ["difficulty_level"]},
                {"fields": ["question_type"]}
            ]
        }
    },
    
    # ============= FACT TABLES =============
    "facts": {
        "exam_attempts": {
            "description": "Student responses to questions (main fact table)",
            "primary_key": "attempt_id",
            "foreign_keys": ["student_id", "exam_id", "question_id"],
            "fields": {
                "attempt_id": {
                    "type": "string",
                    "description": "Unique attempt identifier",
                    "example": "ATT123456",
                    "constraints": "NOT NULL, UNIQUE"
                },
                "student_id": {
                    "type": "string",
                    "description": "Student who answered",
                    "example": "STU001234",
                    "foreign_key": "students.student_id"
                },
                "exam_id": {
                    "type": "string",
                    "description": "Exam being taken",
                    "example": "EXM001",
                    "foreign_key": "exams.exam_id"
                },
                "question_id": {
                    "type": "string",
                    "description": "Question answered",
                    "example": "QST0001",
                    "foreign_key": "questions.question_id"
                },
                
                # KPI FIELDS (7 numerical facts)
                "response_time": {
                    "type": "float",
                    "description": "Time taken to answer (seconds)",
                    "example": 145.32,
                    "kpi": "avg_response_time"
                },
                "is_correct": {
                    "type": "boolean",
                    "description": "Whether answer is correct",
                    "example": True,
                    "kpi": "accuracy_rate"
                },
                "ai_usage_score": {
                    "type": "float",
                    "description": "AI tool usage probability (0.0-1.0)",
                    "example": 0.85,
                    "constraints": "0.0 <= ai_usage_score <= 1.0",
                    "kpi": "ai_usage_score"
                },
                "keystroke_count": {
                    "type": "integer",
                    "description": "Number of keystrokes",
                    "example": 234,
                    "kpi": "response_variance"
                },
                "paste_count": {
                    "type": "integer",
                    "description": "Number of paste operations",
                    "example": 2,
                    "kpi": "response_variance"
                },
                "answer_hash": {
                    "type": "string",
                    "description": "SHA256 hash of answer (for collusion detection)",
                    "example": "a3f5c9d8e1b2...",
                    "kpi": "answer_similarity_index"
                },
                "answer_text": {
                    "type": "text",
                    "description": "Full answer content",
                    "example": "The time complexity is O(n log n)..."
                },
                "submission_timestamp": {
                    "type": "datetime",
                    "description": "When answer was submitted",
                    "example": "2024-10-15T14:35:22.123Z",
                    "indexed": True
                },
                "marks_obtained": {
                    "type": "integer",
                    "description": "Score received",
                    "example": 8
                },
                "flagged": {
                    "type": "boolean",
                    "description": "Marked as suspicious",
                    "example": False
                }
            },
            "indexes": [
                {"fields": ["attempt_id"], "unique": True},
                {"fields": ["student_id", "exam_id", "submission_timestamp"]},
                {"fields": ["exam_id", "question_id"]},
                {"fields": ["answer_hash"]},
                {"fields": ["submission_timestamp"]},
                {"fields": ["ai_usage_score"]}
            ]
        },
        
        "session_logs": {
            "description": "Student session behavior during exams",
            "primary_key": "log_id",
            "foreign_keys": ["student_id", "exam_id"],
            "fields": {
                "log_id": {
                    "type": "string",
                    "description": "Unique log identifier",
                    "example": "LOG789012",
                    "constraints": "NOT NULL, UNIQUE"
                },
                "student_id": {
                    "type": "string",
                    "description": "Student being monitored",
                    "example": "STU001234",
                    "foreign_key": "students.student_id"
                },
                "exam_id": {
                    "type": "string",
                    "description": "Exam session",
                    "example": "EXM001",
                    "foreign_key": "exams.exam_id"
                },
                
                # BEHAVIORAL METRICS (KPI contributors)
                "tab_switches": {
                    "type": "integer",
                    "description": "Number of browser tab switches",
                    "example": 12,
                    "kpi": "session_irregularity_score"
                },
                "idle_time_seconds": {
                    "type": "float",
                    "description": "Total idle time",
                    "example": 342.5,
                    "kpi": "session_irregularity_score"
                },
                "focus_loss_count": {
                    "type": "integer",
                    "description": "Times window lost focus",
                    "example": 8,
                    "kpi": "session_irregularity_score"
                },
                "copy_count": {
                    "type": "integer",
                    "description": "Copy operations performed",
                    "example": 3
                },
                "screenshot_attempts": {
                    "type": "integer",
                    "description": "Screenshot detection triggers",
                    "example": 0
                },
                "device_type": {
                    "type": "string",
                    "description": "Device category",
                    "example": "Desktop",
                    "values": ["Desktop", "Laptop", "Tablet", "Mobile"]
                },
                "browser": {
                    "type": "string",
                    "description": "Browser used",
                    "example": "Chrome",
                    "values": ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
                },
                "ip_address": {
                    "type": "string",
                    "description": "Client IP address",
                    "example": "192.168.1.100"
                },
                "session_start": {
                    "type": "datetime",
                    "description": "Session start time",
                    "example": "2024-10-15T14:00:00Z"
                },
                "session_end": {
                    "type": "datetime",
                    "description": "Session end time",
                    "example": "2024-10-15T16:00:00Z"
                },
                "warnings_issued": {
                    "type": "integer",
                    "description": "Number of warnings shown",
                    "example": 1
                }
            },
            "indexes": [
                {"fields": ["log_id"], "unique": True},
                {"fields": ["student_id", "exam_id"]},
                {"fields": ["session_start"]},
                {"fields": ["tab_switches"]},
                {"fields": ["focus_loss_count"]}
            ]
        }
    },
    
    # ============= KPI DEFINITIONS =============
    "kpis": {
        "avg_response_time": {
            "description": "Average time to answer questions",
            "calculation": "AVG(exam_attempts.response_time)",
            "unit": "seconds",
            "interpretation": "AI cheaters: 10-30s, Normal: 60-180s"
        },
        "accuracy_rate": {
            "description": "Percentage of correct answers",
            "calculation": "SUM(is_correct) / COUNT(*) * 100",
            "unit": "percentage",
            "interpretation": "AI cheaters: 85-95%, Normal: 50-75%"
        },
        "response_variance": {
            "description": "Standard deviation of response times",
            "calculation": "STDDEV(exam_attempts.response_time)",
            "unit": "seconds",
            "interpretation": "Low variance (<10s) suggests automation"
        },
        "ai_usage_score": {
            "description": "AI tool detection confidence",
            "calculation": "AVG(exam_attempts.ai_usage_score)",
            "unit": "probability (0-1)",
            "interpretation": ">0.7 is suspicious, >0.85 is high risk"
        },
        "answer_similarity_index": {
            "description": "Jaccard similarity between student answers",
            "calculation": "COUNT(matching answer_hash) / TOTAL",
            "unit": "ratio (0-1)",
            "interpretation": ">0.8 similarity suggests collusion"
        },
        "suspicion_confidence": {
            "description": "Composite risk score",
            "calculation": "ai_usage_score * 0.4 + (1 - response_variance) * 0.3 + session_irregularity * 0.3",
            "unit": "score (0-1)",
            "interpretation": "<0.3=Low, 0.3-0.7=Medium, >0.7=High risk"
        },
        "session_irregularity_score": {
            "description": "Session behavior anomaly score",
            "calculation": "tab_switches * 0.3 + focus_loss_count * 0.4 + (idle_time / total_time) * 0.3",
            "unit": "score (0-1)",
            "interpretation": ">0.6 indicates suspicious behavior"
        }
    },
    
    # ============= DIMENSIONS FOR ANALYSIS =============
    "analytical_dimensions": [
        {
            "name": "student_id",
            "description": "Individual student tracking",
            "use_case": "Identify specific cheaters"
        },
        {
            "name": "course_id",
            "description": "Course-level aggregation",
            "use_case": "Compare cheating rates across courses"
        },
        {
            "name": "exam_id",
            "description": "Exam-specific analysis",
            "use_case": "Monitor individual exam integrity"
        },
        {
            "name": "program",
            "description": "Academic program comparison",
            "use_case": "Identify programs with higher risks"
        },
        {
            "name": "region",
            "description": "Geographic patterns",
            "use_case": "Regional cheating trends"
        },
        {
            "name": "difficulty_level",
            "description": "Question/exam difficulty",
            "use_case": "Correlation between difficulty and cheating"
        },
        {
            "name": "time_window",
            "description": "Temporal aggregation",
            "use_case": "Hourly/daily trend analysis",
            "granularity": ["hour", "day", "week"]
        },
        {
            "name": "device_type",
            "description": "Device-based behavior",
            "use_case": "Cheating patterns by device"
        },
        {
            "name": "exam_type",
            "description": "Midterm vs final vs quiz",
            "use_case": "Cheating rates by exam type"
        },
        {
            "name": "question_type",
            "description": "MCQ vs essay behavior",
            "use_case": "AI usage by question format"
        }
    ]
}

# Export schema to JSON for documentation
if __name__ == "__main__":
    import json
    
    with open('docs/complete_schema.json', 'w') as f:
        json.dump(SCHEMA_DEFINITION, f, indent=2)
    
    print("✓ Schema definition exported to docs/complete_schema.json")
    print(f"\nSchema Summary:")
    print(f"  Dimension Tables: {len(SCHEMA_DEFINITION['dimensions'])}")
    print(f"  Fact Tables: {len(SCHEMA_DEFINITION['facts'])}")
    print(f"  KPIs Defined: {len(SCHEMA_DEFINITION['kpis'])}")
    print(f"  Analytical Dimensions: {len(SCHEMA_DEFINITION['analytical_dimensions'])}")

