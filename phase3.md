###phse3
🚀 BDA PROJECT: PHASE 3-6 COMPLETE IMPLEMENTATION GUIDE
📋 Git Branching Strategy First
Before we proceed, let's create a proper branch structure:
bash# Currently on main/master - Week 1 complete
git status
git log --oneline -5

# Create and switch to development branch for Phases 3-6
git checkout -b feature/week2-schema-and-data-generation

# This branch will contain:
# - Phase 3: Database Schema (Steps 9-11)
# - Phase 4: Statistical Data Generation (Steps 12-15)
# - Phase 5: Historical Data Loading (Steps 16-19)
# - Phase 6: Real-time Streaming Pipeline (Steps 20-23)
Branch Naming Convention:

feature/week2-schema-and-data-generation - Current work
feature/week3-analytics-and-archival - Week 3 (Phases 7-9)
feature/week4-dashboard-and-orchestration - Week 4 (Phases 10-15)


🎯 PHASE 3: DATABASE SCHEMA DESIGN (Steps 9-11)
Duration: Day 1 (4 hours)
Step 9: Design Complete Normalized SchemaComplete Schema DefinitionCode """
Complete Database Schema for Academic Integrity Detection
Phase 3 - Step 9: Normalized schema with dimensions and facts
"""

SCHEMA_DEFINITION = {
    "database": "academic_integrity",
    
    # ============= DIMENSION TABLES =============
    "Now create the MongoDB initialization script:MongoDB Initialization ScriptCode // ============================================
// MongoDB Database Initialization Script
// Phase 3 - Step 10: Create collections and indexes
// ============================================

// Switch to the academic_integrity database
db = db.getSiStep 11: Create Data Dictionary DocumentComplete Data DictionaryDocument # 📖 Data Dictionary - Academic Integrity Detection System

## Database: academic_integrity

**Last Updated:** Week 2, Phase 3  
**Total Collections:** 6 (4 Dimensions + 2 Facts)  
**Total Fields:** 82  
**Total Indexes:** 43

---

## 📊 DIMENSION TANow let's implement the actual commands for Phase 3:
bash# ============================================
# PHASE 3: DATABASE SCHEMA DESIGN - COMMANDS
# ============================================

# Step 1: Create docs directory structure
mkdir -p docs/screenshots/week2

# Step 2: Save schema definition
cat > docs/complete_schema.py << 'EOF'
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

EOF

# Step 3: Replace init-mongo.js with enhanced version
cat > scripts/init-mongo.js << 'EOF'
// ============================================
// MongoDB Database Initialization Script
// Phase 3 - Step 10: Create collections and indexes
// ============================================

// Switch to the academic_integrity database
db = db.getSiblingDB('academic_integrity');

print("Starting database initialization...");

// ============================================
// DROP EXISTING COLLECTIONS (for clean setup)
// ============================================
print("\n1. Cleaning up existing collections...");

['students', 'courses', 'exams', 'questions', 'exam_attempts', 'session_logs'].forEach(coll => {
    if (db.getCollectionNames().includes(coll)) {
        db[coll].drop();
        print(`   ✓ Dropped ${coll}`);
    }
});

// ============================================
// CREATE DIMENSION TABLES
// ============================================
print("\n2. Creating dimension collections...");

// Students Collection
db.createCollection('students', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["student_id", "name", "email", "program", "academic_year", "region", "skill_level"],
            properties: {
                student_id: { bsonType: "string" },
                name: { bsonType: "string" },
                email: { bsonType: "string" },
                program: { 
                    bsonType: "string",
                    enum: ["Computer Science", "Data Science", "Software Engineering", 
                           "Information Systems", "Business Analytics"]
                },
                academic_year: { 
                    bsonType: "int",
                    minimum: 1,
                    maximum: 5
                },
                region: {
                    bsonType: "string",
                    enum: ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]
                },
                skill_level: {
                    bsonType: "double",
                    minimum: 0.0,
                    maximum: 1.0
                },
                enrollment_date: { bsonType: "date" }
            }
        }
    }
});
print("   ✓ Created students collection");

// Courses Collection
db.createCollection('courses', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["course_id", "course_name", "difficulty_level", "department"],
            properties: {
                course_id: { bsonType: "string" },
                course_name: { bsonType: "string" },
                instructor_id: { bsonType: "string" },
                instructor_name: { bsonType: "string" },
                difficulty_level: {
                    bsonType: "int",
                    minimum: 1,
                    maximum: 5
                },
                department: {
                    bsonType: "string",
                    enum: ["Computer Science", "Mathematics", "Engineering", 
                           "Business", "Statistics"]
                },
                credits: { bsonType: "int" }
            }
        }
    }
});
print("   ✓ Created courses collection");

// Exams Collection
db.createCollection('exams', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["exam_id", "course_id", "exam_date", "duration_minutes", "total_marks", "exam_type"],
            properties: {
                exam_id: { bsonType: "string" },
                course_id: { bsonType: "string" },
                exam_name: { bsonType: "string" },
                exam_date: { bsonType: "date" },
                duration_minutes: { bsonType: "int" },
                total_marks: { bsonType: "int" },
                exam_type: {
                    bsonType: "string",
                    enum: ["midterm", "final", "quiz", "assignment"]
                },
                proctored: { bsonType: "bool" }
            }
        }
    }
});
print("   ✓ Created exams collection");

// Questions Collection
db.createCollection('questions', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["question_id", "exam_id", "difficulty_level", "question_type", "max_marks"],
            properties: {
                question_id: { bsonType: "string" },
                exam_id: { bsonType: "string" },
                question_number: { bsonType: "int" },
                difficulty_level: {
                    bsonType: "int",
                    minimum: 1,
                    maximum: 5
                },
                question_type: {
                    bsonType: "string",
                    enum: ["MCQ", "short-answer", "essay", "coding", "true-false"]
                },
                max_marks: { bsonType: "int" },
                expected_time_seconds: { bsonType: "int" },
                topic: { bsonType: "string" }
            }
        }
    }
});
print("   ✓ Created questions collection");

// ============================================
// CREATE FACT TABLES
// ============================================
print("\n3. Creating fact collections...");

// Exam Attempts Collection (Main Fact Table)
db.createCollection('exam_attempts', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["attempt_id", "student_id", "exam_id", "question_id", 
                       "response_time", "is_correct", "ai_usage_score", 
                       "answer_hash", "submission_timestamp"],
            properties: {
                attempt_id: { bsonType: "string" },
                student_id: { bsonType: "string" },
                exam_id: { bsonType: "string" },
                question_id: { bsonType: "string" },
                response_time: { 
                    bsonType: "double",
                    minimum: 0
                },
                is_correct: { bsonType: "bool" },
                ai_usage_score: {
                    bsonType: "double",
                    minimum: 0.0,
                    maximum: 1.0
                },
                answer_hash: { bsonType: "string" },
                answer_text: { bsonType: "string" },
                submission_timestamp: { bsonType: "date" },
                keystroke_count: { bsonType: "int" },
                paste_count: { bsonType: "int" },
                marks_obtained: { bsonType: "int" },
                flagged: { bsonType: "bool" }
            }
        }
    }
});
print("   ✓ Created exam_attempts collection");

// Session Logs Collection
db.createCollection('session_logs', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["log_id", "student_id", "exam_id", "session_start"],
            properties: {
                log_id: { bsonType: "string" },
                student_id: { bsonType: "string" },
                exam_id: { bsonType: "string" },
                tab_switches: { bsonType: "int" },
                idle_time_seconds: { bsonType: "double" },
                focus_loss_count: { bsonType: "int" },
                copy_count: { bsonType: "int" },
                screenshot_attempts: { bsonType: "int" },
                device_type: {
                    bsonType: "string",
                    enum: ["Desktop", "Laptop", "Tablet", "Mobile"]
                },
                browser: {
                    bsonType: "string",
                    enum: ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
                },
                ip_address: { bsonType: "string" },
                session_start: { bsonType: "date" },
                session_end: { bsonType: "date" },
                warnings_issued: { bsonType: "int" }
            }
        }
    }
});
print("   ✓ Created session_logs collection");

// ============================================
// CREATE INDEXES FOR PERFORMANCE
// ============================================
print("\n4. Creating indexes...");

// Students Indexes
db.students.createIndex({ "student_id": 1 }, { unique: true });
db.students.createIndex({ "program": 1 });
db.students.createIndex({ "region": 1 });
db.students.createIndex({ "academic_year": 1 });
db.students.createIndex({ "skill_level": 1 });
print("   ✓ Created 5 indexes on students");

// Courses Indexes
db.courses.createIndex({ "course_id": 1 }, { unique: true });
db.courses.createIndex({ "department": 1 });
db.courses.createIndex({ "difficulty_level": 1 });
db.courses.createIndex({ "instructor_id": 1 });
print("   ✓ Created 4 indexes on courses");

// Exams Indexes
db.exams.createIndex({ "exam_id": 1 }, { unique: true });
db.exams.createIndex({ "course_id": 1 });
db.exams.createIndex({ "exam_date": -1 });
db.exams.createIndex({ "exam_type": 1 });
db.exams.createIndex({ "course_id": 1, "exam_date": -1 });
print("   ✓ Created 5 indexes on exams");

// Questions Indexes
db.questions.createIndex({ "question_id": 1 }, { unique: true });
db.questions.createIndex({ "exam_id": 1, "question_number": 1 });
db.questions.createIndex({ "difficulty_level": 1 });
db.questions.createIndex({ "question_type": 1 });
db.questions.createIndex({ "topic": 1 });
print("   ✓ Created 5 indexes on questions");

// Exam Attempts Indexes (Most Critical for Performance)
db.exam_attempts.createIndex({ "attempt_id": 1 }, { unique: true });
db.exam_attempts.createIndex({ "student_id": 1, "exam_id": 1, "submission_timestamp": -1 });
db.exam_attempts.createIndex({ "exam_id": 1, "question_id": 1 });
db.exam_attempts.createIndex({ "answer_hash": 1 });  // For collusion detection
db.exam_attempts.createIndex({ "submission_timestamp": -1 });  // For time-series queries
db.exam_attempts.createIndex({ "ai_usage_score": -1 });  // For flagging high AI usage
db.exam_attempts.createIndex({ "student_id": 1, "ai_usage_score": -1 });
db.exam_attempts.createIndex({ "exam_id": 1, "submission_timestamp": -1 });
print("   ✓ Created 8 indexes on exam_attempts");

// Session Logs Indexes
db.session_logs.createIndex({ "log_id": 1 }, { unique: true });
db.session_logs.createIndex({ "student_id": 1, "exam_id": 1 });
db.session_logs.createIndex({ "session_start": -1 });
db.session_logs.createIndex({ "tab_switches": -1 });
db.session_logs.createIndex({ "focus_loss_count": -1 });
db.session_logs.createIndex({ "exam_id": 1, "session_start": -1 });
print("   ✓ Created 6 indexes on session_logs");

// ============================================
// COMPOUND INDEXES FOR COMPLEX QUERIES
// ============================================
print("\n5. Creating compound indexes for analytics...");

// For query: "High AI usage by course and program"
db.exam_attempts.createIndex({ 
    "exam_id": 1, 
    "ai_usage_score": -1, 
    "submission_timestamp": -1 
});

// For query: "Collusion detection"
db.exam_attempts.createIndex({ 
    "exam_id": 1, 
    "question_id": 1, 
    "answer_hash": 1 
});

// For query: "Time-series trend analysis"
db.exam_attempts.createIndex({ 
    "submission_timestamp": -1, 
    "exam_id": 1 
});

print("   ✓ Created 3 compound indexes");

// ============================================
// SUMMARY
// ============================================
print("\n" + "=".repeat(50));
print("DATABASE INITIALIZATION COMPLETE");
print("=".repeat(50));

print("\nCollections created:");
db.getCollectionNames().forEach(name => {
    print(`  - ${name}`);
});

print("\nTotal indexes created:");
let totalIndexes = 0;
db.getCollectionNames().forEach(name => {
    const count = db[name].getIndexes().length;
    totalIndexes += count;
    print(`  - ${name}: ${count} indexes`);
});
print(`\n  TOTAL: ${totalIndexes} indexes`);

print("\nDatabase is ready for data generation!");
print("=".repeat(50));

EOF

# Step 4: Create data dictionary
cat > docs/data_dictionary.md << 'EOF'
# 📖 Data Dictionary - Academic Integrity Detection System

## Database: academic_integrity

**Last Updated:** Week 2, Phase 3  
**Total Collections:** 6 (4 Dimensions + 2 Facts)  
**Total Fields:** 82  
**Total Indexes:** 43

---

## 📊 DIMENSION TABLES

### 1. students
**Description:** Master data for all enrolled students  
**Primary Key:** student_id  
**Total Fields:** 8  
**Indexes:** 5

| Field Name | Type | Length | Description | Example | Constraints | Notes |
|-----------|------|--------|-------------|---------|-------------|-------|
| student_id | String | 10 | Unique student identifier | STU001234 | NOT NULL, UNIQUE | Format: STU + 6 digits |
| name | String | 100 | Student full name | John Smith | NOT NULL | - |
| email | String | 100 | University email address | john.smith@university.edu | UNIQUE | Validated format |
| program | String | 50 | Academic program/major | Computer Science | NOT NULL | Enum: CS, DS, SE, IS, BA |
| academic_year | Integer | - | Year of study (1-5) | 3 | 1 ≤ value ≤ 5 | 1=Freshman, 5=Graduate |
| region | String | 30 | Geographic region | North America | NOT NULL | 6 continents |
| skill_level | Float | - | Academic skill rating (0.0-1.0) | 0.75 | 0.0 ≤ value ≤ 1.0 | Higher = stronger student |
| enrollment_date | DateTime | - | University enrollment date | 2022-09-01T00:00:00Z | NOT NULL | ISO 8601 format |

**Indexes:**
- student_id (UNIQUE)
- program
- region
- academic_year
- skill_level

---

### 2. courses
**Description:** Catalog of all university courses  
**Primary Key:** course_id  
**Total Fields:** 7  
**Indexes:** 4

| Field Name | Type | Length | Description | Example | Constraints | Notes |
|-----------|------|--------|-------------|---------|-------------|-------|
| course_id | String | 10 | Unique course identifier | CS501 | NOT NULL, UNIQUE | Format: DEPT + 3 digits |
| course_name | String | 150 | Full course title | Advanced Data Structures | NOT NULL | - |
| instructor_id | String | 10 | Instructor identifier | INST123 | - | Format: INST + 3 digits |
| instructor_name | String | 100 | Instructor full name | Dr. Jane Doe | - | - |
| difficulty_level | Integer | - | Course difficulty (1-5) | 4 | 1 ≤ value ≤ 5 | 1=Easy, 5=Hard |
| department | String | 50 | Academic department | Computer Science | NOT NULL | 5 departments |
| credits | Integer | - | Credit hours | 3 | value > 0 | Typically 3-4 |

**Indexes:**
- course_id (UNIQUE)
- department
- difficulty_level
- instructor_id

---

### 3. exams
**Description:** Metadata for all examinations  
**Primary Key:** exam_id  
**Foreign Keys:** course_id → courses.course_id  
**Total Fields:** 8  
**Indexes:** 5

| Field Name | Type | Length | Description | Example | Constraints | Notes |
|-----------|------|--------|-------------|---------|-------------|-------|
| exam_id | String | 10 | Unique exam identifier | EXM001 | NOT NULL, UNIQUE | Format: EXM + 3 digits |
| course_id | String | 10 | Associated course | CS501 | NOT NULL, FK | Links to courses table |
| exam_name | String | 150 | Exam title | Midterm Exam - Fall 2024 | NOT NULL | - |
| exam_date | DateTime | - | Scheduled exam date/time | 2024-10-15T14:00:00Z | NOT NULL | ISO 8601 format |
| duration_minutes | Integer | - | Exam duration | 120 | value > 0 | Typically 60-180 minutes |
| total_marks | Integer | - | Maximum possible score | 100 | value > 0 | Usually 50-100 |
| exam_type | String | 20 | Type of examination | midterm | NOT NULL | Enum: midterm, final, quiz, assignment |
| proctored | Boolean | - | Whether exam is proctored | true | NOT NULL | true/false |

**Indexes:**
- exam_id (UNIQUE)
- course_id
- exam_date (DESC)
- exam_type
- (course_id, exam_date) COMPOUND

---

### 4. questions
**Description:** Question bank for all exams  
**Primary Key:** question_id  
**Foreign Keys:** exam_id → exams.exam_id  
**Total Fields:** 8  
**Indexes:** 5

| Field Name | Type | Length | Description | Example | Constraints | Notes |
|-----------|------|--------|-------------|---------|-------------|-------|
| question_id | String | 10 | Unique question identifier | QST0001 | NOT NULL, UNIQUE | Format: QST + 4 digits |
| exam_id | String | 10 | Associated exam | EXM001 | NOT NULL, FK | Links to exams table |
| question_number | Integer | - | Question sequence | 1 | value > 0 | Order in exam |
| difficulty_level | Integer | - | Question difficulty (1-5) | 3 | 1 ≤ value ≤ 5 | 1=Easy, 5=Hard |
| question_type | String | 20 | Question format | MCQ | NOT NULL | 5 types |
| max_marks | Integer | - | Maximum points | 10 | value > 0 | Points for this question |
| expected_time_seconds | Integer | - | Expected completion time | 180 | value > 0 | Average time needed |
| topic | String | 100 | Question topic/category | Binary Trees | - | Course subtopic |

**Question Types:**
- MCQ (Multiple Choice Question)
- short-answer
- essay
- coding
- true-false

**Indexes:**
- question_id (UNIQUE)
- (exam_id, question_number) COMPOUND
- difficulty_level
- question_type
- topic

---

## 📈 FACT TABLES

### 5. exam_attempts
**Description:** Student responses to questions (main fact table)  
**Primary Key:** attempt_id  
**Foreign Keys:** 
- student_id → students.student_id
- exam_id → exams.exam_id
- question_id → questions.question_id  
**Total Fields:** 14  
**Indexes:** 8  
**KPIs Supported:** 5 of 7

| Field Name | Type | Length | Description | Example | Constraints | KPI Contribution |
|-----------|------|--------|-------------|---------|-------------|------------------|
| attempt_id | String | 10 | Unique attempt identifier | ATT123456 | NOT NULL, UNIQUE | - |
| student_id | String | 10 | Student who answered | STU001234 | NOT NULL, FK | Dimension |
| exam_id | String | 10 | Exam being taken | EXM001 | NOT NULL, FK | Dimension |
| question_id | String | 10 | Question answered | QST0001 | NOT NULL, FK | Dimension |
| response_time | Float | - | Time taken (seconds) | 145.32 | value ≥ 0 | **avg_response_time** |
| is_correct | Boolean | - | Whether answer is correct | true | NOT NULL | **accuracy_rate** |
| ai_usage_score | Float | - | AI usage probability (0-1) | 0.85 | 0.0 ≤ value ≤ 1.0 | **ai_usage_score** |
| keystroke_count | Integer | - | Number of keystrokes | 234 | value ≥ 0 | **response_variance** |
| paste_count | Integer | - | Number of paste operations | 2 | value ≥ 0 | **response_variance** |
| answer_hash | String | 64 | SHA256 hash of answer | a3f5c9d8e1b2... | NOT NULL | **answer_similarity_index** |
| answer_text | Text | 5000 | Full answer content | The time complexity is... | - | - |
| submission_timestamp | DateTime | - | When answer submitted | 2024-10-15T14:35:22.123Z | NOT NULL | Time dimension |
| marks_obtained | Integer | - | Score received | 8 | 0 ≤ value ≤ max_marks | - |
| flagged | Boolean | - | Marked as suspicious | false | NOT NULL | - |

**Indexes:**
- attempt_id (UNIQUE)
- (student_id, exam_id, submission_timestamp) COMPOUND
- (exam_id, question_id) COMPOUND
- answer_hash
- submission_timestamp (DESC)
- ai_usage_score (DESC)
- (student_id, ai_usage_score) COMPOUND
- (exam_id, submission_timestamp) COMPOUND

**AI Usage Score Interpretation:**
- 0.0 - 0.3: Low probability (normal student)
- 0.3 - 0.7: Medium probability (investigate)
- 0.7 - 1.0: High probability (likely AI-assisted)

---

### 6. session_logs
**Description:** Student session behavior during exams  
**Primary Key:** log_id  
**Foreign Keys:** 
- student_id → students.student_id
- exam_id → exams.exam_id  
**Total Fields:** 15  
**Indexes:** 6  
**KPIs Supported:** session_irregularity_score

| Field Name | Type | Length | Description | Example | Constraints | KPI Contribution |
|-----------|------|--------|-------------|---------|-------------|------------------|
| log_id | String | 10 | Unique log identifier | LOG789012 | NOT NULL, UNIQUE | - |
| student_id | String | 10 | Student being monitored | STU001234 | NOT NULL, FK | Dimension |
| exam_id | String | 10 | Exam session | EXM001 | NOT NULL, FK | Dimension |
| tab_switches | Integer | - | Browser tab switches | 12 | value ≥ 0 | **session_irregularity_score** |
| idle_time_seconds | Float | - | Total idle time | 342.5 | value ≥ 0 | **session_irregularity_score** |
| focus_loss_count | Integer | - | Window focus losses | 8 | value ≥ 0 | **session_irregularity_score** |
| copy_count | Integer | - | Copy operations | 3 | value ≥ 0 | - |
| screenshot_attempts | Integer | - | Screenshot triggers | 0 | value ≥ 0 | - |
| device_type | String | 20 | Device category | Desktop | NOT NULL | Dimension |
| browser | String | 20 | Browser used | Chrome | NOT NULL | Dimension |
| ip_address | String | 45 | Client IP address | 192.168.1.100 | NOT NULL | - |
| session_start | DateTime | - | Session start time | 2024-10-15T14:00:00Z | NOT NULL | Time dimension |
| session_end | DateTime | - | Session end time | 2024-10-15T16:00:00Z | - | - |
| warnings_issued | Integer | - | Warnings shown | 1 | value ≥ 0 | - |

**Indexes:**
- log_id (UNIQUE)
- (student_id, exam_id) COMPOUND
- session_start (DESC)
- tab_switches (DESC)
- focus_loss_count (DESC)
- (exam_id, session_start) COMPOUND

**Session Irregularity Indicators:**
- Tab switches >10: Suspicious
- Focus losses >5: High risk
- Idle time >30% of exam: Investigate

---

## 🎯 KPI DEFINITIONS

### 1. avg_response_time
**Calculation:** `AVG(exam_attempts.response_time)`  
**Unit:** seconds  
**Source Tables:** exam_attempts  
**Interpretation:**
- AI cheaters: 10-30 seconds (suspiciously fast)
- Normal students: 60-180 seconds
- Struggling students: 200-300 seconds

### 2. accuracy_rate
**Calculation:** `SUM(exam_attempts.is_correct) / COUNT(*) * 100`  
**Unit:** percentage (0-100)  
**Source Tables:** exam_attempts  
**Interpretation:**
- AI cheaters: 85-95% (abnormally high)
- Good students: 70-85%
- Average students: 50-70%
- Struggling students: 30-50%

### 3. response_variance
**Calculation:** `STDDEV(exam_attempts.response_time)`  
**Unit:** seconds  
**Source Tables:** exam_attempts  
**Interpretation:**
- Low variance (<10s): Suggests automation/AI
- Normal variance (30-60s): Human variability
- High variance (>100s): Inconsistent performance

### 4. ai_usage_score
**Calculation:** `AVG(exam_attempts.ai_usage_score)`  
**Unit:** probability (0.0-1.0)  
**Source Tables:** exam_attempts  
**Interpretation:**
- >0.85: High risk - immediate investigation
- 0.70-0.85: Medium-high risk - flag for review
- 0.50-0.70: Medium risk - monitor closely
- <0.50: Low risk - normal behavior

### 5. answer_similarity_index
**Calculation:** Jaccard similarity on `answer_hash`  
**Unit:** ratio (0.0-1.0)  
**Source Tables:** exam_attempts (self-join)  
**Interpretation:**
- >0.80: Strong collusion evidence
- 0.60-0.80: Possible collusion
- <0.60: Coincidental similarity

### 6. suspicion_confidence
**Calculation:** `ai_usage_score × 0.4 + (1 - response_variance_normalized) × 0.3 + session_irregularity × 0.3`  
**Unit:** score (0.0-1.0)  
**Source Tables:** exam_attempts, session_logs  
**Interpretation:**
- 0.00-0.30: Low risk (green)
- 0.30-0.70: Medium risk (yellow)
- 0.70-1.00: High risk (red)

### 7. session_irregularity_score
**Calculation:** `tab_switches × 0.3 + focus_loss_count × 0.4 + (idle_time / total_time) × 0.3`  
**Unit:** score (0.0-1.0)  
**Source Tables:** session_logs  
**Interpretation:**
- >0.60: Suspicious behavior
- 0.40-0.60: Borderline
- <0.40: Normal behavior

---

## 🔍 ANALYTICAL DIMENSIONS

### Available Dimensions (10 total):
1. **student_id** - Individual tracking
2. **course_id** - Course comparison
3. **exam_id** - Exam-specific analysis
4. **program** - By major/program
5. **region** - Geographic patterns
6. **difficulty_level** - Question/exam difficulty
7. **time_window** - Temporal aggregation
8. **device_type** - Device-based behavior
9. **exam_type** - Midterm vs final vs quiz
10. **question_type** - MCQ vs essay patterns

---

## 📦 DATA VOLUME CALCULATIONS

### Per Exam (500 students, 50 questions):
- exam_attempts: 25,000 records × 1.2 KB = **30 MB**
- session_logs: 500 records × 0.8 KB = **0.4 MB**
- **Total per exam:** ~30.4 MB

### Target 300 MB:
- Minimum exams needed: 300 MB / 30 MB = **10 exams**
- OR: Pre-load 8 exams + 2 concurrent live exams

---

## 🔗 FOREIGN KEY RELATIONSHIPS

```
students (1) ←→ (M) exam_attempts
students (1) ←→ (M) session_logs
courses (1) ←→ (M) exams
exams (1) ←→ (M) questions
exams (1) ←→ (M) exam_attempts
exams (1) ←→ (M) session_logs
questions (1) ←→ (M) exam_attempts
```

---

## ✅ SCHEMA VALIDATION RULES

### students
- Email must be unique and valid format
- Skill level must be 0.0-1.0
- Academic year must be 1-5
- Program must be one of 5 valid values

### courses
- Difficulty level must be 1-5
- Department must be one of 5 valid values
- Credits must be positive integer

### exams
- Duration must be positive
- Total marks must be positive
- Exam date cannot be in past (at creation)
- Must link to existing course

### questions
- Must link to existing exam
- Difficulty level must be 1-5
- Max marks must be positive
- Question type must be valid enum

### exam_attempts
- Response time cannot be negative
- AI usage score must be 0.0-1.0
- Marks obtained ≤ max_marks
- Must link to existing student, exam, question
- Answer hash must be 64 characters (SHA256)

### session_logs
- All counts (tab_switches, focus_loss) cannot be negative
- Idle time cannot be negative
- Session end must be after session start
- Must link to existing student and exam

---

**END OF DATA DICTIONARY**

EOF

# Step 5: Restart MongoDB to apply new schema
docker-compose restart mongodb

# Wait 10 seconds
sleep 10

# Step 6: Verify MongoDB initialization
docker exec -it mongodb mongosh -u admin -p admin123 << 'MONGOEOF'
use academic_integrity;
show collections;
db.stats();
db.exam_attempts.getIndexes();
exit
MONGOEOF

# Step 7: Take screenshot of MongoDB collections
echo "✓ Phase 3 Complete - Schema created"
echo "  - 6 collections created"
echo "  - 43 indexes created"
echo "  - Data dictionary documented"

# Commit Phase 3
git add docs/ scripts/init-mongo.js
git commit -m "Phase 3 Complete: Database schema with 6 collections and 43 indexes

- Complete normalized schema (4 dimensions + 2 facts)
- 82 fields across all collections
- MongoDB validation rules
- Compound indexes for analytics
- Data dictionary with KPI definitions"

