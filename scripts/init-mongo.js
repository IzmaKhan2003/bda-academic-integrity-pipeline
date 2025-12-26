#!/usr/bin/env mongo

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

