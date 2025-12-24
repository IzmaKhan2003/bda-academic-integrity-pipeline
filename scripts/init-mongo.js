// Switch to academic_integrity database
db = db.getSiblingDB('academic_integrity');

// Create collections with validation
db.createCollection('students', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['student_id', 'name', 'program'],
      properties: {
        student_id: { bsonType: 'string' },
        name: { bsonType: 'string' },
        program: { bsonType: 'string' },
        academic_year: { bsonType: 'int' },
        region: { bsonType: 'string' },
        skill_level: { bsonType: 'double' }
      }
    }
  }
});

db.createCollection('courses');
db.createCollection('exams');
db.createCollection('questions');
db.createCollection('exam_attempts');
db.createCollection('session_logs');

// Create indexes for performance
db.students.createIndex({ student_id: 1 }, { unique: true });
db.courses.createIndex({ course_id: 1 }, { unique: true });
db.exams.createIndex({ exam_id: 1 }, { unique: true });
db.questions.createIndex({ question_id: 1 }, { unique: true });

// Compound indexes for join queries
db.exam_attempts.createIndex({ student_id: 1, exam_id: 1, submission_timestamp: -1 });
db.exam_attempts.createIndex({ exam_id: 1, question_id: 1 });
db.exam_attempts.createIndex({ answer_hash: 1 }); // For collusion detection

db.session_logs.createIndex({ student_id: 1, exam_id: 1 });
db.session_logs.createIndex({ session_start: -1 });

print('MongoDB initialized successfully with collections and indexes');
