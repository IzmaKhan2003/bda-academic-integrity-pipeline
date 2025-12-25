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
