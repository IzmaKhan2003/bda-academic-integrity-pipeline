# 📊 Superset Dashboard Setup Guide
## Phase 11 - Steps 44-49

Since Superset doesn't have a complete REST API for creating dashboards programmatically, follow these manual steps:

---

## PREREQUISITE: Create Datasets

### 1. Create `exam_attempts` Dataset

**Steps:**
1. Go to **Data → Datasets**
2. Click **+ Dataset**
3. Select:
   - **Database:** MongoDB - Academic Integrity
   - **Schema:** (leave blank)
   - **Table:** `exam_attempts`
4. Click **Add**
5. Click on the dataset name to configure
6. Under **Columns**, ensure these are recognized:
   - `student_id` (String)
   - `exam_id` (String)
   - `ai_usage_score` (Number)
   - `submission_timestamp` (DateTime)
   - `is_correct` (Boolean)
   - `response_time` (Number)

### 2. Create `session_logs` Dataset

Repeat above steps for `session_logs` collection.

---

## DASHBOARD 1: Academic Integrity Risk Heatmap

### Create the Chart

1. Go to **Charts → + Chart**
2. Select:
   - **Dataset:** exam_attempts
   - **Chart Type:** Heatmap
3. Click **Create New Chart**

### Configure Chart

**Query Settings:**
- **Metrics:**
  - Add metric: `COUNT(DISTINCT student_id)`
  - Name it: `student_count`

- **Rows:**
  - Add: `exam_id` (or create a calculated column for `course_name`)

- **Columns:**
  - Add calculated column: `risk_level`
  - SQL: 
```sql
    CASE 
      WHEN ai_usage_score < 0.3 THEN 'Low Risk'
      WHEN ai_usage_score < 0.7 THEN 'Medium Risk'
      ELSE 'High Risk'
    END
```

**Customize:**
- **Color Scheme:** Red-Yellow-Green (diverging)
- **Title:** Academic Integrity Risk Heatmap
- **Normalize:** By row

**Filters:**
- **Time Range:** Last 2 hours
- Add filter: `submission_timestamp` >= NOW() - INTERVAL 2 HOUR

Click **Save** and add to new dashboard: `Academic Integrity Detection`

---

## DASHBOARD 2: AI Usage Trend Analysis

### Create the Chart

1. **Charts → + Chart**
2. **Dataset:** exam_attempts
3. **Chart Type:** Time-series Line Chart

### Configure Chart

**Query Settings:**
- **Time Column:** `submission_timestamp`
- **Time Grain:** 10 minutes
- **Metrics:**
  - `AVG(ai_usage_score)` → Name: `avg_ai_usage`

- **Group By (Series):** `exam_id` (limit to top 5)

**Customize:**
- **Title:** AI Usage Trends (Last 2 Hours)
- **Y-Axis:** 0 to 1.0
- **Show Legend:** Yes
- **Line Style:** Smooth

**Filters:**
- **Time Range:** Last 2 hours

Click **Save** and add to dashboard: `Academic Integrity Detection`

---

## DASHBOARD 3: Suspicious Student Leaderboard

### Create the Chart

1. **Charts → + Chart**
2. **Dataset:** exam_attempts
3. **Chart Type:** Bar Chart

### Configure Chart

**Query Settings:**
- **Metrics:**
  - Add calculated metric: `suspicion_score`
  - SQL Expression:
```sql
    AVG(ai_usage_score) * 0.6 + 
    (COUNT(DISTINCT CASE WHEN is_correct = true THEN 1 END) / COUNT(*)) * 0.4
```

- **Dimension:** `student_id`

- **Row Limit:** 20

- **Sort By:** `suspicion_score` DESC

**Customize:**
- **Title:** Top 20 Suspicious Students
- **Orientation:** Horizontal
- **Color Scheme:** Red gradient
- **Show Values:** Yes

**Filters:**
- **Time Range:** Last 2 hours
- Add filter: `suspicion_score` > 0.5

Click **Save** and add to dashboard: `Academic Integrity Detection`

---

## DASHBOARD 4: Collusion Detection Summary

### Create Table Chart (Network graph requires plugin)

1. **Charts → + Chart**
2. **Dataset:** exam_attempts
3. **Chart Type:** Table

### Configure Chart

**Query Settings:**
- **Columns:**
  - `student_id` (rename to Student 1)
  - Add calculated column for matching pairs

- **Metrics:**
  - `COUNT(DISTINCT answer_hash)` → `matching_answers`

- **Group By:** `answer_hash`

- **Having Clause:**
```sql
  COUNT(DISTINCT student_id) >= 2
```

**Customize:**
- **Title:** Potential Collusion Cases
- **Show Filters:** Yes
- **Page Size:** 20

**Filters:**
- **Time Range:** Last 2 hours

Click **Save** and add to dashboard: `Academic Integrity Detection`

---

## STEP 48: Configure Dashboard Auto-Refresh

### Enable Auto-Refresh for Dashboard

1. Open dashboard: `Academic Integrity Detection`
2. Click **Edit Dashboard** (pencil icon)
3. Click **⚙ Settings**
4. Under **JSON Metadata**, add:
```json
   {
     "refresh_frequency": 60,
     "default_filters": {},
     "color_scheme": "supersetColors"
   }
```
5. Click **Save**

### Enable Auto-Refresh for Individual Charts

For each chart:
1. Click **⋮** (three dots) → **Edit Chart**
2. In **Advanced** tab, find **Cache Timeout**
3. Set to: `60` seconds
4. Click **Save**

---

## STEP 49: Optimize Dashboard Performance

### Query Performance Tips

1. **Use Time Range Filters:**
   - Always filter to last 2-4 hours
   - Reduces data scanned

2. **Enable Query Caching:**
   - Superset → Data → Databases
   - Edit MongoDB connection
   - Enable "Cache timeout" = 60 seconds

3. **Pre-aggregate Data:**
   - Create materialized views in MongoDB
   - Or use Redis cache for common queries

4. **Limit Result Sets:**
   - Use `LIMIT 100` in SQL queries
   - Set row limits on charts

---

## VERIFICATION: Test Dashboard Updates

### Method 1: Manual Refresh

1. Open dashboard
2. Click **↻ Refresh** button
3. Observe data changes
4. Check timestamp updates

### Method 2: Auto-Refresh

1. Wait 60 seconds
2. Observe charts updating automatically
3. Check browser console for refresh logs

### Method 3: Generate New Data
```bash
# Trigger data generator
docker exec spark-master timeout 60 python3 /spark/jobs/realtime_data_generator.py

# Wait 60 seconds
# Refresh dashboard - numbers should change
```

---

## TROUBLESHOOTING

### Dashboard Not Auto-Refreshing

**Solution 1:** Check browser console
- Open DevTools (F12)
- Look for errors in Console tab
- Check Network tab for API calls

**Solution 2:** Manually refresh
- Click ↻ button on dashboard
- If data updates, auto-refresh setting is the issue

**Solution 3:** Clear browser cache
```bash
# In browser
Ctrl + Shift + Delete → Clear cache
```

### Charts Show "No Data"

**Solution:** Check MongoDB connection
```bash
# Test MongoDB from Superset container
docker exec superset python3 << 'PYEND'
from pymongo import MongoClient
client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
db = client['academic_integrity']
print(f"exam_attempts count: {db.exam_attempts.count_documents({})}")
PYEND
```

### Slow Query Performance

**Solution:** Add indexes in MongoDB
```bash
docker exec mongodb mongosh -u admin -p admin123 << 'MONGOEND'
use academic_integrity;
db.exam_attempts.createIndex({submission_timestamp: -1});
db.exam_attempts.createIndex({ai_usage_score: -1});
db.exam_attempts.createIndex({student_id: 1, exam_id: 1});
MONGOEND
```

---

## FINAL DASHBOARD SCREENSHOTS

**Take screenshots of:**
1. Full dashboard view (all 4 charts)
2. Risk heatmap with live data
3. AI usage trend line chart
4. Suspicious students leaderboard
5. Dashboard settings showing auto-refresh

**Save to:** `docs/screenshots/week3/`

---

**END OF DASHBOARD SETUP GUIDE**
