#!/bin/bash

while true; do
    clear
    echo "=========================================="
    echo "MONGODB SIZE MONITOR"
    echo "=========================================="
    echo ""
    date
    echo ""
    
    docker exec mongodb mongosh -u admin -p admin123 --eval "
    use academic_integrity;
    var stats = db.stats();
    print('Total Size: ' + (stats.dataSize / (1024*1024)).toFixed(2) + ' MB');
    print('');
    print('Collections:');
    print('  students: ' + db.students.count());
    print('  courses: ' + db.courses.count());
    print('  exams: ' + db.exams.count());
    print('  questions: ' + db.questions.count());
    print('  exam_attempts: ' + db.exam_attempts.count());
    print('  session_logs: ' + db.session_logs.count());
    " --quiet
    
    sleep 10
done
