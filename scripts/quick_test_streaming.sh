#!/bin/bash

echo "=========================================="
echo "QUICK STREAMING PIPELINE TEST"
echo "=========================================="
echo ""

# Start consumer in background
echo "Starting consumer..."
docker exec -d spark-master python3 /spark/jobs/kafka_to_mongo_consumer_enhanced.py

sleep 5

# Run generator for 2 minutes
echo "Running generator for 2 minutes..."
timeout 120 docker exec spark-master python3 /spark/jobs/generators/realtime_data_generator.py || true

# Check results
echo ""
echo "Results:"
docker exec mongodb mongosh -u admin -p admin123 --eval "
use academic_integrity;
print('exam_attempts: ' + db.exam_attempts.count());
print('session_logs: ' + db.session_logs.count());
" --quiet

echo ""
echo "✓ Test complete!"
