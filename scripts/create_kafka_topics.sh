#!/bin/bash

echo "Creating Kafka topics..."

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic exam_attempts \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --if-not-exists

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic session_logs \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --if-not-exists

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic student_events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --if-not-exists

echo "Verifying topics created..."
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

echo "Kafka topics created successfully!"
