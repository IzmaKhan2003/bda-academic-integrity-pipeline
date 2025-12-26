#!/bin/bash

echo "======================================"
echo "INITIALIZING APACHE SUPERSET"
echo "======================================"
echo ""

# Step 1: Initialize Superset database
echo "1. Initializing Superset database..."
docker exec superset superset db upgrade

echo ""

# Step 2: Create admin user
echo "2. Creating admin user..."
docker exec superset superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin123

echo ""

# Step 3: Initialize Superset
echo "3. Initializing Superset..."
docker exec superset superset init

echo ""

echo "✓ Superset initialized successfully"
echo ""
echo "Access Superset: http://localhost:8088"
echo "Username: admin"
echo "Password: admin123"
echo ""
echo "======================================"
