"""
Configure Superset Database Connections
Phase 11 - Step 43
"""

import requests
import json

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"

print("=" * 70)
print("CONFIGURING SUPERSET DATABASE CONNECTIONS")
print("=" * 70)

# Step 1: Login to get access token
print("\n1. Logging in to Superset...")
login_data = {
    "username": USERNAME,
    "password": PASSWORD,
    "provider": "db"
}

session = requests.Session()
response = session.post(
    f"{SUPERSET_URL}/api/v1/security/login",
    json=login_data
)

if response.status_code == 200:
    access_token = response.json()["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    print("   ✓ Login successful")
else:
    print(f"   ✗ Login failed: {response.text}")
    exit(1)

# Step 2: Get CSRF token
csrf_response = session.get(
    f"{SUPERSET_URL}/api/v1/security/csrf_token/",
    headers=headers
)
csrf_token = csrf_response.json()["result"]
headers["X-CSRFToken"] = csrf_token

print("\n2. Configuring PostgreSQL connection...")

postgres_config = {
    "database_name": "Academic Integrity Analytics",
    "sqlalchemy_uri": "postgresql+psycopg2://superset:superset@postgres:5432/analytics",
    "expose_in_sqllab": True,
    "allow_run_async": True,
    "allow_ctas": True,
    "allow_cvas": True,
    "allow_dml": False
}

response = session.post(
    f"{SUPERSET_URL}/api/v1/database/",
    headers=headers,
    json=postgres_config
)

if response.status_code in [200, 201]:
    print("   ✓ PostgreSQL connection created")
else:
    print(f"   ✗ Failed to create PostgreSQL connection: {response.text}")

print("\n3. Configuring Redis connection...")

# Note: Superset doesn't natively support Redis as a database
# We'll use MongoDB for all queries and cache results in Redis separately
print("   ℹ Redis is used as cache backend, not as direct data source")
print("   ℹ All dashboard queries will use MongoDB")

print("\n" + "=" * 70)
print("CONFIGURATION COMPLETE")
print("=" * 70)
print("\nNext steps:")
print("1. Open Superset UI: http://localhost:8088")
print("2. Go to Data > Databases to verify connections")
print("3. Create datasets from MongoDB collections")
