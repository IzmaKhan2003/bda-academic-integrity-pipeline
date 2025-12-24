#!/bin/bash
set -e

# Create metastore database for Hive
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE metastore;
    GRANT ALL PRIVILEGES ON DATABASE metastore TO airflow;
EOSQL

echo "PostgreSQL initialized with airflow and metastore databases"
