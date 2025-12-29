# Use official Spark image
FROM apache/spark:3.4.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python packages with timeout and PyPI mirror
RUN python3 -m pip install --upgrade pip setuptools wheel

# Split install to avoid timeout issues
RUN pip3 install --default-timeout=100 -i https://pypi.org/simple numpy
RUN pip3 install --default-timeout=100 -i https://pypi.org/simple pandas
RUN pip3 install --default-timeout=100 -i https://pypi.org/simple pymongo kafka-python faker scipy pyspark redis

# Switch back to spark user
USER spark
