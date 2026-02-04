FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    duckdb==1.1.3 \
    pandas \
    pyarrow \
    requests \
    tabulate \
    ipython

# Create directories
RUN mkdir -p /app/data /app/scripts /app/sql

WORKDIR /app
