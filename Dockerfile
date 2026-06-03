FROM python:3.11-slim

WORKDIR /app

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed data/sample output logs_summary error_reports checkpoint

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "-c", "print('Real-Time Log Monitoring System - Ready')"]
