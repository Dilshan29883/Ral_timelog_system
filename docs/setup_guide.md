# Real-Time Log Monitoring System - Setup Guide

## Prerequisites

### System Requirements
- **OS**: Linux, macOS, or Windows (with WSL2)
- **RAM**: 8GB minimum (16GB recommended)
- **Disk**: 20GB free space for logs and data
- **Java**: OpenJDK 17 or later
- **Python**: 3.11 or later
- **Docker** (optional but recommended): 20.10+
- **Docker Compose** (optional): 2.0+

### Software Installation

#### macOS
```bash
# Using Homebrew
brew install java@17 python@3.11 docker docker-compose
```

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install openjdk-17-jdk python3.11 python3.11-venv docker.io docker-compose
```

#### Windows (WSL2)
```powershell
# Install WSL2
wsl --install

# In WSL2 Ubuntu terminal:
sudo apt-get update
sudo apt-get install openjdk-17-jdk python3.11 python3.11-venv docker.io
```

---

## Installation

### 1. Clone/Download Project
```bash
cd ~/projects
git clone <repo-url> real-timelog-system
cd real-timelog-system
```

### 2. Create Virtual Environment (Host)
```bash
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Python Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Configuration

### Edit Main Configuration
```bash
# Review and customize settings
nano config/app_config.json
```

Key configuration options:
- `num_records`: Total log records to generate
- `batch_size`: Records per batch
- `anomaly_percentage`: Percentage of error events (0.0-1.0)
- `services`: List of service names
- `hosts`: List of host names
- `response_time_threshold_ms`: Latency threshold for anomalies
- `error_rate_threshold`: Error rate threshold for alerts

---

## Running the System

### Option 1: Local File-Based Mode (Simplest)

```bash
# 1. Generate sample logs
python scripts/log_generator.py

# 2. Run batch analysis
python spark/batch_analysis.py \
    config/app_config.json \
    data/sample/sample_logs.txt \
    output/

# 3. (Optional) Run streaming simulation
python spark/streaming_job.py \
    --config config/app_config.json \
    --source file \
    --log-dir data/sample
```

### Option 2: Docker Compose (Complete Stack)

```bash
# 1. Build and start all services
docker-compose up -d

# 2. Wait for services to be ready (~30 seconds)
sleep 30

# 3. Access services
# Jupyter: http://localhost:8888 (token: dev)
# Spark UI: http://localhost:8080
# Kafka: localhost:9092

# 4. Run log generator (from host terminal)
python scripts/log_generator.py

# 5. Run streaming job (from host terminal)
python spark/streaming_job.py \
  --config config/app_config.json \
    --source file \
  --log-dir data/sample

# 6. Stop services
docker-compose down
```

### Option 3: Kafka-Based Streaming

```powershell
# 1. Start Docker services
docker-compose up -d

# 2. Start log producer (in one terminal)
.\venv\Scripts\Activate.ps1
python -m scripts.kafka_producer `
  --bootstrap-servers localhost:9092 `
  --topic raw-logs `
  --batches 50

# 3. Start streaming consumer (in another terminal)
.\venv\Scripts\Activate.ps1
python scripts/kafka_consumer.py `
  --bootstrap-servers localhost:9092 `
  --topic raw-logs

# 4. Run streaming analysis job (in third terminal)
.
\venv\Scripts\Activate.ps1
python spark/streaming_job.py `
  --config config/app_config.json `
  --source kafka `
    --output-mode append
```

---

## Jupyter Notebook

### Local Mode
```bash
# 1. Ensure Spark is available
export SPARK_HOME=/usr/local/spark  # Adjust path if needed

# 2. Start Jupyter
jupyter notebook notebooks/

# 3. Open analysis.ipynb and run cells in order
```

### Docker Mode
```bash
# 1. Jupyter automatically starts at http://localhost:8888
# 2. Token: dev (configured in docker-compose.yml)
# 3. Navigate to /home/jovyan/notebooks/analysis.ipynb
```

---

## Monitoring & Debugging

### View Spark Logs
```bash
# Local mode
tail -f logs/spark-app.log

# Docker mode
docker logs spark-master -f
```

### Check Processing Status
```bash
# View checkpoint directory
ls -la checkpoint/logs/

# Inspect output files
ls -la output/logs_summary/
```

### Run Individual Components

```bash
# Test log generation
python scripts/log_generator.py

# Test utilities
python -c "from spark.utils import load_json_config; print(load_json_config('config/app_config.json'))"

# Run batch analysis only
python spark/batch_analysis.py config/app_config.json data/sample/sample_logs.txt output/
```

---

## Troubleshooting

### Issue: "Java not found"
```bash
# Check Java installation
java -version

# If not installed:
# macOS: brew install java@17
# Ubuntu: sudo apt-get install openjdk-17-jdk
```

### Issue: "PySpark not found"
```bash
# Reinstall PySpark
pip install --force-reinstall pyspark==3.5.0
```

### Issue: "Port 8888 already in use"
```bash
# Use different port
jupyter notebook notebooks/ --port 8889
```

### Issue: "Docker daemon not running"
```bash
# Start Docker daemon
# macOS: open -a Docker
# Ubuntu: sudo service docker start
# Windows: Open Docker Desktop
```

### Issue: "Kafka connection refused"
```bash
# Check Kafka is running in Docker
docker-compose logs kafka

# Verify broker is accessible
docker exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

### Issue: Streaming job not reading files
```bash
# Verify directory permissions
ls -la data/sample/

# Check log file format (should be JSONL)
head data/sample/sample_logs.txt | head -1
```

---

## Performance Tuning

### For Local Development
```json
{
  "spark": {
    "master": "local[4]",
    "driver": {"memory": "2g"},
    "executor": {"cores": 1}
  }
}
```

### For Production Cluster
```json
{
  "spark": {
    "master": "spark://spark-master:7077",
    "driver": {"memory": "4g"},
    "executor": {"cores": 4, "memory": "8g", "instances": 10}
  }
}
```

### Streaming Parameters
```json
{
  "streaming": {
    "trigger_interval_seconds": 5,
    "window_duration_seconds": 60,
    "slide_duration_seconds": 10
  }
}
```

---

## File Structure Reference

```
real-timelog-system/
├── config/               # Configuration files
│   ├── app_config.json   # Main configuration
│   ├── spark_config.yaml # Spark tuning
│   └── kafka_config.json # Kafka settings
├── scripts/              # Python scripts
│   ├── log_generator.py  # Synthetic log generation
│   ├── kafka_producer.py # Kafka producer
│   └── kafka_consumer.py # Kafka consumer
├── spark/                # Spark jobs
│   ├── streaming_job.py  # Streaming pipeline
│   ├── batch_analysis.py # Batch processing
│   └── utils.py          # Utilities
├── data/                 # Data directories
│   ├── raw/              # Raw logs
│   ├── processed/        # Processed logs
│   └── sample/           # Sample datasets
├── notebooks/            # Jupyter notebooks
│   └── analysis.ipynb    # Analysis notebook
├── output/               # Output files
│   ├── logs_summary/     # Aggregated metrics
│   └── error_reports/    # Alerts
├── docker-compose.yml    # Docker orchestration
├── Dockerfile            # Container definition
├── requirements.txt      # Python dependencies
└── README.md            # Main documentation
```

---

## Next Steps

1. **Explore the Code**: Review the module implementations in `spark/` and `scripts/`
2. **Run the Notebook**: Execute the Jupyter notebook to see analysis in action
3. **Customize Configuration**: Adjust thresholds and parameters for your needs
4. **Scale the System**: Deploy to a Spark cluster for production workloads
5. **Integrate Dashboards**: Connect to Grafana for real-time monitoring

---

## Support & Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Documentation](https://docs.docker.com/)
- [Jupyter Documentation](https://jupyter.org/documentation)

---

## License

[Your License Here]

---

**Last Updated**: April 2026  
**Version**: 1.0.0
