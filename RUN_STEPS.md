# Real-Time Log Monitoring System - Run Steps

Complete guide to run the project in different modes.

---

## Option 1: Quick Start (Local File-Based)

### Prerequisites
- Python 3.11+
- Java 17+ (required by Spark)
- ~2GB RAM available

### Steps

```bash
# 1. Navigate to project directory
cd real-timelog-system

# 2. Create virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Generate sample logs
python scripts/log_generator.py

# 5. Run batch analysis
python spark/batch_analysis.py config/app_config.json data/sample/sample_logs.txt output/

# 6. Run streaming pipeline (file-based)
python spark/streaming_job.py --config config/app_config.json --source file --log-dir data/sample
```

**Expected Output:**
- Console shows streaming aggregations every 10 seconds
- Output files saved to `output/logs_summary/`

---

## Option 2: Docker Compose (Complete Stack)

All services in containers: Spark, Kafka, Zookeeper, Jupyter.

### Prerequisites
- Docker & Docker Compose installed
- ~4GB RAM available
- Port 8080, 8888, 9092, 2181 available

### Steps

```bash
# 1. Navigate to project directory
cd real-timelog-system

# 2. Start all services (builds automatically on first run)
docker-compose up -d

# 3. Wait for services to be ready (~30 seconds)
sleep 30

# 4. Verify services are running
docker compose ps

# 5. Access services:
#    - Spark Master UI: http://localhost:8080
#    - Spark Worker UI: http://localhost:8081
#    - Jupyter: http://localhost:8888 (token: dev)
#    - Kafka: localhost:9092
#    - Zookeeper: localhost:2181

# 6. Generate sample logs (from host terminal)
python scripts/log_generator.py

# 7. Run streaming with file source (from host terminal)
python spark/streaming_job.py --config config/app_config.json --source file --log-dir data/sample

# 8. (Optional) Stop all services
docker-compose down
```

**Service URLs:**
| Service | URL | Purpose |
|---------|-----|---------|
| Spark Master UI | http://localhost:8080 | Monitor Spark jobs |
| Spark Worker UI | http://localhost:8081 | Monitor executors |
| Jupyter Notebook | http://localhost:8888 | Interactive analysis |
| Kafka Broker | localhost:9092 | Message streaming |

---

## Option 3: Kafka-Based Streaming

Decoupled producer and consumer using Kafka message queue.

### Prerequisites
- Docker Compose running (from Option 2)
- Terminal with Python venv activated

### Steps (3 terminals)

**Terminal 1: Start Docker services**
```bash
cd real-timelog-system
docker-compose up -d
sleep 30
```

**Terminal 2: Start Kafka producer (generates logs)**
```powershell
cd real-timelog-system
.\venv\Scripts\Activate.ps1

python scripts/kafka_producer.py `
    --bootstrap-servers localhost:9092 `
    --topic raw-logs `
    --batches 50 `
    --batch-size 100
```

**Terminal 3: Start streaming consumer**
```powershell
cd real-timelog-system
.\venv\Scripts\Activate.ps1

python spark/streaming_job.py `
    --config config/app_config.json `
    --source kafka
```

**What happens:**
1. Producer sends logs to Kafka topic `raw-logs`
2. Streaming job reads from Kafka
3. Console shows real-time aggregations and anomalies
4. Press Ctrl+C to stop

---

## Option 4: Jupyter Notebook (Interactive Analysis)

### Prerequisites
- Docker Compose running (from Option 2)
- OR: Python venv with dependencies installed

### Steps

```bash
# 1. Navigate to project
cd real-timelog-system

# 2A. If using Docker
# Jupyter is already running at http://localhost:8888
# Token: dev

# 2B. If using local Python
jupyter notebook notebooks/

# 3. Open analysis.ipynb
# Click on: notebooks/analysis.ipynb

# 4. Run cells in order (Shift+Enter)
# - Cell 1-5: Environment setup and imports
# - Cell 6-8: Generate synthetic log data
# - Cell 9-14: Data preprocessing and feature engineering
# - Cell 15-25: Streaming pipeline demonstration
# - Cell 26+: Results, visualizations, and analysis
```

**Cell Groups:**
- **Setup (Cells 1-5):** Initialize Spark and load libraries
- **Data Generation (Cells 6-8):** Create 10,000 sample logs
- **Preprocessing (Cells 9-14):** Add features, validate data
- **Processing (Cells 15-25):** Apply design patterns (filtering, aggregation, windowing)
- **Results (Cells 26+):** Visualizations and statistical analysis

---

## Option 5: Windows PowerShell Notes

PowerShell uses backticks (`) for line continuation, not backslash (\).

### Single-line commands (recommended)
```powershell
python spark/streaming_job.py --config config/app_config.json --source kafka
```

### Multi-line commands (PowerShell)
```powershell
python spark/streaming_job.py `
   --config config/app_config.json `
   --source kafka
```

---

## Troubleshooting

### Issue: "Port 8888 already in use"
```bash
# Use different Jupyter port
jupyter notebook notebooks/ --port 8889
```

### Issue: "Docker daemon not running"
- **Windows:** Open Docker Desktop
- **macOS:** `open -a Docker`
- **Linux:** `sudo service docker start`

### Issue: "Kafka connection refused"
```bash
# Check Kafka is running
docker compose logs kafka

# Verify from inside container
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Issue: "Java not found"
```bash
# macOS
brew install java@17

# Ubuntu/Debian
sudo apt-get install openjdk-17-jdk

# Windows
# Download from https://www.oracle.com/java/technologies/downloads/#java17
```

### Issue: "ModuleNotFoundError: No module named 'pyspark'"
```bash
# Reinstall in venv
pip install --force-reinstall pyspark==3.5.0
```

### Issue: "Failed to find data source: kafka"
- Ensure using: `python spark/streaming_job.py` (not docker exec)
- Streaming job auto-downloads Kafka packages from Maven

### Issue: Streaming job hangs after starting
- Normal behavior: waiting for data
- Producer is running? Check Terminal 2
- Press Ctrl+C to stop and restart

---

## Output Locations

| Output | Location | Description |
|--------|----------|-------------|
| Sample logs | `data/sample/analysis_sample_logs.jsonl` | Generated synthetic logs |
| Error reports | `output/error_reports/` | Aggregated error statistics |
| Metrics | `output/logs_summary/` | Service-level metrics |
| Checkpoints | `checkpoint/logs/` | Streaming job checkpoints |
| Visualizations | `output/logs_summary/*.png` | Charts and graphs |

---

## Performance Expectations

| Mode | Throughput | Latency | Setup Time |
|------|-----------|---------|-----------|
| Local (file) | 10,000 events/sec | <5 sec | ~30 sec |
| Kafka | 10,000 events/sec | <5 sec | ~60 sec |
| Docker | 10,000 events/sec | <5 sec | ~45 sec |
| Cluster | 100,000+ events/sec | <5 sec | ~2 min |

---

## Next Steps After Running

1. **Analyze Results:** Check `output/logs_summary/` for metrics
2. **View Visualizations:** Open PNG files from Jupyter or output folder
3. **Tune Thresholds:** Edit `config/app_config.json` to adjust anomaly detection
4. **Scale Up:** Run on Spark cluster by changing `--master` parameter
5. **Production Deployment:** Use Kubernetes or cloud platforms

---

## Getting Help

- **Configuration:** See `config/app_config.json` for all tuning options
- **Architecture:** Read `README.md` for system design details
- **Code Examples:** Check `spark/` directory for source code
- **Logs:** View Docker logs: `docker compose logs -f <service-name>`
- **Jupyter:** Run cells individually and inspect outputs

---

**Last Updated:** April 2026  
**Tested On:** Python 3.11, Spark 3.5.0, Docker 24.0
