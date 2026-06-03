# HDFS Setup for Real-Time Log Monitoring System

Complete guide for using Hadoop Distributed File System with this project.

---

## Overview

HDFS is used for:
- **Persistent log storage** across distributed nodes
- **Scalable data replay** from stored logs
- **Fault-tolerant redundancy** (replication factor: 3)
- **Integration with Spark** for batch processing

---

## Quick Start: 3 Options

### Option 1: Development (No HDFS Required)
For testing and development, use local file system:
```bash
python spark/streaming_job.py --source file --log-dir data/sample
```
✓ No setup needed  
✓ Works on laptop  
✗ Data lost on restart

---

### Option 2: Single-Machine HDFS

#### Prerequisites
```bash
# macOS
brew install hadoop

# Ubuntu/Debian
sudo apt-get install hadoop-hdfs

# CentOS
sudo yum install hadoop-hdfs

# Or download from https://hadoop.apache.org/releases.html
```

#### Setup Steps
```bash
# 1. Set Hadoop environment
export HADOOP_HOME=/usr/local/hadoop  # Adjust path as needed
export PATH=$HADOOP_HOME/bin:$PATH

# 2. Format HDFS (first time only!)
hdfs namenode -format

# 3. Start HDFS services
$HADOOP_HOME/sbin/start-dfs.sh

# 4. Verify running
jps
# Output should show: NameNode, DataNode, SecondaryNameNode

# 5. Run setup script
bash hdfs/hdfs_commands.sh

# 6. Verify logs directory
hdfs dfs -ls /logs
```

#### Use with Project
```bash
# Edit spark/streaming_job.py to use HDFS source:
python spark/streaming_job.py \
    --config config/app_config.json \
    --source file \
    --log-dir hdfs://localhost:9000/logs/raw
```

---

### Option 3: Production Hadoop Cluster

#### Prerequisites
- Hadoop cluster running (NameNode + DataNodes)
- Network connectivity to NameNode (port 9000)
- Hadoop CLI client installed

#### Configuration Steps
```bash
# 1. Edit HDFS_NAMENODE in hdfs_commands.sh
HDFS_NAMENODE="hdfs://namenode-hostname:9000"

# 2. Run setup script
bash hdfs/hdfs_commands.sh

# 3. Verify cluster connectivity
hdfs dfs -ls hdfs://namenode-hostname:9000/
```

---

## HDFS Directory Structure

```
/logs/
├── raw/              # Original uploaded logs
│   └── sample_logs.txt
├── processed/        # Processed by Spark
│   └── *.parquet
├── checkpoints/      # Streaming job state
│   └── *.checkpoint
└── results/          # Final analysis output
    └── *.parquet
```

---

## Running the HDFS Setup Script

### Before Running
```bash
# 1. Ensure HDFS is running
hdfs dfs -ls /  # Should not error

# 2. Create local log file (if doesn't exist)
mkdir -p data/sample
python scripts/log_generator.py
```

### Run Script
```bash
# On Linux/macOS
bash hdfs/hdfs_commands.sh

# On Windows (via WSL or Git Bash)
bash hdfs/hdfs_commands.sh
```

### Expected Output
```
HDFS Operations for Log Monitoring System
===========================================

1. Creating HDFS directories...
2. Setting permissions...
3. Verifying directory structure...
drwxr-xr-x   - hdfs hdfs          /logs
drwxr-xr-x   - hdfs hdfs          /logs/raw
drwxr-xr-x   - hdfs hdfs          /logs/processed
...

4. Storage Statistics:
Total usage:
0  /logs

5. Uploading sample logs to HDFS...
2026-04-30 16:00:00,123 | ...

6. Setting replication factor for logs...
Replication 3 set successfully for /logs/raw

✓ HDFS setup complete!
```

---

## Common Commands

### View Logs in HDFS
```bash
hdfs dfs -ls -h /logs/raw
hdfs dfs -cat hdfs://localhost:9000/logs/raw/sample_logs.txt
```

### Upload New Data
```bash
hdfs dfs -put data/sample/new_logs.txt /logs/raw/
```

### Download Results
```bash
hdfs dfs -get /logs/results/* output/
```

### Delete Old Data
```bash
hdfs dfs -rm -r /logs/raw/old_logs_*
```

### Check Storage Usage
```bash
hdfs dfs -du -sh /logs
hdfs dfs -du -sh /logs/*  # Per directory
```

### Monitor HDFS Health
```bash
# Check NameNode status
hdfs dfsadmin -report

# List live DataNodes
hdfs dfsadmin -report | grep "Live datanodes"

# Check missing blocks
hdfs dfsadmin -report | grep "Under Replicated"
```

---

## Integrating with Spark

### Read from HDFS
```python
df = spark.read.json("hdfs://localhost:9000/logs/raw/sample_logs.txt")
```

### Write to HDFS
```python
df.write.mode("overwrite").parquet("hdfs://localhost:9000/logs/results/")
```

### Streaming from HDFS
```bash
python spark/streaming_job.py \
    --config config/app_config.json \
    --source file \
    --log-dir hdfs://localhost:9000/logs/raw
```

---

## Troubleshooting

### Problem: "Connection refused"
```bash
# HDFS NameNode not running
$HADOOP_HOME/sbin/start-dfs.sh

# Check status
jps  # Should show NameNode
```

### Problem: "Permission denied"
```bash
# HDFS user issue - run as hadoop user
sudo -u hdfs bash hdfs/hdfs_commands.sh

# Or check permissions
hdfs dfs -ls /logs
```

### Problem: "Cannot create file"
```bash
# HDFS not writable - format and restart
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
```

### Problem: "No space left on device"
```bash
# Check cluster storage
hdfs dfsadmin -report

# Check node disk usage
df -h $HADOOP_HOME/../data
```

### Problem: Replication factor cannot be set
```bash
# Ensure enough DataNodes are running
hdfs dfsadmin -report | grep "Live datanodes"

# For testing with 1 node, set replication to 1
hdfs dfs -setrep -w 1 /logs/raw
```

---

## Environment Variables

Set in `.bashrc` or `.bash_profile`:
```bash
export HADOOP_HOME=/usr/local/hadoop
export HDFS_NAMENODE="hdfs://localhost:9000"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Add to PATH
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

---

## Production Checklist

- [ ] HDFS cluster deployed across 3+ nodes
- [ ] Replication factor set to 3 for durability
- [ ] Backup NameNode configured for HA
- [ ] HDFS quota set to prevent runaway logs
- [ ] Automated log rotation/deletion policy
- [ ] Monitoring alerts configured
- [ ] Regular backup of NameNode metadata
- [ ] Network security (firewall rules for port 9000)

---

## References

- [Hadoop HDFS User Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Spark + HDFS Integration](https://spark.apache.org/docs/latest/hadoop-provided.html)
- [Replication and Rack Awareness](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsRackAwareness.html)

---

**Last Updated:** April 2026  
**Tested On:** Hadoop 3.3.0, JDK 17
