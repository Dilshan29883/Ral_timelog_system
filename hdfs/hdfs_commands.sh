#!/bin/bash

################################################################################
# HDFS Commands for Real-Time Log Monitoring System
# 
# This script contains useful HDFS operations for log storage and management
#
# ═══════════════════════════════════════════════════════════════════════════
# HOW TO RUN
# ═══════════════════════════════════════════════════════════════════════════
#
# OPTION 1: Docker-based HDFS (Recommended for development)
# ──────────────────────────────────────────────────────────────────────────
#   Note: Current Docker Compose does not include HDFS. This script is for
#   reference when deploying to a Hadoop cluster.
#
# OPTION 2: Standalone HDFS (Single machine cluster)
# ──────────────────────────────────────────────────────────────────────────
#   1. Install Hadoop (choose one method):
#
#      METHOD A: Download from Apache (Recommended)
#      - Visit: https://hadoop.apache.org/releases.html
#      - Download: hadoop-3.3.6.tar.gz
#      - Extract: tar -xzf hadoop-3.3.6.tar.gz -C /usr/local/
#      - Link: sudo ln -s /usr/local/hadoop-3.3.6 /usr/local/hadoop
#      - Set $HADOOP_HOME environment variable
#
#      METHOD B: Ubuntu/Debian (via apt-get)
#      $ sudo apt-get update
#      $ sudo apt-get install openjdk-11-jdk-headless
#      Note: hadoop package may not be available in default repos
#      Use METHOD A or METHOD C instead
#
#      METHOD C: macOS (via Homebrew)
#      $ brew install hadoop
#
#      METHOD D: Using Docker (if Hadoop not available locally)
#      See hdfs/README_HDFS.md for Docker approach
#
#   2. Set environment variable:
#      export HADOOP_HOME=/usr/local/hadoop
#      export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
#
#   3. Start HDFS NameNode and DataNode:
#      $HADOOP_HOME/sbin/start-dfs.sh
#
#   4. Verify HDFS is running:
#      hdfs dfs -ls /
#      (or check: jps)
#
#   5. Run this script:
#      bash hdfs/hdfs_commands.sh
#
# OPTION 3: Production Hadoop Cluster
# ──────────────────────────────────────────────────────────────────────────
#   1. Modify HDFS_NAMENODE to point to cluster NameNode:
#      HDFS_NAMENODE="hdfs://namenode-host:9000"
#
#   2. Ensure you have Hadoop CLI installed on client machine
#
#   3. Run script:
#      bash hdfs/hdfs_commands.sh
#
# ═══════════════════════════════════════════════════════════════════════════
# PREREQUISITES
# ═══════════════════════════════════════════════════════════════════════════
# - Hadoop installed and $HADOOP_HOME set
# - HDFS NameNode running and accessible
# - Sufficient disk space on HDFS (for logs)
# - Read/write permissions to HDFS /logs directory
# - SSH access to Hadoop cluster (for multi-node)
#
# ═══════════════════════════════════════════════════════════════════════════
# WHAT THIS SCRIPT DOES
# ═══════════════════════════════════════════════════════════════════════════
# 1. Creates HDFS directory structure for logs
# 2. Sets appropriate file permissions (755)
# 3. Verifies directory creation
# 4. Shows storage statistics
# 5. Uploads sample logs (if available)
# 6. Sets replication factor to 3 (for redundancy)
#
# ═══════════════════════════════════════════════════════════════════════════

# Configuration
HDFS_USER="hdfs"
HDFS_NAMENODE="hdfs://localhost:9000"
LOG_PATH="/logs"
DATA_PATH="${HDFS_NAMENODE}${LOG_PATH}"

echo "HDFS Operations for Log Monitoring System"
echo "==========================================="

# Create directories
echo -e "\n1. Creating HDFS directories..."
hdfs dfs -mkdir -p ${DATA_PATH}/raw
hdfs dfs -mkdir -p ${DATA_PATH}/processed
hdfs dfs -mkdir -p ${DATA_PATH}/checkpoints
hdfs dfs -mkdir -p ${DATA_PATH}/results

# Set permissions
echo "2. Setting permissions..."
hdfs dfs -chmod 755 ${DATA_PATH}
hdfs dfs -chmod 755 ${DATA_PATH}/raw
hdfs dfs -chmod 755 ${DATA_PATH}/processed

# List directory structure
echo -e "\n3. Verifying directory structure..."
hdfs dfs -ls -R ${DATA_PATH}

# Show usage statistics
echo -e "\n4. Storage Statistics:"
echo "Total usage:"
hdfs dfs -du -sh ${DATA_PATH}

# Upload sample data (if local files exist)
if [ -f "data/sample/sample_logs.txt" ]; then
    echo -e "\n5. Uploading sample logs to HDFS..."
    hdfs dfs -put -f data/sample/sample_logs.txt ${DATA_PATH}/raw/
fi

# Set replication factor
echo -e "\n6. Setting replication factor for logs..."
hdfs dfs -setrep -w 3 ${DATA_PATH}/raw

echo -e "\n✓ HDFS setup complete!"

################################################################################
# USAGE EXAMPLES & TROUBLESHOOTING
################################################################################

# Example 1: Check HDFS connectivity
# hdfs dfs -ls /

# Example 2: View logs in HDFS
# hdfs dfs -ls -h ${DATA_PATH}/raw

# Example 3: Download results from HDFS
# hdfs dfs -get ${DATA_PATH}/results/* output/

# Example 4: Delete old logs
# hdfs dfs -rm -r ${DATA_PATH}/raw/logs_2026_01_*

# Troubleshooting:
# ─────────────────────────────────────────────────────────────────────────
# ERROR: "Call from client X to hdfs@Y:9000 failed on connection exception"
#   → HDFS NameNode not running. Start with: $HADOOP_HOME/sbin/start-dfs.sh
#
# ERROR: "Permission denied"
#   → Not HDFS user. Run with: sudo -u hdfs bash hdfs/hdfs_commands.sh
#
# ERROR: "mkdir: Failed to create directory"
#   → Check HDFS is writable. Run: hdfs dfs -chmod 777 /
#
# For more: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html
################################################################################
