# Hadoop Installation for Ubuntu/Debian

## Quick Installation Steps

Run these commands one by one on your Ubuntu/Debian machine:

### Step 1: Install Java (Required)
```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk-headless wget
java -version
```

### Step 2: Download Hadoop
```bash
cd /tmp
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```

### Step 3: Install Hadoop
```bash
sudo tar -xzf hadoop-3.3.6.tar.gz -C /usr/local/
sudo ln -s /usr/local/hadoop-3.3.6 /usr/local/hadoop
sudo chown -R $USER:$USER /usr/local/hadoop
```

### Step 4: Configure SSH (Required by Hadoop)

Hadoop uses SSH to communicate between nodes. Even on a single machine, it needs this.

```bash
# Install SSH server
sudo apt-get install -y openssh-server openssh-client

# Start SSH service
sudo service ssh start

# Generate SSH key (press Enter for all prompts)
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa

# Add public key to authorized_keys for passwordless login
cat ~/.ssh/id_rsa.pub >> ~/.ssh/6+KYTT64wpMbBUmjqOtmDuTgQbVJjCDP1CQAvdS4+M0
chmod 600 ~/.ssh/6+KYTT64wpMbBUmjqOtmDuTgQbVJjCDP1CQAvdS4+M0

# Test SSH to localhost (should not ask for password)
ssh localhost 'echo "SSH works!"'
```

If SSH test succeeds, you should see: `SSH works!`

### Step 5: Configure Environment Variables

```bash
cat >> ~/.bashrc << 'EOF'
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
EOF

source ~/.bashrc
```

### Step 5: Verify Installation
```bash
hadoop version
java -version
```

### Step 6: Format HDFS NameNode (First Time Only!)
```bash
hdfs namenode -format
```
⚠️ **WARNING:** This will delete all HDFS data! Only run once on fresh install.

### Step 7: Start HDFS
```bash
start-dfs.sh
```

### Step 8: Verify HDFS is Running
```bash
jps
```
✓ Output should show: NameNode, DataNode, SecondaryNameNode

---

## After Installation: Run Project HDFS Setup

Once Hadoop is running:

```bash
# Navigate to project directory
cd ~/real-timelog-system

# Run HDFS setup script
bash hdfs/hdfs_commands.sh
```

Expected output:
```
HDFS Operations for Log Monitoring System
===========================================

1. Creating HDFS directories...
2. Setting permissions...
3. Verifying directory structure...
   drwxr-xr-x   - hdfs hdfs          /logs
4. Storage Statistics...
5. Uploading sample logs to HDFS...
6. Setting replication factor...
✓ HDFS setup complete!
```

---

## Verify Setup

```bash
# Check HDFS is working
hdfs dfs -ls /logs

# View uploaded logs
hdfs dfs -cat /logs/raw/sample_logs.txt | head -5

# Check storage usage
hdfs dfs -du -sh /logs
```

---

## Troubleshooting

### Error: "Unable to locate package hadoop"
→ This is normal. Use the manual download method above (Steps 2-3)

### Error: "Connection refused"
→ HDFS not running. Run: `start-dfs.sh`

### Error: "mkdir: Failed to create directory"
→ Check HDFS is writable: `hdfs dfs -ls /`

### Error: "Permission denied"
→ Run as correct user: `sudo -u hdfs bash hdfs/hdfs_commands.sh`

---

## Stop HDFS (When Done)
```bash
stop-dfs.sh
```

---

## Next Steps

After HDFS is running:

1. **Option A:** Use file-based streaming (no HDFS):
   ```bash
   python spark/streaming_job.py --source file --log-dir data/sample
   ```

2. **Option B:** Use HDFS with Spark:
   ```bash
   python spark/streaming_job.py --source file --log-dir hdfs://localhost:9000/logs/raw
   ```

See **RUN_STEPS.md** for all project execution options.
