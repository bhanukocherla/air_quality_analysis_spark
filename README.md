# Air Quality Analysis - Section 1: Data Ingestion

## Project Objective
Build an end-to-end pipeline for air quality monitoring that:
- Ingests real-time sensor data (PM2.5, temperature, humidity)
- Processes and merges multiple data streams
- Performs quality checks and transformations
- Enables analysis and forecasting

## Section 1 Objective
Ingest and pre-process raw sensor data from TCP streams to create:
✔ Clean, timestamped records  
✔ Merged metrics per location-time  
✔ Validated output ready for analysis

## Prerequisites
```bash
pip install -r requirements.txt
```
# Install Java (Spark dependency)
```bash
sudo apt update
```
```bash
sudo apt install default-jdk -y
```

# Verify installation
```bash
java -version
```
```bash
readlink -f $(which java)
```

# Set environment variables
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```
```bash
export PATH=$JAVA_HOME/bin:$PATH
```

## Commands to Run
```bash
# Terminal 1: Start TCP server (data producer)
python3 ingestion/tcp_log_file_streaming_server.py
```

```bash
# Terminal 2: Start Spark streaming (data processor)
python3 ingestion/structured_streaming_ingestion.py
```

## Issues Faced & Solutions

### Broken Pipe Errors
- **Cause**: Spark client disconnecting mid-stream
- **Fix**: Added proper JSON formatting and throttling in TCP server

### Missing Output Files
- **Cause**: Checkpoint directory conflicts
- **Fix**: Clear Output/ directory between test runs

### Java Not Found
- **Cause**: Incorrect JAVA_HOME path
- **Fix**: Verified path with `readlink -f $(which java)`

## Sample Output

![image](https://github.com/user-attachments/assets/0c03f4b2-243c-48ee-9dee-55188406c632)






