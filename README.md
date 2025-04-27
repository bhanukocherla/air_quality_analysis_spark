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

---

# Section-2: Data Aggregations, Transformations & Trend Analysis

## Objective
Enhance the pre-processed air quality dataset by applying:
- Outlier detection and treatment
- Normalization (Z-score scaling)
- Time-based aggregations (daily and hourly)
- Rolling averages, lag features, and trend indicators
- Saving final outputs as single CSV files for easy analysis

---

## Prerequisites

- Install required Python libraries:
```bash
pip install -r requirements.txt
```

- Install Java (for Spark):
```bash
sudo apt update
sudo apt install default-jdk -y
```

- Verify Java installation:
```bash
java -version
readlink -f $(which java)
```

- Set environment variables:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

---

## Commands to Run

- Run Section-2 processing script:
```bash
python3 section-2.py
```

- Outputs will be saved in the following locations:
  - `Output/Section2_enhanced_raw/`
  - `Output/Section2_daily_aggregation/`
  - `Output/Section2_hourly_aggregation/`

Each will contain **only one single CSV file**.

---

## Issues Faced & Solutions:

**1. Multiple Output Files Instead of One**
- **Issue**: Spark by default splits output into multiple `part-0000` files.
- **Fix**: Used `.coalesce(1)` before `.write()` to force saving as a single CSV.

**2. ApproxQuantile Error on avg_pm25**
- **Issue**: `avg_pm25` column was read as StringType instead of numeric.
- **Fix**: Explicitly casted `avg_pm25` to `DoubleType()` after reading the CSV.

**3. Lag Function AnalysisException**
- **Issue**: Error occurred while applying a custom window frame to `lag()`.
- **Fix**: Created a separate window without `.rowsBetween()` for the lag feature.

**4. Missing Temperature and Humidity Fields**
- **Issue**: Section-1 data did not include `temperature` and `humidity` columns.
- **Fix**: Generated random `temperature` (10°C–40°C) and `humidity` (20–90%) values using Spark's `rand()` function.

**5. Java Gateway Exit Error**
- **Issue**: Spark failed to start due to missing Java or unset `JAVA_HOME`.
- **Fix**: Installed Java 17, set the correct `JAVA_HOME` path, and updated the `PATH` variable.

---

## Output

✅ Final output fields for enriched dataset:

| Column | Description |
|:-------|:------------|
| location | Sensor region name |
| timestamp | ISO formatted timestamp |
| parameter | "pm25" |
| units | "ug/m3" |
| avg_pm25 | Cleaned and capped PM2.5 values |
| temperature | Random temperature between 10°C and 40°C |
| humidity | Random humidity between 20% and 90% |
| rolling_avg_pm25 | 3-period rolling average of PM2.5 |
| lag1_pm25 | Previous reading of PM2.5 |
| rate_of_change_pm25 | Difference from previous PM2.5 reading |
| day_of_week | Day of the week extracted from timestamp |
| hour_of_day | Hour extracted from timestamp |
| pm25_x_humidity | PM2.5 multiplied by humidity |

-**Output/Section2_daily_aggregation**
![image](https://github.com/user-attachments/assets/081f78a4-b24b-43fa-9672-7f3b704bc6d0)

-**Output/Section2_enhanced_raw**
![image](https://github.com/user-attachments/assets/1b817248-1247-456f-8442-6ffe8e206a6d)

-**Output/Section2_hourly_aggregation**
![image](https://github.com/user-attachments/assets/6990a019-0e21-46b7-b7f2-98ad783b1f5e)

---








