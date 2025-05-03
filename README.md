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


# Section-3: Air Quality Analysis with Spark SQL
## Objective
Perform SQL-based analysis on the enhanced air quality dataset by:

Registering dataframes as temporary SQL views

Running SQL queries for key statistics and trends

Generating summarized outputs for reporting
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
- Set environment variables:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

---


## Commands to Run
# Run Section-3 SQL analysis script:
```bash
 python3 section-3.py
```
--> Outputs will be saved in the following location:
    Output/Section3_SQL_Results/
 --->Each SQL query result will be saved as a separate CSV file.

### Issues Faced & Solutions
# 1. File Not Found Error

***Issue: Path to enhanced Section-2 output CSV was incorrect.***

Fix: Double-checked the file path and ensured Section-2 was run before Section-3.

# 2. Java Gateway Exit Error

***Issue: Spark session failed to initialize due to missing Java.***

Fix: Installed Java and properly set JAVA_HOME and PATH environment variables.

# 3. AnalysisException during SQL Queries

***Issue: Some columns had unexpected names or types.***

Fix: Explicitly casted columns where needed and validated schema using df.printSchema().

**Output for section 3 output_section3_csv

![image](https://github.com/user-attachments/assets/6ec228c5-146c-4798-ba68-38c5730e4483)



# Section 4: Air Quality Forecasting using Spark MLlib

This module trains and evaluates a machine learning model to forecast PM2.5 air quality levels based on historical weather and pollution data. It is a part of the full pipeline for the **Air Quality Monitoring & Forecasting Project**.

---

##  Objectives

- Select relevant features for air quality forecasting
- Train a **Linear Regression** model using **Spark MLlib**
- Evaluate model performance (RMSE and R²)
- Tune hyperparameters using **CrossValidator**
- Save the trained model and predictions for dashboard use

---

##  Files

| File | Description |
|------|-------------|
| `section-4.py` | Main script for training and evaluating the ML model |
| `Output/section4_model/` | Directory to store the trained model |
| `Output/section4_predictions/` | Directory containing actual vs. predicted PM2.5 values (CSV format) |

---

##  Directory Structure

air_quality_analysis_spark/
│
├── Output/
│ ├── output_section3_csv/ # Cleaned input data from Section 3
│ ├── section4_model/ # Trained Spark MLlib model
│ └── section4_predictions/ # Actual vs. predicted PM2.5 CSVs
│
├── section-4.py # Main script for Section 4
├── README.md # You're reading it!
└── requirements.txt # Python dependencies
---

## ⚙️ Features Used for Training

- `temperature`
- `humidity`
- `lag1_pm25`
- `rolling_avg_pm25`
- `rate_of_change_pm25`

> These features are assembled into a feature vector using `VectorAssembler`.

---

## 📈 Model Output

- **Initial RMSE** and **R²** on test data
- **Best RMSE** and **Best R²** after hyperparameter tuning
- Saved model available at: `Output/section4_model/air_quality_lr_model/`
- Prediction results available at: `Output/section4_predictions/` (includes actual vs. predicted PM2.5)

---

##  Data Handling

- Loaded from: `Output/output_section3_csv/*.csv`
- Null values in features are handled via mean imputation using `Imputer`
- Target label for prediction: `avg_pm25`

---

##  Real-Time Integration Plan

Model is designed to be used in streaming pipelines:

1. Stream incoming data from TCP or Kafka
2. Preprocess using same feature pipeline
3. Load this trained model
4. Predict PM2.5 in real-time
5. Display in dashboard or trigger alerts

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

##  How to Run

```bash
python section-4.py
```
Make sure the dataset from Section 3 is available in:

```bash
Output/output_section3_csv/
```
And required Python packages are installed:
```bash
pip install pyspark findspark
```
---
## Evaluation Metrics
Metric	Description
RMSE	Root Mean Squared Error – measures average error
R²	Coefficient of Determination – indicates goodness of fit

## Next Steps (Section 5)
Use the output predictions (Output/section4_predictions/) to build:

Time-series plots of predicted vs actual PM2.5

AQI classification visualizations

Real-time dashboards (e.g., with Plotly, Matplotlib, or Grafana)
---
## Output (Console)
csharp
Copy
Edit
Initial RMSE: 4.87
Initial R²: 0.83
[Tuned] Best RMSE: 4.52
[Tuned] Best R²: 0.86

# Section-5: Real-Time Pipeline Integration & Dashboard Visualization

## Objective

## Integrate all components into a real-time Spark pipeline and visualize insights using an interactive dashboard. This section includes:

- Running the full data ingestion-to-prediction pipeline
- Generating real-time alerts based on PM2.5 thresholds
- Serving live results via a Streamlit dashboard

## Install Python libraries
```bash
pip install -r requirements.txt
```

## Install Java (Spark dependency)
```bash
sudo apt update
```
```bash
sudo apt install default-jdk -y
```

## Verify Java installation
```bash
java -version
```
```bash
readlink -f $(which java)
```

## Set environment variables
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```
```bash
export PATH=$JAVA_HOME/bin:$PATH
```

## Commands to Run

```bash
python3 dashboardVisualization.py
```
## output

![image](https://github.com/user-attachments/assets/61bf647d-49e7-4d1c-a9e4-a81340d0cf70)






    

    








