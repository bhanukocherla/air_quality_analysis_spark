# PySpark Section-2: Full Final Version (with random Temperature & Humidity, Single Output Files)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, lag, avg, when, to_timestamp, to_date, hour, dayofweek, lit, rand
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# Initialize Spark
spark = SparkSession.builder.appName("Section2_FeatureEngineering_Final").getOrCreate()

# Load Section-1 Output
input_path = "Output/Section1_output_csv"
raw_df = spark.read.option("header", True).csv(input_path)

# Cast avg_pm25 to DoubleType
raw_df = raw_df.withColumn("avg_pm25", col("avg_pm25").cast(DoubleType()))

# Convert datetime to timestamp
raw_df = raw_df.withColumn("datetime", to_timestamp("datetime"))

# Add random temperature (10°C to 40°C) and humidity (20% to 90%)
raw_df = raw_df.withColumn("temperature", (rand() * 30 + 10).cast("double"))
raw_df = raw_df.withColumn("humidity", (rand() * 70 + 20).cast("double"))

# Handle missing values (impute avg_pm25 with mean)
avg_pm25_mean = raw_df.select(mean(col("avg_pm25"))).first()[0]
raw_df = raw_df.withColumn("avg_pm25", when(col("avg_pm25").isNull(), avg_pm25_mean).otherwise(col("avg_pm25")))

# Handle outliers (cap avg_pm25 between 1st and 99th percentiles)
quantiles = raw_df.approxQuantile("avg_pm25", [0.01, 0.99], 0.0)
lower_bound = quantiles[0]
upper_bound = quantiles[1]
raw_df = raw_df.withColumn("avg_pm25", 
            when(col("avg_pm25") < lower_bound, lower_bound)
            .when(col("avg_pm25") > upper_bound, upper_bound)
            .otherwise(col("avg_pm25")))

# Standardize avg_pm25 (Z-score normalization)
mean_val = raw_df.select(mean(col("avg_pm25"))).first()[0]
stddev_val = raw_df.select(stddev(col("avg_pm25"))).first()[0]
raw_df = raw_df.withColumn("avg_pm25_zscore", (col("avg_pm25") - mean_val) / stddev_val)

# Daily Aggregation
daily_df = raw_df.withColumn("date", to_date("datetime"))\
                  .groupBy("location", "date")\
                  .agg(avg("avg_pm25").alias("daily_avg_pm25"))

# Hourly Aggregation
hourly_df = raw_df.withColumn("hour", hour("datetime"))\
                   .groupBy("location", "hour")\
                   .agg(avg("avg_pm25").alias("hourly_avg_pm25"))

# Rolling Average and Lag Features
rolling_window = Window.partitionBy("location").orderBy("datetime").rowsBetween(-2, 0)
lag_window = Window.partitionBy("location").orderBy("datetime")

raw_df = raw_df.withColumn("rolling_avg_pm25", avg("avg_pm25").over(rolling_window))
raw_df = raw_df.withColumn("lag1_pm25", lag("avg_pm25", 1).over(lag_window))
raw_df = raw_df.withColumn("rate_of_change_pm25", (col("avg_pm25") - col("lag1_pm25")))

# Rename datetime -> timestamp for final output
raw_df = raw_df.withColumnRenamed("datetime", "timestamp")

# Add constant columns: parameter and units
raw_df = raw_df.withColumn("parameter", lit("pm25"))
raw_df = raw_df.withColumn("units", lit("ug/m3"))

# Add day_of_week and hour_of_day
raw_df = raw_df.withColumn("day_of_week", dayofweek("timestamp"))
raw_df = raw_df.withColumn("hour_of_day", hour("timestamp"))

# Add pm25_x_humidity feature
raw_df = raw_df.withColumn("pm25_x_humidity", col("avg_pm25") * col("humidity"))

# Show sample output
raw_df.select("location", "timestamp", "parameter", "units", "avg_pm25", "temperature", "humidity", "rolling_avg_pm25", "day_of_week", "hour_of_day", "pm25_x_humidity").show(5)

# Save outputs as SINGLE FILE each
raw_df.coalesce(1).write.mode("overwrite").csv("Output/Section2_enhanced_raw", header=True)
daily_df.coalesce(1).write.mode("overwrite").csv("Output/Section2_daily_aggregation", header=True)
hourly_df.coalesce(1).write.mode("overwrite").csv("Output/Section2_hourly_aggregation", header=True)

