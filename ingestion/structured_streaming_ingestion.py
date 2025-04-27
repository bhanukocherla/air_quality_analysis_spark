from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, to_timestamp, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# TCP Configuration
TCP_HOST = 'localhost'
TCP_PORT = 9996

# Output and Checkpoint Paths (CHANGED FOR CSV)
OUTPUT_PATH = 'Output/Section1_output_csv'  # Changed to csv-specific path
CHECKPOINT_PATH = 'Output/Section1_checkpoint_csv'  # Separate checkpoint for CSV

def define_schema():
    """Define schema for the incoming data"""
    return StructType([
        StructField("location_id", StringType()),
        StructField("sensors_id", StringType()),
        StructField("location", StringType()),
        StructField("datetime", TimestampType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("parameter", StringType()),
        StructField("units", StringType()),
        StructField("value", DoubleType())
    ])

def start_spark_streaming():
    """Start Spark Structured Streaming with CSV output."""
    spark = SparkSession.builder \
        .appName("AirQualityStructuredStreamingCSV") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read data from TCP socket
    raw_df = spark.readStream \
        .format("socket") \
        .option("host", TCP_HOST) \
        .option("port", TCP_PORT) \
        .option("includeTimestamp", True) \
        .load()

    # Parse the data
    parsed_df = raw_df.select(
        from_json(
            regexp_replace(col("value"), r"[\[\]']", ""), 
            define_schema()
        ).alias("data")
    ).select("data.*")

    # Filter and process
    filtered_df = parsed_df.filter(
        (col("parameter") == "pm25") &
        (col("value").isNotNull())
    ).withWatermark("datetime", "1 hour")

    aggregated_df = filtered_df.groupBy("location", "datetime").agg(
        avg("value").alias("avg_pm25")
    )

    # Write to CSV (MODIFIED SECTION)
    query = aggregated_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("header", "true") \
        .option("truncate", False) \
        .start()

    # Add console output for debugging
    console_query = aggregated_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    print("Spark CSV Streaming started. Press Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    start_spark_streaming()