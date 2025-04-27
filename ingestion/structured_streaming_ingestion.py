from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import time
import socket

# Environment Configuration
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 2g --conf spark.driver.maxResultSize=2g pyspark-shell'
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'

# Data Schema
SCHEMA = StructType([
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

TCP_HOST = 'localhost'
TCP_PORT = 9992

def create_spark_session():
    """Create optimized Spark session"""
    return SparkSession.builder \
        .appName("AirQualityStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.receiver.maxRate", "50") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.streaming.checkpointLocation", "spark_checkpoints") \
        .getOrCreate()

def parse_stream(raw_df):
    """Robust data parsing with error handling"""
    try:
        # Step 1: Clean the data
        cleaned = raw_df.withColumn("clean_value", 
            regexp_replace(col("value"), "[\[\]']", ""))
        
        # Step 2: Split into columns
        split_data = cleaned.withColumn("data", split(col("clean_value"), ",\s*"))
        
        # Step 3: Extract fields with validation
        parsed = split_data.select(
            col("data").getItem(0).alias("location_id"),
            col("data").getItem(1).alias("sensors_id"),
            col("data").getItem(2).alias("location"),
            to_timestamp(col("data").getItem(3), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("datetime"),
            col("data").getItem(4).cast("double").alias("lat"),
            col("data").getItem(5).cast("double").alias("lon"),
            col("data").getItem(6).alias("parameter"),
            col("data").getItem(7).alias("units"),
            col("data").getItem(8).cast("double").alias("value")
        ).filter(
            (col("parameter") == "pm25") & 
            col("value").isNotNull()
        )
        
        return parsed.withColumn("processing_time", lit(time.strftime("%Y-%m-%d %H:%M:%S")))
        
    except Exception as e:
        print(f"Data parsing error: {e}")
        return None

def start_tcp_server():
    """Starts the TCP server to receive data"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((TCP_HOST, TCP_PORT))
    server_socket.listen(5)
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}...")
    return server_socket

def accept_data_from_client(server_socket):
    """Accepts data from the TCP client"""
    client_socket, client_address = server_socket.accept()
    print(f"Connected to client: {client_address}")
    
    try:
        while True:
            data = client_socket.recv(1024).decode('utf-8')  # Receive data from the client
            if data:
                yield data  # Yield data to Spark Streaming for processing
            else:
                break  # No more data, client closed the connection
    except Exception as e:
        print(f"Error receiving data: {e}")
    finally:
        client_socket.close()
        print(f"Closed connection to client: {client_address}")

def main():
    # Initialize Spark with enhanced configuration
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Prepare output directories
    os.makedirs("Output/Section1_output", exist_ok=True)
    os.makedirs("Output/Section1_checkpoint", exist_ok=True)
    
    # Start TCP server
    server_socket = start_tcp_server()
    
    # Start reading data from the server using the generator
    raw_stream_rdd = spark.sparkContext.parallelize(accept_data_from_client(server_socket))

    # Convert RDD to DataFrame and apply parsing logic
    raw_stream_df = spark.readStream \
        .schema(SCHEMA) \
        .csv(raw_stream_rdd)

    # Parse the stream with error handling
    parsed_stream = parse_stream(raw_stream_df)
    
    if parsed_stream is None:
        print(" Failed to initialize stream parsing")
        return
    
    # Configure output with multiple sinks
    query = parsed_stream.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "Output/Section1_output") \
        .option("checkpointLocation", "Output/Section1_checkpoint") \
        .option("failOnDataLoss", "false") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Console output for monitoring
    console = parsed_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("🚀 Stream processing started. Monitoring at http://localhost:4040")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n Stopping streaming query...")
        query.stop()
        console.stop()

if __name__ == "__main__":
    main()
