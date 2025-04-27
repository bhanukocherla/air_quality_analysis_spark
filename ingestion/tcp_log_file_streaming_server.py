import os
import shutil
import time
import csv
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, split, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# TCP Configuration
TCP_HOST = 'localhost'  # The host to bind the server
TCP_PORT = 9992 # The port on which to listen for incoming connections

# Folder Paths
LOG_FOLDER_PATH = '/workspaces/air_quality_analysis_spark/ingestion/data/pending'  # Path to the folder containing log files
PROCESSED_FOLDER_PATH = os.path.join('/workspaces/air_quality_analysis_spark/ingestion/data/', 'processed')  # Subfolder where processed logs will be moved

def create_tcp_server():
    """
    Creates and returns a TCP server socket.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((TCP_HOST, TCP_PORT))  # Bind to the specified host and port
    server_socket.listen(5)  # Listen for up to 5 incoming connections
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}...")
    return server_socket

def ensure_processed_folder():
    """
    Ensures that the 'processed' subfolder exists within the log folder.
    If it doesn't exist, it will be created automatically.
    """
    os.makedirs(PROCESSED_FOLDER_PATH, exist_ok=True)  # Create 'processed' folder if not already present

def process_csv_line(line):
    """
    Parses a single CSV line into a list of values.
    
    Arguments:
    line -- The line to be parsed (CSV format).
    
    Returns:
    A list containing the parsed values from the CSV line.
    """
    csv_reader = csv.reader([line])  # Wrap the line as an iterable so it can be processed by csv.reader
    for record in csv_reader:
        return record  # Return the first (and only) record parsed from the line
    return []  # Return an empty list if parsing fails

def process_log_file(file_path, client_socket):
    """
    Processes a log file line by line, sending each line to the connected TCP client.
    After processing, the log file is moved to the 'processed' folder.
    
    Arguments:
    file_path -- The full path to the log file being processed.
    client_socket -- The connected TCP client socket.
    """
    filename = os.path.basename(file_path)  # Extract the filename from the full file path
    print(f"📄 Processing: {filename}")

    try:
        with open(file_path, 'r') as f:  # Open the log file for reading
            for line in f:
                line = line.strip()  # Strip any leading/trailing whitespace
                if line:  # Skip empty lines
                    # Parse the line into a CSV record
                    record = process_csv_line(line)

                    # If a valid record is parsed, send it to the client
                    if record:
                        send_line_to_client(client_socket, str(record))  # Convert record to a string for transmission
                    else:
                        print(f"Skipping invalid line: {line}")  # Log any invalid lines that don't parse correctly

                    time.sleep(0.01)  # Optional delay to avoid overloading the client

    except Exception as e:
        print(f"Error processing {filename}: {e}")  # Log any errors that occur while processing the file
        return

    # Move the processed file to the 'processed' folder
    dest_path = os.path.join(PROCESSED_FOLDER_PATH, filename)  # Destination path for the processed file
    shutil.move(file_path, dest_path)  # Move the file to the 'processed' folder
    print(f"Moved {filename} to 'processed/'")  # Log confirmation that the file has been moved


def send_line_to_client(client_socket, line):
    """
    Sends a single line (CSV record) to the connected TCP client.
    
    Arguments:
    client_socket -- The TCP client socket.
    line -- The line to send.
    """
    client_socket.send((line + "\n").encode('utf-8'))  # Send the line to the client as a byte string
    print(f"Sent: {line}")  # Log the sent message


def process_stream_data():
    """Main processing pipeline for streaming data"""
    # Create the Spark session
    spark = SparkSession.builder \
        .appName("AirQualityStreaming") \
        .getOrCreate()

    # Define schema for air quality data
    schema = StructType([
        StructField("location_id", StringType(), True),
        StructField("sensors_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("datetime", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("parameter", StringType(), True),
        StructField("units", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    # Read the streaming data from the TCP server
    raw_stream_df = spark \
        .readStream \
        .format("socket") \
        .option("host", TCP_HOST) \
        .option("port", TCP_PORT) \
        .load()

    # Parse and clean the incoming data
    parsed_df = raw_stream_df \
        .withColumn("clean_value", split(col("value"), ",\s*")) \
        .select(
            col("clean_value").getItem(0).alias("location_id"),
            col("clean_value").getItem(1).alias("sensors_id"),
            col("clean_value").getItem(2).alias("location"),
            to_timestamp(col("clean_value").getItem(3), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("datetime"),
            col("clean_value").getItem(4).cast("double").alias("lat"),
            col("clean_value").getItem(5).cast("double").alias("lon"),
            col("clean_value").getItem(6).alias("parameter"),
            col("clean_value").getItem(7).alias("units"),
            col("clean_value").getItem(8).cast("double").alias("value")
        ).filter(
            (col("parameter") == "pm25") & 
            col("value").isNotNull()
        )

    # Perform a simple aggregation (average PM2.5 by location)
    aggregated_df = parsed_df.groupBy("location", "datetime").agg(
        {"value": "avg"}
    ).withColumnRenamed("avg(value)", "avg_pm25")

    # Output the results to the console (can be changed to another sink like a file or DB)
    query = aggregated_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


def start_server():
    """
    Main function to start the server, accept client connections, and process log files.
    """
    ensure_processed_folder()  # Ensure the 'processed' folder exists before starting
    server_socket = create_tcp_server()  # Create and start the TCP server
    accept_data_from_client(server_socket)

def accept_data_from_client(server_socket):
    """
    Accepts incoming client connections and processes log data.
    """
    try:
        while True:
            print("Waiting for a client to connect...")
            client_socket, client_address = server_socket.accept()  # Accept a client connection
            print(f"Connected to {client_address}")

            try:
                # Process incoming log file data from the client
                process_log_file(LOG_FOLDER_PATH, client_socket)  # Specify the folder where logs are located
            except Exception as e:
                print(f"Error while processing log data: {e}")
            finally:
                client_socket.close()  # Close the client socket after processing

    except Exception as e:
        print(f"Error accepting client connections: {e}")



if __name__ == '__main__':
    # Start the server to listen for log file data from TCP
    start_server()

    # Start the Spark Structured Streaming process for real-time analysis
    process_stream_data()
