import os
import shutil
import time
import csv
import socket
import json

# Configuration
TCP_HOST = 'localhost'
TCP_PORT = 9996

LOG_FOLDER_PATH = '/workspaces/air_quality_analysis_spark/ingestion/data/pending'
PROCESSED_FOLDER_PATH = '/workspaces/air_quality_analysis_spark/ingestion/data/processed'

def process_csv_line(line):
    """Parse a single CSV line and convert to dictionary."""
    fields = line.strip().split(',')
    if len(fields) != 9:
        return None
        
    return {
        "location_id": fields[0].strip(),
        "sensors_id": fields[1].strip(),
        "location": fields[2].strip(),
        "datetime": fields[3].strip(),
        "lat": float(fields[4].strip()),
        "lon": float(fields[5].strip()),
        "parameter": fields[6].strip(),
        "units": fields[7].strip(),
        "value": float(fields[8].strip())
    }

def send_log_files(client_socket):
    """Send all pending log files line-by-line over TCP."""
    files = os.listdir(LOG_FOLDER_PATH)
    files.sort()  # Process files in consistent order

    for file in files:
        file_path = os.path.join(LOG_FOLDER_PATH, file)
        if not file.endswith('.csv') or not os.path.isfile(file_path):
            continue
            
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header if exists
            
            for row in reader:
                if len(row) != 9:
                    continue
                    
                try:
                    data = {
                        "location_id": row[0].strip(),
                        "sensors_id": row[1].strip(),
                        "location": row[2].strip(),
                        "datetime": row[3].strip(),
                        "lat": float(row[4].strip()),
                        "lon": float(row[5].strip()),
                        "parameter": row[6].strip(),
                        "units": row[7].strip(),
                        "value": float(row[8].strip())
                    }
                    client_socket.send((json.dumps(data) + '\n').encode('utf-8'))
                    print(f"📤 Sent: {row}")
                    time.sleep(0.1)  # Add slight delay to prevent overwhelming
                except (ValueError, IndexError) as e:
                    print(f"Error processing row: {row} - {e}")
                    continue

        # After sending, move the file to 'processed'
        shutil.move(file_path, os.path.join(PROCESSED_FOLDER_PATH, file))
        print(f"Moved {file} to processed/")

def start_tcp_server():
    """Start the TCP server."""
    os.makedirs(PROCESSED_FOLDER_PATH, exist_ok=True)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((TCP_HOST, TCP_PORT))
    server_socket.listen(1)
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")

    while True:
        print("⏳ Waiting for client to connect...")
        try:
            client_socket, addr = server_socket.accept()
            print(f"Client connected: {addr}")
            
            try:
                send_log_files(client_socket)
            except BrokenPipeError:
                print("Client disconnected unexpectedly")
            except Exception as e:
                print(f"Error during sending logs: {e}")
            finally:
                client_socket.close()
                print("🔌 Client disconnected.")
                
        except KeyboardInterrupt:
            print("\nShutting down server...")
            server_socket.close()
            break
        except Exception as e:
            print(f"Server error: {e}")
            continue

if __name__ == '__main__':
    start_tcp_server()