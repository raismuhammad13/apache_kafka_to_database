from kafka import KafkaConsumer 
import json
import os
import time
from datetime import timedelta, datetime
import pyodbc
import concurrent.futures
from dotenv import find_dotenv, load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

# Kafka Environment variables
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC")
GPS_TOPIC = os.getenv("GPS_TOPIC")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC")

# SQL Server database environment variables
server = os.getenv("server")
database = os.getenv("database")
username = os.getenv("username")
password = os.getenv("password")
driver = os.getenv("driver")

# Function to setup the consumer
def topic_kafka_consumer(topic, server, group_id):
    # logger.info(f"Setting up Kafka consumer for topic: {topic}")
    return KafkaConsumer(
            topic,
            bootstrap_servers = server,
            auto_offset_reset="earliest",
            enable_auto_commit = True,
            group_id = group_id,
            value_deserializer = lambda x: json.loads(x.decode('utf-8'))
            )

    
# Connection with MS SQL Server
def mssql_connection(driver, server, database, username, password):
    try:
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        connection = pyodbc.connect(conn_str)
        # logger.info("Database connection successful.")
        return connection
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

# Generating the dynamic query according to the data in the kafka topic
def generate_query(table, data, columns):
    placeholders = placeholders = ", ".join(["?" for _ in data.keys()])
    query = f"""
        INSERT INTO {table} ({columns}) VALUES ({placeholders})
            """
    return query

# Insert function. Insert the data into the MSSQL Server tables
def insert_data(table, query, data):
    connection = mssql_connection(driver, server, database, username, password)
    if connection is None:
        logger.error("Skipping data insertion due to failed database connection.")
        return
    try:     
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        logger.info(f"Data inserted successfully into table {table}")
    except pyodbc.Error as e:
        logger.error(f"Error inserting data into table {table}: {e}")
    finally:
        cursor.close()
        connection.close()

# Function that gets the data from the kafka consumer and retrieve the required data 
def topic_data_extraction(topics_data):
    for message in topics_data:
        # Extracting the topic name as a table to insert into sql db
        table = message.topic.strip()
        # The actual data from the consumer
        topic_data = message.value
        
        # Converting the loaction to string for ease while inserting into database table
        if 'location' in topic_data:
            topic_data['location'] = f"{topic_data['location'][0]},{topic_data['location'][1]}"

        # Extraction of the keys of json formated data as columns
        columns = tuple(topic_data.keys())
        columns = ', '.join(columns)
        # Actual data as a tuple
        data = tuple(topic_data.values())

        # Insert query for inserting the data
        query = generate_query(table, topic_data, columns)
        insert_data(table, query, data) 


def main():
    # Extraction the data from each kafka topic
    vehicle_data = topic_kafka_consumer(VEHICLE_TOPIC, KAFKA_BOOTSTRAP_SERVER, "vehicle_data")
    gps_data = topic_kafka_consumer(GPS_TOPIC, KAFKA_BOOTSTRAP_SERVER, "gps_data")
    traffic_camera_data = topic_kafka_consumer(TRAFFIC_TOPIC, KAFKA_BOOTSTRAP_SERVER, "traffic_camera_data")
    weather_data = topic_kafka_consumer(WEATHER_TOPIC, KAFKA_BOOTSTRAP_SERVER, "weather_data")
    emergency_incident_data = topic_kafka_consumer(EMERGENCY_TOPIC, KAFKA_BOOTSTRAP_SERVER, "emergency_data")

    # Tasks to run in parallel
    tasks = [
        (vehicle_data),
        (gps_data),
        (traffic_camera_data),
        (weather_data),
        (emergency_incident_data)
    ]

    # Run the tasks in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(topic_data_extraction, data) for data in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Will raise an exception if the thread had an error
            except Exception as e:
                logger.error(f"Error occurred during execution: {e}")

    time.sleep(1.5)   

if __name__=="__main__":
  main()