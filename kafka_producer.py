import json
from kafka import KafkaProducer
import os
import time
import uuid
import random
from datetime import timedelta, datetime
from dotenv import find_dotenv, load_dotenv
import logging


# Configure logging###################  
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

# Coordinates of london
LONDON_COORDINATES = {
    'latitude': 51.5074,
    'longitude': -0.1278
}

# Coordinates of Birmingham
BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904
}


# Calculate the movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"]
                    - LONDON_COORDINATES["latitude"])/100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"]
                    - LONDON_COORDINATES["longitude"])/100


random.seed(42)
start_time = datetime.now()   # Vehicle Start time
start_location = LONDON_COORDINATES.copy()  # Vehicle Start Location

# Data Serializer before send the data into Kafka topics
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Generating the time
def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30,60))
    return start_time

# Generating the GPS data
def generate_gps_data(device_id, timestamp, vehicle_type = "private"):
    return {
        "id": str(uuid.uuid4()),
        "deviceID": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0,40),
        "direction": "North-Eash",
        "Vehicle_type": vehicle_type
    }

# Generating the Traffic Camera Data
def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": str(uuid.uuid4()),
        "deviceID": device_id,
        "cameraID": camera_id,
        "location": location,
        "timestamp": timestamp,
        "snapshot": "Base64EncodedString"
    }

# Generating the Weather Data
def generate_weather_data(device_id, timestamp, location):
    return {
        "id":str(uuid.uuid4()),
        "deviceID": device_id,
        "location": location,
        "timestamp": timestamp,
        "temperature": random.uniform(-5,27),
        "weatherCondition": random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        "precipitation": random.uniform(0,25),
        "windSpeed": random.uniform(0,100),
        "humidity": random.randint(0,100),
        "airQualityIndex": random.uniform(0,500)
    }

# Generating the emergency data
def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": str(uuid.uuid4()),
        "deviceID": device_id,
        "incidentID": str(uuid.uuid4()),
        "type": random.choice(["Accident", "Fire", "Medical", "Police", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Deacription of the incident"
    }


# Finding the location of vehicle
def stimulate_vehicle_movement():
    global start_location

    # Move towards birminghm
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    # Adding some randomness
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location

# Generating vehicle data
def generate_vehicle_data(device_id):
    location = stimulate_vehicle_movement()
    return {
        "id": str(uuid.uuid4()),
        "device": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location['latitude'], location['longitude']),
        "speed": random.uniform(10,40),
        "direction": "North-East",
        "make": "BMW",
        "model": "C500",
        "year": 2024,
        "fuelType": "Hybrid"
    }

# Funciton to send the data into kafka topic
def producer_data_to_kafka_topic(producer, topic, data):
    producer.send(
        topic,
        data
    )
    logger.info(f"Data sent to topic {topic}")

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data["timestamp"], vehicle_data["location"], "Canon_34312")
        weather_data = generate_weather_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])

        # Checking if the vehicle reached to its destination or not.
        if (vehicle_data["location"][0] >= BIRMINGHAM_COORDINATES["latitude"]
                and vehicle_data["location"][1] <= BIRMINGHAM_COORDINATES["longitude"]):
                logger.info("Vehicle reached Birmingham. Simulation ending.....")
                break
        
        # Producing the data into kafka topic
        producer_data_to_kafka_topic(producer, VEHICLE_TOPIC, vehicle_data)
        producer_data_to_kafka_topic(producer, GPS_TOPIC, gps_data)
        producer_data_to_kafka_topic(producer, TRAFFIC_TOPIC, traffic_camera_data)
        producer_data_to_kafka_topic(producer, WEATHER_TOPIC, weather_data)
        producer_data_to_kafka_topic(producer, EMERGENCY_TOPIC, emergency_incident_data)


        # logger.info(f"vehicle_data: {vehicle_data}")
        # logger.info(f"gps_data: {gps_data}")
        # logger.info(f"traffic_camera_data: {traffic_camera_data}")
        # logger.info(f"weather_data: {weather_data}")
        # logger.info(f"emergency_incident_data: {emergency_incident_data}")
        
        time.sleep(4)

if __name__ == "__main__":
    # Configuring the kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, #['127.0.0.1:9092'],
        value_serializer=json_serializer
    )
    try:
        simulate_journey(producer, "Vehicle_123")
    except KeyboardInterrupt:
        logger.info("Simulation ended by the user.")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")