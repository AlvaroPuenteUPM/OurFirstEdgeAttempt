""" # Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import asyncio
import sys
import signal
import threading
from azure.iot.device.aio import IoTHubModuleClient


# Event indicating client stop
stop_event = threading.Event()


def create_client():
    client = IoTHubModuleClient.create_from_edge_environment()

    # Define function for handling received messages
    async def receive_message_handler(message):
        # NOTE: This function only handles messages sent to "input1".
        # Messages sent to other inputs, or to the default, will be discarded
        if message.input_name == "input1":
            print("the data in the message received on input1 was ")
            print(message.data)
            print("custom properties are")
            print(message.custom_properties)
            print("forwarding mesage to output1")
            await client.send_message_to_output(message, "output1")

    try:
        # Set handler on the client
        client.on_message_received = receive_message_handler
    except:
        # Cleanup if failure occurs
        client.shutdown()
        raise

    return client


async def run_sample(client):
    # Customize this coroutine to do whatever tasks the module initiates
    # e.g. sending messages
    while True:
        await asyncio.sleep(1000)


def main():
    if not sys.version >= "3.5.3":
        raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
    print ( "IoT Hub Client for Python" )

    # NOTE: Client is implicitly connected due to the handler being set on it
    client = create_client()

    # Define a handler to cleanup when module is is terminated by Edge
    def module_termination_handler(signal, frame):
        print ("IoTHubClient sample stopped by Edge")
        stop_event.set()

    # Set the Edge termination handler
    signal.signal(signal.SIGTERM, module_termination_handler)

    # Run the sample
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_sample(client))
    except Exception as e:
        print("Unexpected error %s " % e)
        raise
    finally:
        print("Shutting down IoT Hub Client...")
        loop.run_until_complete(client.shutdown())
        loop.close()


if __name__ == "__main__":
    main()
 """
 
import csv
import json
import time
import os
import paho.mqtt.client as mqtt

# Connection parameters
BROKER_ADDRESS = "10.0.0.4"
BROKER_PORT = 1883
BROKER_USERNAME = ""
BROKER_PASSWORD = ""
CLIENT_ID = "car"
TLS_ENABLED = False
# MQTT topic
TOPIC = "car-data"

# Get the current working directory
current_dir = os.getcwd()
# Specify the path to the 'Data' folder
data_path = os.path.join(current_dir, 'Data')
# Speify the filename
# filename = os.path.join(data_path, 'car.csv')
filename = 'car.csv'



# Function in charge of putting the data in json format
def create_json(time, latitude, longitude, distance, speed, steeringAngle, engineRpm, lateralGs, accelerationGs):
    # Append the data to a list of dictionaries
    data = {
        'time': time,
        'latitude': latitude,
        'longitude': longitude,
        'distance': distance,
        'speed': speed,
        'steeringAngle': steeringAngle,
        'engineRpm': engineRpm,
        'lateralGs': lateralGs,
        'accelerationGs': accelerationGs 
    }
    # Transform dictionary to json
    return json.dumps(data, indent=4)

# Collect simulated data
def simulate_data():
    # Open file if it's not already open
    if not hasattr(simulate_data, "csv_file"):
        simulate_data.csv_file = open(filename, 'r')
        simulate_data.csvReader = csv.DictReader(simulate_data.csv_file)
    try:
        # Read the next row
        row = next(simulate_data.csvReader)
    except StopIteration:
        # If we have reached the end of the file, close it and start over
        simulate_data.csv_file.close()
        simulate_data.csv_file = open(filename, 'r')
        simulate_data.csvReader = csv.DictReader(simulate_data.csv_file)
        row = next(simulate_data.csvReader)
    # Collect data
    time = row['Time']
    latitude = float(row['Latitude / Y Position (Degrees)'])
    longitude = float(row['Longitude / X Position (Degrees)'])
    distance = float(row['Distance (Miles)'])
    speed = float(row['Speed (MPH)'])
    steeringAngle = float(row['Steering Angle'])
    engineRpm = float(row['Engine RPM'])
    lateralGs = float(row['Lateral Gs'])
    accelerationGs = float(row['Acceleration Gs'])
    # Return data in json format
    return create_json(time, latitude, longitude, distance,
                        speed, steeringAngle, engineRpm, lateralGs, accelerationGs)
    
# Callback to check if connected
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker.")
    else:
        print(f"Connection to MQTT broker failed: {rc}")

# Callback to check if published
def on_publish(client, userdata, mid):
    print(f"Car data published to topic {TOPIC}")

# Function that send data to the edge broker
def send_data(data):
   # MQTT client
    client = mqtt.Client(client_id=CLIENT_ID)
    if TLS_ENABLED:
        client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
    # Establish callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish
    # Set authentication if needed
    if BROKER_USERNAME and BROKER_PASSWORD:
        client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)
    # Connect to the broker
    client.connect(BROKER_ADDRESS, BROKER_PORT)
    # Publish message
    client.publish(TOPIC, data, qos=1)
    # Debug info
    print(f"Sending data: {data}")

# Main function
if __name__ == "__main__":
    while True:
        # Collect data
        data = simulate_data()
        # Send the data collected to our edge device
        send_data(data)
        # Wait 2 seconds until we receive another round of data
        time.sleep(2)
