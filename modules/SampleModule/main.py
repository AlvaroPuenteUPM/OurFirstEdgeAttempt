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
 
import json
import json
import ssl
import paho.mqtt.client as mqtt
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message

# Connection parameters
BROKER_ADDRESS = "10.0.0.4"
BROKER_PORT = 1883
BROKER_USERNAME = ""
BROKER_PASSWORD = ""
CLIENT_ID = "receiver"
TLS_ENABLED = False
CONNECTION_STRING = "HostName=FinalAssignmentHub.azure-devices.net;DeviceId=EdgeDeviceFinalAssignment;SharedAccessKey=dSb6UMgY8OLm9hKWsRxYfWlU5hLFKdqKIZvH2226dZY="
# MQTT topic
TOPIC = "car-data"

lastAngle = 0.0

def send_message(data):
    try:
        # Client connection
        module_client = IoTHubModuleClient.create_from_connection_string(CONNECTION_STRING)
        module_client.connect()
        # Prepare message
        dataMessage = Message(data)
        # Send message
        module_client.send_message_to_output(dataMessage, "output1")
        # Print message if we suceed
        print("Message sent: {}".format(dataMessage))
    except Exception as ex:
        # Print error in the sending process
        print("Error sending alert: {}".format(ex))
        
# Some speed adaptation to european standarts      
def process_speed(speedMPH):
    # Convert miles per hour to kilometers per hour
    return float(speedMPH)*1.60934

# Some distance adaptation to european standarts
def process_distance(distanceMPH):
    # Convert miles to kilometers
    return float(distanceMPH)*1.60934
    
# Check swerves
def process_Steering(newAngle):
    global lastAngle
    # If the angle difference is greater or equal than 0.2 we assume that the driver swerved
    if abs(lastAngle - newAngle) >= 0.2:
        send_message("Driver swerved!")
    lastAngle = newAngle

# Check if the car is over-revving or under-revving
def process_rpm(rpm):
    # Check current engine revolutions
    if rpm > 3000:
        send_message("Inefficient driving: need to shift up a gear")
    elif rpm < 1500:
        send_message("Inefficient driving: need to shift down a gear")
    
# Check if a collision occurs (gs > 3)
def process_gs(lateralGs, accelerationGs):
    if (abs(lateralGs) >= 3.0 or abs(accelerationGs) >= 3.0):
        send_message("Alert: A crash has been detected. It is advisable to call the emergency services")
    
# Process the incoming data
def process_data(data):
    # Data adaptation
    newDistance = process_distance(data['distance'])
    newSpeed =  process_speed(data['speed'])
    dictionary = {'time':data['time'], 'distance':newDistance, 'speed':newSpeed, 'latitude':data['latitude'], 'longitude':data['longitude']}
    jsonString = json.dumps(dictionary, indent=4)
    send_message(jsonString)
    # Check alerts
    process_rpm(float(data['engineRpm']))
    process_Steering(float(data['steeringAngle']))
    process_gs(float(data['lateralGs']), float(data['accelerationGs']))
    
# Callback to check if connected
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker. Subscribing to topic {TOPIC}")
        client.subscribe(TOPIC)
    else:
        print(f"Connection to MQTT broker failed: {rc}")

# Callback when a message arrives
def on_message(client, userdata, message):
    # Get json message
    payload = json.loads(message.payload.decode())
    print(f"Received data: {payload}")
    # Process data
    process_data(payload)

if __name__ == "__main__":
    # MQTT client
    client = mqtt.Client(client_id=CLIENT_ID)
    # If TLS is enabled
    if TLS_ENABLED:
        client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
    # Establish callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    # Set authentication if needed
    if BROKER_USERNAME and BROKER_PASSWORD:
        client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)
    # Connect to the broker
    client.connect(BROKER_ADDRESS, BROKER_PORT)
    # Listen for new messages
    client.loop_forever()