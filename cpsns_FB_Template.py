import math
from paho.mqtt.client import Client as MQTTClient
#from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import time
import queue
import argparse
import json
import sys
import os
import uuid
import re
import numpy as np

PRIVATE_CONFIG_FILE_DEFAULT = "private_config.json" # locate this file in the private folder and chmod 600 it.
PUBLIC_CONFIG_FILE_DEFAULT = "public_config.json"   # this file can be located in a public folder 

bReading = False
bWriting = False

# Replaces the subtopics of the topic by the strings in the list
def replace_subtopics(topic, replacements):
    subtopics = topic.split('/')
    for i in range(min(len(subtopics), len(replacements))):
        if replacements[i]:
            subtopics[i] = replacements[i]
    return '/'.join(subtopics)

def on_publish(client, userdata, mid):
    print(f"Message {mid} published.")

def on_connect_in(mqttc_in, userdata, flags, rc, properties=None):
    global json_config_public
    print("MQTT_IN: Connected with response code %s" % rc)
    for topic in json_config_public["MQTT_IN"]["TopicsToSubscribe"]:
        print(f"MQTT_IN: Subscribing to the topic {topic}...")
        mqttc_in.subscribe(topic, qos=json_config_public["MQTT_IN"]["QoS"])

def on_connect_out(mqttc_in, userdata, flags, rc, properties=None):
    print("MQTT_OUT: Connected with response code %s" % rc)

def on_subscribe(self, mqttc_in, userdata, msg):
    print("Subscribed. Message: " + str(msg))

def on_message(client, userdata, msg):
    global bReading, bWriting

    print(f"Topic: {msg.topic}\nPayload:\n{msg.payload}")

    while bReading:        
        time.sleep(0.01) # make the thread sleep
    bWriting = True

    # do something...

    bWriting = False



def main():
    global json_config_private, json_config_public
    global bReading, bWriting

    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="Write the description here...")
    parser.add_argument('--config_private', type=str, help='Specify the JSON configuration file for PRIVATE data. Defaults to ' + PRIVATE_CONFIG_FILE_DEFAULT, default=PRIVATE_CONFIG_FILE_DEFAULT)
    parser.add_argument('--config_public', type=str, help='Specify the JSON configuration file for PUBLIC data. Defaults to ' + PUBLIC_CONFIG_FILE_DEFAULT, default=PUBLIC_CONFIG_FILE_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    # Name of the configuration file
    strConfigFile = args.config_private
    # Read the configuration file
    print(f"Reading private configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_private = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # Name of the configuration file
    strConfigFile = args.config_public
    # Read the configuration file
    print(f"Reading public configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_public = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # MQTT_IN stuff
    #mqttc_in = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    mqttc_in = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_IN"]["userId"] != "":
        mqttc_in.username_pw_set(json_config_private["MQTT_IN"]["userId"], json_config_private["MQTT_IN"]["password"])

    mqttc_in.on_connect = on_connect_in
    mqttc_in.on_message = on_message
    mqttc_in.on_subscribe = on_subscribe
    mqttc_in.connect(json_config_private["MQTT_IN"]["host"], json_config_private["MQTT_IN"]["port"], 60) # we subscribe to the topics in on_connect callback

    mqttc_in.loop_start()
    # MQTT_IN done

    # MQTT_OUT stuff
    #mqttc_out = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    mqttc_out = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_OUT"]["userId"] != "":
        mqttc_out.username_pw_set(json_config_private["MQTT_OUT"]["userId"], json_config_private["MQTT_OUT"]["password"])

    mqttc_out.on_connect = on_connect_out
    mqttc_out.on_publish = on_publish
    mqttc_out.connect(json_config_private["MQTT_OUT"]["host"], json_config_private["MQTT_OUT"]["port"], 60) 

    mqttc_out.loop_start()
    # MQTT_OUT done

    while True:
        time.sleep(0.1)

        while bWriting:        
            time.sleep(0.01) # make the thread sleep
        bReading = True

        # Do something 

        bReading = False

if __name__ == "__main__":
    main()
