"""
This program will
1. read CP-SENS MQTT messages, both data and metadata
2. perform integrate the data twice (normal use: integrate acceleration to velocity and/or displacement)
3. publish the MQTT messages (to the same broker), with modified PHYSICS:
    - /acc/ --> /vel/ (keeping the other parts of the topic identical)
    - /acc/ --> /displ/ (keeping the other parts of the topic identical)
    - metadata will get extended the ANALYSIS CHAIN section
"""
import numpy as np
from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import argparse
import struct
import time
import json
import copy
import Integration_KF_Chatzi as intgr
import sys
import os

# Default configuration files
PRIVATE_CONFIG_FILE_DEFAULT = "private_config.json" # locate this file in the private folder and chmod 600 it.
PUBLIC_CONFIG_FILE_DEFAULT = "public_config.json"   # this file can be located in a public folder 

myDict = {}

# Replaces the subtopics of the topic by the strings in the list
def replace_subtopics(topic, replacements):
    subtopics = topic.split('/')
    for i in range(min(len(subtopics), len(replacements))):
        if replacements[i]:
            subtopics[i] = replacements[i]
    return '/'.join(subtopics)

def on_connect_in(mqttc_in, userdata, flags, rc, properties=None):
    global json_config_public
    print("MQTT_IN: Connected with response code %s" % rc)
    for topic in json_config_public["MQTT_IN"]["TopicsToSubscribe"]:
        print(f"MQTT_IN: Subscribing to the topic {topic}...")
        mqttc_in.subscribe(topic, qos=json_config_public["MQTT_IN"]["QoS"])

def on_connect_out(mqttc_in, userdata, flags, rc, properties=None):
    print("MQTT_OUT: Connected with response code %s" % rc)

def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("Subscribed. Message: " + str(msg))

def on_message(client, userdata, msg):
    global myDict, mqttc_out 
    topic = msg.topic
    substrings = topic.split('/')
    bIsMetadata = True
    if substrings[6] == "data":
        bIsMetadata = False
    elif substrings[6] == "metadata":
        bIsMetadata = True
    else:
        raise Exception("Unknown topic" + substrings[6])
    
    # Initial values
    v0 = 0.0
    d0 = 0.0
    P0 = np.eye(2)
    # Covariances
    # TODO: take from the command line
    Q = 1.e-6
    R = 1.e-10   # % Q/R=10 Nice and smooth but the magnitude is smaller

    # Create a tuple made of the topic string without the last element (data/metadata)
    myKey = tuple(substrings[:-1])

    if bIsMetadata:
        # Process JSON metadata
        # Add the key to the dictionary
        if myKey not in myDict:
            # Parse the payload
            json_metadata = json.loads(msg.payload)
            nSamples = json_metadata['Data']['Samples']
            cType = json_metadata['Data']['Type'][0]
            Ts = 1.0 / json_metadata["Analysis chain"][0]["Sampling"]
            # Modify the topic
            newMetadataTopic = topic.replace("/" + substrings[4] + "/", "/displ/")
            newDataTopic = topic.replace("/" + substrings[4] + "/", "/displ/").replace("/metadata", "/data")
            # Make a deep copy of the last element of the Analysis chain:
            lastAnalysisInChain = json_metadata["Analysis chain"][-1]
            lastAnalysisInChain_copy = copy.deepcopy(lastAnalysisInChain)
            lastAnalysisInChain_copy["Name"] = "Integration"
            lastAnalysisInChain_copy["Output"] = "Displacement"
            json_metadata["Analysis chain"].append(lastAnalysisInChain_copy)
            # Modify Units in the Data section
            json_metadata["Data"]["Unit"] = "m"
            json_metadata_str = json.dumps(json_metadata, indent=4)
            # Append the updated metadate to the dictionary
            #                0         1      2                  3                 4             5   6   7   8
            myDict[myKey] = [nSamples, cType, json_metadata_str, newMetadataTopic, newDataTopic, d0, v0, P0, Ts]
        if myKey in myDict:
            # Publish it!
            print(f"Publish {myDict[myKey][3]}...")
            mqttc_out.publish(myDict[myKey][3], myDict[myKey][2])
    else: # data message
        if myKey in myDict:
            # Parse the payload
            payload = msg.payload
            descriptorLength, metadataVer = struct.unpack_from('HH', payload)
            # how many samples and what's its type, float or double?
            cType = myDict[myKey][1]
            nSamples = myDict[myKey][0]
            if nSamples == -1: # unknown or variable
                # calculate nSamples from the payload length
                payload_len = len(payload)
                nSamples = round((payload_len-descriptorLength)/struct.calcsize(cType))
            # Data
            strBinFormat = str(nSamples) + str(cType)  # e.g., '640f' for 640 floats
            # data
            data = np.array(struct.unpack_from(strBinFormat, payload, descriptorLength))
            '''
            # time stamp of the payload
            secFromEpoch = struct.unpack_from('Q', payload, 4)[0]
            nanosec = struct.unpack_from('Q', payload, 12)[0]
            # nSamples
            nSamplesFromDAQStart = 0
            if metadataVer >= 2:
                nSamplesFromDAQStart = struct.unpack_from('Q', payload, 20)[0]
            else:
                raise Exception("Incompatible version. Use earlier version of cpsns_LifePlot!")
            '''
            # Integrate
            Ts = myDict[myKey][8]
            d, v, P = intgr.Integration_KF_Chatzi(data, Ts, Q, R, myDict[myKey][5], myDict[myKey][6], myDict[myKey][7])  # d, v, P = Integration_KF_Chatzi(a, Ts, Q, R, d0=0, v0=0, P0=np.eye(2))
            # Update. MATLAB: d0_1 = d(end); v0_1 = v(end); P0_1 = P;
            myDict[myKey][5] = d[-1]
            myDict[myKey][6] = v[-1]
            myDict[myKey][7] = P
            # Form the payload
            if cType == 'f':
                d = d.astype(np.float32)
            newPayload = payload[0:descriptorLength] + d.tobytes()
            # Publish
            mqttc_out.publish(myDict[myKey][4], newPayload)
        else:
            print("Waiting for the metadata...")


def main():
    global mqttc_out 
    global json_config_public
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
    mqttc_in = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    #mqttc_in = MQTTClient()

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
    mqttc_out = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    #mqttc_out = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_OUT"]["userId"] != "":
        mqttc_out.username_pw_set(json_config_private["MQTT_OUT"]["userId"], json_config_private["MQTT_OUT"]["password"])

    mqttc_out.on_connect = on_connect_out
    mqttc_out.connect(json_config_private["MQTT_OUT"]["host"], json_config_private["MQTT_OUT"]["port"], 60) 

    mqttc_out.loop_start()
    # MQTT_OUT done

    while True:
        # Sleep for a short time to reduce CPU usage
        time.sleep(0.1)
        pass


if __name__ == "__main__":
    main()
