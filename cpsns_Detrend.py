"""
This program will
1. read CP-SENS MQTT messages, both data and metadata
2. calculate running average and remove it
3. publish the MQTT messages (to the same broker), with modified ANALYSIS:
    - /raw/ --> /detrend/ (keeping the other parts of the topic identical)
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

HOST_DEFAULT = "dtl-server-2.st.lab.au.dk"
PORT_DEFAULT = 8090
USERNAME_DEFAULT = "hbk1"
PASSWORD_DEFAULT = "hbk1shffd"
MQTT_TOPIC_DEFAULT = "cpsens/+/+/+/+/raw/+"

myDict = {}
strMQTTTopic = MQTT_TOPIC_DEFAULT

mqttc = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)


def on_connect(mqttc, userdata, flags, rc, properties=None):
    print("connected with response code %s" % rc)
    mqttc.subscribe(strMQTTTopic)


def on_subscribe(self, mqttc, userdata, msg, granted_qos):
    print("mid/response = " + str(msg) + " / " + str(granted_qos))


def on_message(client, userdata, msg):
    topic = msg.topic
    # print(topic)
    substrings = topic.split('/')
    bIsMetadata = True
    if substrings[6] == "data":
        bIsMetadata = False
    elif substrings[6] == "metadata":
        bIsMetadata = True
    else:
        raise Exception("Unknown topic" + substrings[6])
    
    # Create a tuple made of the topic string without the last element (data/metadata)
    myKey = tuple(substrings[:-1])

    global myDict
    if bIsMetadata:
        # Process JSON metadata
        # Add the key to the dictionary
        if myKey not in myDict:
            # Parse the payload
            json_metadata = json.loads(msg.payload)
            nSamples = json_metadata['Data']['Samples']
            cType = json_metadata['Data']['Type'][0]
            # Modify the topic
            newMetadataTopic = topic.replace("/" + substrings[5] + "/", "/detrend/")
            newDataTopic = topic.replace("/" + substrings[5] + "/", "/detrend/").replace("/metadata", "/data")
            # Modify the metadata: add to the analysis section
            newAnalysis = {"Name": "Detrend", "Output": "detrended"}
            json_metadata["Analysis chain"].append(newAnalysis)
            json_metadata_str = json.dumps(json_metadata, indent=4)
            # Append the updated metadate to the dictionary
            #                0         1      2                  3                 4             5            6
            myDict[myKey] = [nSamples, cType, json_metadata_str, newMetadataTopic, newDataTopic, np.int64(0), 0.0]
        if myKey in myDict:
            # Publish it!
            mqttc.publish(myDict[myKey][3], myDict[myKey][2])
    else:
        if myKey in myDict:
            nSamples = myDict[myKey][0]
            cType = myDict[myKey][1]
            # Parse the payload
            payload = msg.payload
            descriptorLength, metadataVer = struct.unpack_from('HH', payload)
            strBinFormat = str(nSamples) + str(cType)  # e.g., '640f' for 640 floats
            data = np.array(struct.unpack_from(strBinFormat, payload, descriptorLength), dtype=float)
            # detrend:
            # TODO: replace the math by something smarter...
            K_minus_1 = myDict[myKey][5]
            if K_minus_1 == 0:
                # Very first time
                xMean_K_minus_1 = data[0]
                K = np.int64(2)
                startFrom = 1
            else:
                # Not very first time
                xMean_K_minus_1 = myDict[myKey][6]
                K = K_minus_1 + 1
                startFrom = 0

            for i in range(startFrom, len(data)):
                xMean_K = xMean_K_minus_1 + (data[i] - xMean_K_minus_1) / K
                # detrend in place
                data[i] -= xMean_K
                # update
                xMean_K_minus_1 = xMean_K
                K += 1
            """
            if K_minus_1 == 0:
                # Very first time
                xMean_K_minus_1 = data[0]
                K = np.int64(2)
                for i in range(1, len(data)):
                    xMean_K = xMean_K_minus_1 + (data[i] - xMean_K_minus_1) / K
                    # detrend in place
                    data[i] -= xMean_K
                    # update
                    xMean_K_minus_1 = xMean_K
                    K += 1
            else:
                # Not very first time
                xMean_K_minus_1 = myDict[myKey][6]
                K = K_minus_1 + 1
                for i in range(len(data)):
                    xMean_K = xMean_K_minus_1 + (data[i] - xMean_K_minus_1) / K
                    # detrend in place
                    data[i] -= xMean_K
                    # update
                    xMean_K_minus_1 = xMean_K
                    K += 1
            """
            # Update
            K -= 1
            myDict[myKey][5] = K
            myDict[myKey][6] = xMean_K
            # Form the payload
            if cType == 'f':
                data = data.astype(np.float32)
            newPayload = payload[0:descriptorLength] + data.tobytes()
            # Publish
            mqttc.publish(myDict[myKey][4], newPayload)
        else:
            print("Waiting for the metadata...")


def main():
    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="This Python script reads the time data from MQTT and outputs it on a life graph.")
    parser.add_argument('--host', type=str, help='Specify the host to connect to. Defaults to ' + HOST_DEFAULT, default=HOST_DEFAULT)
    parser.add_argument('--port', type=int, help='Connect to the port specified. Defaults to ' + str(PORT_DEFAULT), default=PORT_DEFAULT)
    parser.add_argument('--username', type=str, help='Provide a username to be used for authenticating with the broker. See also the --pw argument. Defaults to ' + USERNAME_DEFAULT, default=USERNAME_DEFAULT)
    parser.add_argument('--pw', type=str, help='Provide a password to be used for authenticating with the broker. See also the --username option. Defaults to ' + PASSWORD_DEFAULT, default=PASSWORD_DEFAULT)
    parser.add_argument('--topic', type=str, help='The topic parameter. Defaults to ' + MQTT_TOPIC_DEFAULT, default=MQTT_TOPIC_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    global strMQTTTopic
    strMQTTTopic = args.topic

    # Set username and password
    mqttc.username_pw_set(args.username, args.pw)

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_subscribe = on_subscribe
    mqttc.connect(args.host, args.port, 60)

    mqttc.loop_start()

    while True:
        # Sleep for a short time to reduce CPU usage
        time.sleep(0.1)
        pass


if __name__ == "__main__":
    main()
