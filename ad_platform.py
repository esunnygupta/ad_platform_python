#!/usr/bin/python3

import json
import paho.mqtt.client as mqtt

mqtt_broker = "3.109.20.0"

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection.")

def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed.")

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("/playlist")
    
def on_message(client, userdata, msg):
    message = json.loads(msg.payload)
    link = message["link"]
    scheduled_time = message["time"]
    print("Link:", link)
    print("Scheduled Time:", scheduled_time)
    # Start a download process if file is not avialble offline.
    # Once, file is found on local storage, start player.

def main():
    mqtt_client = mqtt.Client("python_client")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_subscribe = on_subscribe
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.connect(mqtt_broker)
    mqtt_client.loop_forever()

if __name__ == "__main__":
    main()