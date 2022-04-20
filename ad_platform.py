#!/usr/bin/python3

import json
import http.client
import urllib.request
import paho.mqtt.client as mqtt
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler

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
    campaign = message["campaign"]
    # link = message["link"]
    # scheduled_time = message["time"]
    print("Campaign:", campaign)
    # print("Link:", link)
    # print("Scheduled Time:", scheduled_time)
    campaign_name = campaign + ".mp4"
    file_exists = Path(campaign_name)
    if file_exists.exists():
        # start player
        print("Invoke Player")

def schedule():
    http_connection = http.client.HTTPConnection('3.109.20.0')
    http_headers = {'Content-type': 'application/json'}
    http_connection.request('GET', '/api/campaign', None, http_headers)
    http_response = http_connection.getresponse()
    json_response = json.loads(http_response.read().decode())
    campaign_list = json_response["advertise"]
    for campaign in campaign_list:
        campaign_name = campaign["campaign_name"]
        campaign_name = campaign_name + ".mp4"
        file_exists = Path(campaign_name)
        if file_exists.exists():
            pass
        else:
            urllib.request.urlretrieve(campaign["video_link"], campaign_name)
            print("Downloaded", campaign_name)

def main():
    # Start a download process.
    scheduler = BackgroundScheduler()
    scheduler.add_job(schedule, 'interval', minutes=1)
    scheduler.start()
    try:
        # Connect to MQTT Broker
        mqtt_client = mqtt.Client("python_client")
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.on_subscribe = on_subscribe
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.connect(mqtt_broker)
        mqtt_client.loop_forever()
    except:
        scheduler.shutdown()

if __name__ == "__main__":
    main()