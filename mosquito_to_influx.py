import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json
import os
from dotenv import load_dotenv

# Load environment variables from local.env file
load_dotenv("local.env")

# Get MQTT configuration from environment variables
mqtt_broker = os.getenv("MQTT_BROKER", "localhost")
mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
mqtt_username = os.getenv("MQTT_USERNAME", "default")
mqtt_password = os.getenv("MQTT_PASSWORD", "default")
mqtt_topic = "ruuvi/gateway/#"

influx_client = InfluxDBClient(host='127.0.0.1', port=8086, database='ruuvi_data')

def on_connect(client, userdata, flags, rc):
  print("Connected with the result code " + str(rc))
  client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
  try:
    data = json.loads(msg.payload.decode())
    print(data)
    json_body = [
      {
        "measurement": data['id'],
        "fields": {
          "temperature": data['temperature'],
          "humidity": data['humidity'],
          "pressure": data['pressure']
        }
      }]
    influx_client.write_points(json_body)
    print("Data written to DB:")
    print(data)

  except Exception as e:
    print("Error in processing the message: ")
    print(e)


client = mqtt.Client()
client.username_pw_set(mqtt_username, mqtt_password)
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqtt_broker, mqtt_port, 60)
client.loop_forever()


