import paho.mqtt.client as mqtt

client = mqtt.Client()
client.connect("ip", 1883, 60)

while True:
    client.publish("test_topic", 'test message')
