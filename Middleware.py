import paho.mqtt.publish as publish
from flask import Flask, request

app = Flask(__name__)


@app.route('/', methods=['POST'])
def status_handler():
    print("recieved a status")
    payload = request.json

    # format payload as required for mqtt

    message = payload     # formatted message as a string
    publish_to_mqtt(message)
    return "hello"


def publish_to_mqtt(message):
    publish.single("ii22/cpa/telemetry/1", message, qos=0, retain=False, hostname="broker.mqttdashboard.com", port=1883)



if __name__ == '__main__':
    print("****************")
    app.run()
