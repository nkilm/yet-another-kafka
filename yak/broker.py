#!/usr/bin/env python3

""" 
- Total 3 instances of brokers should be created
- One of the broker should be made as a leader
- Leader should maintain/create topics
- Leader should know which messages are not yet received by the consumer
- All brokers should maintain logs of all the operations

Topics 
- topics should be stored as directories/folders, 
	within those folders the message contents should be stored as partitions 
- The performance of the partition is very important

POST localhost:LEADER_PORT/<str:topic> - by producer

GET localhost:LEADER_PORT/<str:topic> - by consumer

Use heartbeats to check the status of the leader

"""
import os
import pathlib
import sys

import pika
from flask import Flask, request
from utils import topic_exists

if len(sys.argv) < 2:
    print("PORT not provided")
    sys.exit(1)

app = Flask(__name__)

LEADER_PORT = sys.argv[1]
STATUS = 200
here = pathlib.Path(__file__).parent.resolve()


@app.route("/status", methods=["GET"])
def status():

    if request.method == "GET":
        return {
            "MESSAGE": "Broker is running",
            "STATUS": STATUS,
            "LEADER_PORT": LEADER_PORT,
        }


@app.route("/topic/<topic>", methods=["POST"])
def broker_communication(topic):

    if request.method == "POST":
        # PRODUCER & CONSUMER

        try:
            topic_path: str = f"{here}\\topics\\{topic}"

            if not topic_exists(topic):
                # create a new topic
                os.mkdir(topic_path)

            # print(request.get_data(as_text=True))
            DATA: dict = request.get_json()

            if DATA.get("is_consumer") is not None:
                channel.queue_declare(queue=topic)
                print("is consumer")
                return {"ack": 1}

            channel.queue_declare(queue=topic)
            channel.basic_publish(exchange="", routing_key=topic, body=DATA.get("msg"))

            # store the messages published to the topic
            with open(f"{topic_path}\\{topic}.txt", "a") as f:
                f.write(DATA.get("msg") + "\n")

            return {"ack": 1}

        except Exception as e:
            print(e)
            return {"ack": 0, "error": e}


if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    app.run(debug=True, port=LEADER_PORT)
