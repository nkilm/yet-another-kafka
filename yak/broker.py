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
import sys
import pathlib

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


@app.route("/topic/<topic>", methods=["GET", "POST"])
def broker_communication(topic):
    data: dict = None

    if request.method == "GET":
        # CONSUMER

        if not topic_exists(topic):
            # create a new topic if not exists
            os.mkdir(f"{here}\\topics\\{topic}")

        if data is None:
            return {"is_empty": True}
        return data

    if request.method == "POST":
        # PRODUCER

        try:
            data: dict = request.json
            topic_path: str = f"{here}\\topics\\{topic}"

            if not topic_exists(topic):
                # create a new topic
                os.mkdir(topic_path)

            # store the messages published to the topic
            info: dict = request.get_json()
            print(info)

            with open(f"{topic_path}\\{topic}.txt", "a") as f:
                f.write(info.get("msg") + "\n")

            return {"ack": 1}

        except Exception as e:
            return {"ack": 0, "error": e}


if __name__ == "__main__":
    app.run(debug=True, port=LEADER_PORT)
