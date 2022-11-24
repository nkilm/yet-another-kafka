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
    else:
        return {"msg": "Method not allowed, only GET method are allowed."}


@app.route("/topic/<topic>", methods=["GET", "POST"])
def Communication(topic):
    data: dict = None

    if request.method == "GET":
        # CONSUMER

        if data is None:
            return {"is_empty": True}

        return data

    elif request.method == "POST":
        # PRODUCER

        data: dict = request.json

        if not topic_exists(topic):
            # create a new topic
            os.mkdir(f"{here}\\topics\\{topic}")
            msg = "topic created successfully"
            # store the messages published to the topic as partitions
        else:
            msg = "topic exists"
            # read the topic from partitions

        return {"msg": msg}

    else:
        return {"msg": "Method not allowed, only GET and POST are allowed."}


if __name__ == "__main__":
    app.run(debug=True, port=LEADER_PORT)