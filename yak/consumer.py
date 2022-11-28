#!/usr/bin/env python3

""" 
- Consumer consumes the messages from the topic 
- if --from-beginning is given before starting the consumer process then 
  all the messages which have been sent to the topic previously should be consumed


"""
import time

import requests
from constants import get_leader

LEADER_PORT = get_leader()


class Consumer:
    """YAK class to consume messages published to a topic"""

    count = 0

    def __init__(self) -> None:
        Consumer.count += 1

        self.leader_url = f"http://localhost:{LEADER_PORT}"
        # self.leader_url = f"http://localhost:7070"
        self.consumer_session = requests.session()
        a = requests.adapters.HTTPAdapter(max_retries=3)
        self.consumer_session.mount("http://", a)

    def __del__(self):
        """
        Destructor for YAK class Consumer.
        E.g:
        c = Consumer()

        del c # this will call destructor
        """
        Consumer.count -= 1

    @classmethod
    def get_producer_count(cls):
        return Consumer.count

    def recv(self, topic: str, from_beginning: bool = False) -> str:
        """
        Consume message(s) from the topic.
        if from_beginning=True then all the messages which were published to the topic will be returned
        """
        try:
            while True:
                topic_url = f"{self.leader_url}/topic/{topic}"

                response = self.consumer_session.get(topic_url)
                print(response.json())
                time.sleep(1)

        except ConnectionError:
            print("Leader is down, please retry after 10s")
        except Exception as e:
            print(e)
