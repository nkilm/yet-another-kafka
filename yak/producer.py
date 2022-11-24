#!/usr/bin/env python3

""" 
- Producer produces/publishes the messages to the topics
- Number of producers should be dynamic

"""


class Producer:
    """YAK class to publish messages to particular topic"""

    count = 0

    def __init__(self) -> None:
        Producer.count += 1

    def __del__(self):
        """
        Destructor for YAK class Producer.

        E.g:
        p = Producer()

        del p # this will call destructor
        """
        Producer.count -= 1

    @classmethod
    def get_producer_count():
        return Producer.count

    def send(self,topic: str, msg: str, from_beginning: bool = False):
        """
        Publish message(s) to the topic.
        if from_beginning=True then all the messages which were published to the topic will be returned
        """
        pass

    def clean_topic(topic: str):
        pass
