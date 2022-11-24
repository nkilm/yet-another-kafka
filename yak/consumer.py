#!/usr/bin/env python3

""" 
- Consumer consumes the messages from the topic 
- if --from-beginning is given before starting the consumer process then 
  all the messages which have been sent to the topic previously should be consumed


"""


class Consumer:
    """YAK class to consume messages published to a topic"""

    count = 0

    def __init__(self) -> None:
        Consumer.count += 1

    def __del__(self):
        """
        Destructor for YAK class Consumer.
        E.g:
        c = Consumer()

        del c # this will call destructor
        """
        Consumer.count -= 1

    @classmethod
    def get_producer_count():
        return Consumer.count

    def recv(self,topic: str, from_beginning: bool = False) -> str:
        """
        Consume message(s) from the topic.
        if from_beginning=True then all the messages which were published to the topic will be returned
        """
        pass
