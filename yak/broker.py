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

"""