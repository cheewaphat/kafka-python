#!/usr/bin/env python
#kafka-console-producer --broker-list "172.19.103.231:9092,172.19.103.232:9092,172.19.103.233:9092" --topic ccp-2300
#kafka-topics --zookeeper "172.19.103.231:2181,172.19.103.232:2181,172.19.103.233:2181" --list
#kafka-console-consumer  --zookeeper "172.19.103.231:2181,172.19.103.232:2181,172.19.103.233:2181" --topic ccp-2300 --from-beginning

import threading, logging, datetime,time
import multiprocessing
import json
#import csv
import os
import datetime

from kafka import KafkaConsumer

class Consumer(multiprocessing.Process):
    
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()        
        
    def stop(self):
        self.stop_event.set()

    def set_ip(self, ip="127.0.0.1"):
        self.ip = ip

    def run(self):
        print "Test IP [%s] : %s" % (self.ip,time.ctime())

        consumer = KafkaConsumer(
            bootstrap_servers=[self.ip],
            auto_offset_reset='earliest'            
            )
        
        topic_lists = consumer.topics()
        sorted(topic_lists, cmp=None, key=None, reverse=True)
        for topic in topic_lists:
            print topic

        consumer.close()


def init_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Consumer Test server init IP")    
    parser.add_argument('-ip', dest='ip', help='IP Server', required=True)
    return parser
  

def main():
    parser = init_parser()
    args = parser.parse_args()
    cons = Consumer()
    cons.set_ip(args.ip)

    tasks = [        
        cons
    ]

    for task in tasks:
        task.start()

    time.sleep(10)
    
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
    

if __name__ == "__main__": 
    main()
