#!/usr/bin/env python
import threading, logging, datetime,time
import multiprocessing
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
        print "IP %s : " % self.ip
        consumer = KafkaConsumer( bootstrap_servers=[self.ip])        
        topic_lists = consumer.topics()        
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
