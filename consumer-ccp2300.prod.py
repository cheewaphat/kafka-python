#!/usr/bin/env python
#kafka-console-producer --broker-list "172.19.103.231:9092,172.19.103.232:9092,172.19.103.233:9092" --topic ccp-2300
#kafka-topics --zookeeper "172.19.103.231:2181,172.19.103.232:2181,172.19.103.233:2181" --list
#kafka-console-consumer  --zookeeper "172.19.103.231:2181,172.19.103.232:2181,172.19.103.233:2181" --topic ccp-2300 --from-beginning

import threading, logging, datetime,time
import multiprocessing
import json
import os
import socket

from kafka import KafkaConsumer

class Consumer(multiprocessing.Process):
    
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()                
        
    def run(self):
        print "Running : %s" % time.ctime()       
        self.hostname = socket.gethostname()
        

        consumer = KafkaConsumer(
            bootstrap_servers=[ 'mtg1-etgkafka-01','mtg1-etgkafka-02','mtg1-etgkafka-03' ],
            client_id="dev-dhw-%s-%s-%s" % ("consumer",self.hostname, self.pid),
            group_id ="dev-dhw",
            auto_offset_reset='latest'            
            )        
        
        consumer.subscribe(['ccp-2300'])

        while not self.stop_event.is_set():
            for message in consumer:                
                logging.info(message)
                if self.stop_event.is_set():
                    logging.log("stop my self")
                    break

        consumer.close()

def main():
    tasks = [        
        Consumer()
    ]

    for task in tasks:
        task.start()

    # time.sleep(10)
    
    # for task in tasks:
    #     task.stop()

    # for task in tasks:
    #     task.join()
    

if __name__ == "__main__":    
    logging.basicConfig(
        format='%(asctime)s-%(name)s-%(levelname)s %(message)s',
        level=logging.INFO
        )
    main()
