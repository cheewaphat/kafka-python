#!/usr/bin/env python
#kafka-console-producer --broker-list "172.19.103.231:9092,172.19.103.232:9092,172.19.103.233:9092" --topic hello
#kafka-console-consumer --zookeeper "172.19.103.231:9092,172.19.103.232:9092,172.19.103.233:9092" --topic am-91004 --from-beginning
#kafka-topics --list --zookeeper "172.19.103.231:2181,172.19.103.232:2181,172.19.103.233:2181"
import threading, logging, time
import multiprocessing
import json
import csv
import os

from kafka import KafkaConsumer



class Consumer(multiprocessing.Process):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers=['172.19.103.231:9092','172.19.103.232:9092','172.19.103.233:9092' ],
            auto_offset_reset='earliest'
            )

        consumer.subscribe([
            'am-91000',
            'am-91001',
            'am-91004',
            'am-91012',
            'am-93000',
            'am-93001',
            'am-93012',
            #'am-all',
            'am-raw'
            'ccp-1001',
            'ccp-1002',
            'ccp-1003',
            'ccp-1004',
            'ccp-1005',
            'ccp-1006',
            'ccp-1007',
            'ccp-1100',
            'ccp-1101',
            'ccp-1102',
            'ccp-1200',
            'ccp-1300',
            'ccp-1400',
            'ccp-1500',
            'ccp-1600',
            'ccp-1700',
            'ccp-1900',
            'ccp-2100',
            'ccp-2200',
            'ccp-2300',
            'ccp-2400',
            'ccp-all',
            'ccp-file',
            'ccp-raw',
            # 'dmc-811001',
            # 'dmc-812001',
            # 'dmc-812005',
            # 'dmc-812006',
            # 'dmc-812007',
            # 'dmc-832005',
            #'dmc-all',
            'dmc-file',
            'dmc-raw',
            # 'etg-logs',
            # 'etg-logss',
            'metrics',
            # 'mldd-50001',
            # 'mldd-50040',
            # 'mldd-50041',
            # 'mldd-52010',
            # 'mldd-52013',
            # 'mldd-52020',
            # 'mldd-52021',
            # 'mldd-52022',
            # 'mldd-52024',
            #'mldd-all',
            'my-topic',
            'performance_topic'
        ])

        for message in consumer:             
            toFile(message)

def toFile(message):            
    dirname = "/tmp/lab/json/"+message.topic+"/"
    path = dirname+message.topic+"_"+str(message.timestamp)+".json"  
    print "write file "+ path

    #check dir
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(path, "a") as f:
        f.write("%(value)s\n" % {"value":message.value})         
  

def main():
    tasks = [Consumer()]

    for t in tasks:
        t.start()

    time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
