#!/usr/bin/env python
import threading, logging, time
import multiprocessing
import json
import csv

from kafka import KafkaConsumer



class Consumer(multiprocessing.Process):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers='172.19.103.232:9092',
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
            'am-all',
            'am-file',
            'am-raw',
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
            'dmc-811001',
            'dmc-812001',
            'dmc-812005',
            'dmc-812006',
            'dmc-812007',
            'dmc-832005',
            'dmc-all',
            'dmc-file',
            'dmc-raw',
            'etg-logs',
            'metrics',
            'mldd-50001',
            'mldd-50040',
            'mldd-50041',
            'mldd-52010',
            'mldd-52013',
            'mldd-52020',
            'mldd-52021',
            'mldd-52022',
            'mldd-52024',
            'mldd-all',
            'my-topic',
            'performance_topic',
            'schedule-topic',
            'targetTopic',
            'test',
            'whatsup-logs'
            ])

        for message in consumer:
            print (message)    
            print (message.topic)            
            print (message.partition)            
            print (message.offset)
            print (message.timestamp)
            print (message.value)
            print ("============")
            writeTopic(message,"/tmp/lab/"+message.topic+".out")
            writeJson(message,"/tmp/lab/"+message.topic+".json")

def writeJson(message,path):    
    print "Write file "+message.topic +" on "+path    
    with open(path, "a") as f:
        f.write("%(value)s\n" % {"value":message.value})

def toCSV(message,path):
    message_parsed = json.loads(message)    

def writeTopic(message,path):
    print "Write file "+message.topic +" on "+path    
    with open(path, "a") as f:
        f.write("%(timestamp)s|%(value)s|\n" % {"timestamp":message.timestamp,"value":message.value})
    

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
