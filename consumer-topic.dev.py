#!/usr/bin/env python
#kafka-console-producer --broker-list "172.19.103.231:9092,172.19.103.232:9092,172.19.103.233:9092" --topic hello
#kafka-topics --list --zookeeper "172.19.103.231:2181,172.19.103.232:2181,172.19.103.233:2181"
import threading, logging, time
import multiprocessing
import json
#import csv
import os
import datetime

from kafka import KafkaConsumer

class Consumer(multiprocessing.Process):
    daemon = True

    def run(self):
        print "Running : %s" % time.ctime()        

        consumer = KafkaConsumer(
            bootstrap_servers=['172.19.103.231:9092','172.19.103.232:9092','172.19.103.233:9092' ],
            auto_offset_reset='earliest'            
            )
        
        consumer.subscribe([
            'am-1001',
            'am-9100',
            'am-91000',
            'am-91001',
            'am-91004',
            'am-91005',
            'am-91007',
            'am-91010',
            'am-91012',
            'am-93000',
            'am-93001',
            'am-93012',
            'am-all',
            'am-file',
            'am-raw',
            'am-rawccp-1001',
            'ccp-0',
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
            'ccp-1200_2',
            'ccp-1300',
            'ccp-1400',
            'ccp-1500',
            'ccp-1600',
            'ccp-1700',
            'ccp-1800',
            'ccp-1900',
            'ccp-2100',
            'ccp-2200',
            'ccp-2300',
            'ccp-2400',
            'ccp-91007',
            'ccp-91012',
            #'ccp-all',
            #'ccp-file',
            #'ccp-raw',
            'dmc-811001',
            'dmc-811005',
            'dmc-812001',
            'dmc-812005',
            'dmc-812006',
            'dmc-812007',
            'dmc-831003',
            'dmc-831005',
            'dmc-832005',
            #'dmc-all',
            #'dmc-file',
            #'dmc-raw',
            #'etg-logs',
            #'etg-logss',
            #'metrics',
            'mldd-50001',
            'mldd-50040',
            'mldd-50041',
            'mldd-52010',
            'mldd-52013',
            'mldd-52020',
            'mldd-52021',
            # 'mldd-52022',
            # 'mldd-52024',
            #'mldd-all'
        ])

        for message in consumer:             
            # logging.info("Topic :%s" % message)
            toFile(message)

def toFile(message):            
    _date   = datetime.datetime.fromtimestamp(message.timestamp/1000).strftime('%Y%m%d')   
    _project = os.path.basename( os.getcwd() )
    dirname = "/tmp/workspace/%s/monitor/json/%s/%s" % ( _project ,_date , message.topic )
    path = "%s/%s_%s_%s.json" % (dirname, message.topic, message.key, message.timestamp)   
    logging.info( "write "+ path )

    #check dir
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(path, "w") as f:
        try:
            f.write("%(value)s\n" % {"value":message.value.decode('utf-8')}) 
        except ValueError:
            logging.error( ValueError.message )
            

    # with open(path, 'a') as outfile:        
    #     try:
    #         dataJson = json.loads(message.value)           
    #         json.dump(dataJson,outfile)                            
    #     except ValueError:
    #         logging.error( ValueError.message )
            
  

def main():
    tasks = [        
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)
    

if __name__ == "__main__":
    _project = os.path.basename( os.getcwd() )
    logging.basicConfig(
        filename='/tmp/workspace/%s/consumer-topic.log' % _project,
        filemode='w',
        #format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        format='%(asctime)s-%(name)s-%(levelname)s %(message)s',
        level=logging.INFO
        )
    main()
