#!/usr/bin/env python
import threading, logging, datetime,time
import multiprocessing
import json,csv
import ConfigParser, os, sys

from kafka import KafkaConsumer

       

# consumer class
class Consumer(multiprocessing.Process):
    daemon  = True

    def __init__(self,cfgFile):
        self.set_config(cfgFile)


    def run(self):
        # bootstrap_servers = map(lambda s: s.strip('\''), self.config.get('kafka', 'bootstrap_servers').split(','))
        # topic = map(lambda s: s.strip('\''), self.config.get('kafka','topic').split(','))

        bootstrap_servers = self.config.get('kafka', 'bootstrap_servers').split(',')
        topic = self.config.get('kafka','topic').split(',')

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest'
            )

        consumer.subscribe(topic)
        
        for message in consumer: 
            self.to_json(message)


    def set_config(self,path):        
        self._abspath = os.path.abspath(path)
        self._execdir = os.path.dirname(self._abspath)        
        self._configfileName = os.path.basename(self._abspath)
        self._configPath= self._abspath

        logging.info( "Loadding config ini = %s" % self._configPath )
        config = ConfigParser.ConfigParser()
        config.read(self._configPath)
        self.config = config
    
    def to_json(self,message):
        print message
        

def main():
    tasks = Consumer(sys.argv[1])    
    # tasks.set_config(sys.argv[1])
    tasks.run()
 

if __name__ == "__main__":
    logging.basicConfig(
        # filename='/tmp/lab/consumer-listener-dev.log',
        # filemode='w',
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
