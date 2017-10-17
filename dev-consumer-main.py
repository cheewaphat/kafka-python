#!/usr/bin/env python
import threading, logging, datetime,time
import multiprocessing
import json,csv
import re
import ConfigParser, os, sys
import traceback


from kafka import KafkaConsumer
from parser import ParserSQL 
       

# consumer class
class Consumer(multiprocessing.Process):
    daemon  = True
    
    def __init__(self,cfgFile):        
        self.set_config(cfgFile)       
        self.currentdate= datetime.date.today()
        # self.dir_tmp    = "/tmp/workspace/kafka-python/tmp/%s" % self.currentdate
        # self.dir_json   = "/tmp/workspace/kafka-python/json/%s" % self.currentdate
        # self.dir_csv    = "/tmp/workspace/kafka-python/csv/%s" % self.currentdate
        self.dir_sql    = "/tmp/workspace/kafka-python/sql/%s" % self.currentdate
    
    
    def set_config(self,path):        
        
        self._abspath = os.path.abspath(path)
        self._execdir = os.path.dirname(self._abspath)        
        self._configfileName = os.path.basename(self._abspath)
        self._configPath= self._abspath        
        config = ConfigParser.ConfigParser()
        config.read(self._configPath)
        self.config = config
        logging.info( "Loadding config .ini = %s" % self._configPath )           


    def run(self):
        bootstrap_servers = self.config.get('kafka', 'bootstrap_servers').split(',')
        topic = self.config.get('kafka','topic').split(',')

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest'            
            )        
        consumer.subscribe(topic)       
        
        #build 
        for msg in consumer:             
            print msg

            # ParserSQL            
            pSQl = ParserSQL(message=msg,config=self.config)   
            pSQl.out( "%s/%s_%s_%s.sql" %( self.dir_sql, msg.topic, msg.key, msg.timestamp ) )


def init_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Consumer to ORACLE Datable")
    parser.add_argument('config',help="configuretion file")        
    return parser


def main():    
    parser = init_parser()
    args = parser.parse_args()
    tasks = Consumer(args.config)        
    tasks.run()
 

if __name__ == "__main__":
    logging.basicConfig(
        # filename='/tmp/workspace/dev-consumer-main.log',
        # filemode='w',
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',        
        level=logging.INFO
        )
    main()
