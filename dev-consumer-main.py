#!/usr/bin/env python
import logging, datetime,time
import multiprocessing
import ConfigParser, os, sys


from kafka import KafkaConsumer
from parser import ParserSQL
from parser import ParserCSV
       

# consumer class
class Consumer(multiprocessing.Process):
    daemon  = True
    
    def set_config(self,path):       
        self.currentdate= datetime.date.today().strftime('%Y%m%d')
        # datetime.datetime.today().strftime('%Y%m%d')     
        self.dir_csv    = "/tmp/workspace/kafka-python/csv/%s" % self.currentdate
        self.dir_sql    = "/tmp/workspace/kafka-python/sql/%s" % self.currentdate         
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
        
        for msg in consumer:                         
            # ParserSQL            
            pSQL = ParserSQL(message=msg,config=self.config)   
            pSQL.out( "%s/%s_%s_%s.sql" %( self.dir_sql, msg.topic, msg.key, msg.timestamp ) )
            # ParserCSV            
            pCSV = ParserCSV(message=msg,config=self.config)   
            pCSV.out( "%s/%s.csv" %( self.dir_csv, msg.topic ) )


def init_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Consumer to ORACLE Datable")
    parser.add_argument('config',help="configuretion file")        
    return parser


def main():    
    parser = init_parser()
    args = parser.parse_args()
    cons = Consumer()        
    cons.set_config(args.config)    

    tasks = [
        cons
    ]

    for t in tasks:
        t.start()

    time.sleep(10)
 

if __name__ == "__main__":
    currentdate= datetime.date.today() 
    logging.basicConfig(
        filename="/tmp/workspace/kafka-python/log/dev_consumer_main_%s.log" % (currentdate),
        # filemode='w',
        # format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',      
        format='%(asctime)s-%(name)s-%(levelname)s %(message)s',
        level=logging.INFO
        )
    main()
