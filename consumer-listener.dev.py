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
            self.to_sql(message)


    def set_config(self,path):        
        self._abspath = os.path.abspath(path)
        self._execdir = os.path.dirname(self._abspath)        
        self._configfileName = os.path.basename(self._abspath)
        self._configPath= self._abspath

        logging.info( "Loadding config ini = %s" % self._configPath )
        config = ConfigParser.ConfigParser()
        config.read(self._configPath)
        self.config = config

    def fields(self,message):
        print message

    def to_sql(self,message):
        # mapping        
        jsonData = json.loads(message.value)
        cfg_schema  =  self.config.get('target','schema')
        cfg_table   =  self.config.get('target','table')
        cfg_mapper  =  self.config.items('database-mapper')
        values = []
        fields = []
        for field_ora, field_json in cfg_mapper:                        
            if field_json :             
                fields.append(field_ora)
                values.append(str(self.parse_json(jsonData,field_json)))
        
        print "INSERT INTO %s %r VALUES %r;" % (str(cfg_table) , (tuple(fields)),tuple(values),) 

        

        
        

   

    def to_csv(self,message):
        print message
    
    def to_json(self,message):
        _date   = datetime.datetime.fromtimestamp(message.timestamp/1000).strftime('%Y%m%d')    
        dirname = "/tmp/lab/json/"+_date+"/"+message.topic+"/"
        path = dirname+message.topic+"_"+str(message.timestamp)+".json"  
        logging.info( "write file "+ path )

        #check dir
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(path, 'w') as outfile:        
            try:
                dataJson = json.loads(message.value)           
                json.dump(dataJson,outfile)                            
            except ValueError:
                logging.error( ValueError.message )

    def to_oracle(self,message):
        print message
    
    def to_query(self,message):
        print message
    
    def parse_json(self,d, keys):        
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.parse_json(d[key], rest)
        else:
            return d[keys] 


def main():
    tasks = Consumer(sys.argv[1])        
        
    tasks.run()
 

if __name__ == "__main__":
    logging.basicConfig(
        # filename='/tmp/lab/consumer-listener-dev.log',
        # filemode='w',
        #format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
