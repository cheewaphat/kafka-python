#!/usr/bin/env python
import threading, logging, datetime,time
import multiprocessing
import json,csv
import re
import ConfigParser, os, sys


from kafka import KafkaConsumer
from parser import ParserCsv
       

# consumer class
class Consumer(multiprocessing.Process):
    daemon  = True
    
    def __init__(self,cfgFile):        
        self.set_config(cfgFile)       
        self.currentdate= datetime.date.today()
        self.dir_tmp    = "/tmp/lab/kafka-python/tmp/%s/" % self.currentdate
        self.dir_json   = "/tmp/lab/kafka-python/json/%s/" % self.currentdate
        self.dir_csv    = "/tmp/lab/kafka-python/csv/%s/" % self.currentdate
        self.dir_sql    = "/tmp/lab/kafka-python/sql/%s/" % self.currentdate
    
    
    def set_config(self,path):        
        self._abspath = os.path.abspath(path)
        self._execdir = os.path.dirname(self._abspath)        
        self._configfileName = os.path.basename(self._abspath)
        self._configPath= self._abspath        

        logging.info( "Loadding config .ini = %s" % self._configPath )
        config = ConfigParser.ConfigParser()
        config.read(self._configPath)
        self.config = config
        # init
        self._cfg_schema  =  self.config.get('target','schema')
        self._cfg_table   =  self.config.get('target','table')
        self._cfg_mapper  =  self.config.items('database-mapper')        


    def run(self):
        bootstrap_servers = self.config.get('kafka', 'bootstrap_servers').split(',')
        topic = self.config.get('kafka','topic').split(',')

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest'            
            )        
        consumer.subscribe(topic)       
        
        #build 
        for message in consumer:             
            self.mapper(message)
            # self.write_tmp(message)
            # self.write_sql(message)
            self.write_csv(message)
            self.write_json(message)

    def write_tmp(self,message):
        dirname = self.dir_tmp
        path    = dirname+message.topic+"_"+str(message.timestamp)+".out"  
        
        self.dir_exists(dirname)

        with open(path, "a") as f:
            print >> f , message
        
        logging.info("write %s" % path)

    def write_csv(self,message):
        dirname = self.dir_csv
        path    = dirname+message.topic+"_"+str(message.timestamp)+".out"  
        
        self.dir_exists(dirname)

        csvMng = ParserCsv(injson=self._map_rows, output=path )
        csvMng.out()
        
        logging.info("write %s" % path)
    
    def write_json(self,message):
        dirname = self.dir_json
        path    = dirname+message.topic+"_"+str(message.timestamp)+".json"  
        self.dir_exists(dirname)
        
        print "write json"
        print self._map_rows
        
        logging.info("write %s" % path)



    def write_sql(self,message):
        dirname = self.dir_sql
        path    = dirname+message.topic+"_"+str(message.timestamp)+".sql"  
        self.dir_exists(dirname)
        
        values = []
        fields = []

        for fld, val in self._map_rows.iteritems():   
            val = self.check_oracle_funciton(fld,val)
            fields.append(fld)
            values.append(val)                    

        with open(path, 'w') as f:                 
            str_field = "%s %r" % (  str(self._cfg_table), tuple(fields), )
            str_value = "%r" % ( tuple(values), )
            f.write( "INSERT INTO %s VALUES %s ;" % (str_field.replace('\'', ''), str_value.replace('"', '') )  )

        logging.info("write %s" % path)
    
    def mapper(self,message):
        jsonData = json.loads(message.value)                
        rows   = {}
        for field_ora, field_json in self._cfg_mapper:   
            if field_json :             
                value = self.parse_json(jsonData,field_json)
                if value :
                    rows.update({field_ora : value})

        self._map_rows   = rows

        if self._map_rows :
            self.isMappeed = True
    

    def to_csv(self,message):
        print message

    
    def to_json(self,message):
        _date   = datetime.datetime.fromtimestamp(message.timestamp/1000).strftime('%Y%m%d')    
        dirname = "/tmp/lab/json/"+_date+"/"+message.topic+"/"
        path = dirname+message.topic+"_"+str(message.timestamp)+".json"  
        logging.info( "write file "+ path )

        self.dir_exists(dirname)

        with open(path, 'w') as outfile:        
            try:
                dataJson = json.loads(message.value)           
                json.dump(dataJson,outfile)                            
            except ValueError:
                logging.error( ValueError.message )


    def to_oracle(self,message):
        print message

    def check_oracle_funciton(self,field=None,value=None):
        if self.config.has_option("oracle-function",field):            
            pattern =   self.config.get('oracle-function', field) 
            iRe = re.compile(re.escape( "{%s}" % field ), re.IGNORECASE)            
            return iRe.sub(value, pattern)

        return value


    def to_query(self,message):
        print message
    
    
    def dir_exists(self,dirname):
        if not os.path.exists(dirname):
            os.makedirs(dirname)


    def parse_json(self,d, keys):                
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.parse_json(d.get(key,""), rest)
        else:
            return str(d.get(keys,""))


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
        # filename='/tmp/lab/consumer-listener-dev.log',
        # filemode='w',
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',        
        level=logging.INFO
        )
    main()
