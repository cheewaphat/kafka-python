#!/usr/bin/env python
import sys,io,os
import logging, datetime,time
import json,csv,re
import ConfigParser

log = logging.getLogger(__name__)


class ParserCSV(object):
    """Parse CSV"""
    count = 0

    def __init__(self,message=None,config=None):
        logging.info("Start Parse CSV")
        self._currentdate= datetime.date.today()
        self._consumer_msg = message       
        self._config = config        
        self._data_header =[]
        # self._data_body  =[]
        self._map_rows = {}

        ParserCSV.count += 1

        if message:
            self.loadConfig()
            self.mapper()            

    def loadConfig(self):
        self._cfg_schema  =  self._config.get('target','schema')
        self._cfg_table   =  self._config.get('target','table')
        self._cfg_mapper  =  self._config.items('database-mapper') 
        logging.info("load schema %s" % self._cfg_schema)
        logging.info("load table %s" % self._cfg_table)        

    def dir_exists(self,filepath):
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))
    
    def mapper(self):
        message = self._consumer_msg
        jsonData = json.loads(message.value)                
        
        for field_ora, field_json in self._cfg_mapper:               
            if field_ora :             
                value = self.parse_json(jsonData,field_json)                                
                self._data_header.append( field_ora )
                # self._data_body.append( value )
                self._map_rows[field_ora] = value

        self.isMappeed = True   


    def parse_json(self,d, keys):                
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.parse_json(d.get(key,""), rest)
        else:
            return str(d.get(keys,""))

    def todo(self,path):                
        try:
            with open(path, 'a') as outfile:                
                writer = csv.DictWriter(outfile, fieldnames=self._data_header,delimiter='|',quoting=csv.QUOTE_MINIMAL)                
                # writer.writeheader()            
                writer.writerow(self._map_rows)            
                logging.info( "Parser CSV done !!" )

        except :
            e = sys.exc_info()[0]            
            logging.error(e)          

        # print values
    
    def out(self,path=None):
        self.dir_exists(path)        
        self.todo(path)
        logging.info("out : %s" % path)
    