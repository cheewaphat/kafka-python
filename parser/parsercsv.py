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
                # print field_ora , value
                self._map_rows.update({field_ora : value})
        

        if self._map_rows :
            self.isMappeed = True
    
    
    def check_oracle_funciton(self,field=None,value=None):
        if self._config.has_option("oracle-function",field):            
            pattern =   self._config.get('oracle-function', field) 
            iRe = re.compile(re.escape( "{%s}" % field ), re.IGNORECASE)            
            return iRe.sub(value, pattern)

        return value

    def parse_json(self,d, keys):                
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self.parse_json(d.get(key,""), rest)
        else:
            return str(d.get(keys,""))

    def todo(self,path):
        # message = self._consumer_msg        
        values = []
        fields = []
        # print self._map_rows.keys     
        for fld, val in self._map_rows.iteritems():               
            fields.append(fld)
            values.append(val)                     
        
        # try:
        with open(path, 'wb+') as outfile:
            # output dict needs a list for new column ordering                       
            writer = csv.DictWriter(outfile, fieldnames=fields,delimiter='|',quoting=csv.QUOTE_MINIMAL)
            # reorder the header first
            writer.writeheader()
            for row in csv.DictReader(self._map_rows):
                writer.writerow(row)            
            # print infile.count
            # for row in csv.DictReader(infile):
            logging.info( "Done " )

        # except expression as identifier:
        #     logging.error(identifier.msg)          

        print values
    
    def out(self,path=None):
        self.dir_exists(path)        
        self.todo(path)
        logging.info("out : %s" % path)
    