#!/usr/bin/env python
import sys,io,os
import logging, datetime,time
import json,re
import ConfigParser

log = logging.getLogger(__name__)


class ParserSQL(object):
    """Parse SQL"""
    count = 0

    def __init__(self,message=None,config=None):
        logging.info("Start Parse SQL")
        self._currentdate= datetime.date.today()
        self._consumer_msg = message       
        self._config = config
        ParserSQL.count += 1

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
        rows   = {}
        for field_ora, field_json in self._cfg_mapper:   
            if field_json :             
                value = self.parse_json(jsonData,field_json)
                if value :
                    rows.update({field_ora : value})

        self._map_rows   = rows

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
        message = self._consumer_msg        
        values = []
        fields = []
        
        for fld, val in self._map_rows.iteritems():   
            val = self.check_oracle_funciton(fld,val)
            fields.append(fld)
            values.append(val)                    

        with open(path, 'wb+') as f:                 
            str_field = "%s %r" % (  str(self._cfg_table), tuple(fields), )
            str_value = "%r" % ( tuple(values), )
            logging.debug ("INSERT INTO %s VALUES %s ;" % (str_field.replace('\'', ''), str_value.replace('"', '') )  )
            f.write( "INSERT INTO %s VALUES %s ;" % (str_field.replace('\'', ''), str_value.replace('"', '') )  )
    
    def out(self,path=None):
        self.dir_exists(path)        
        self.todo(path)
        logging.info("out : %s" % path)
    