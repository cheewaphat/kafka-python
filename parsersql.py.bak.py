#!/usr/bin/env python
import sys,io
import json,csv
import logging
import re
import ConfigParser

log = logging.getLogger(__name__)


class ParserSQLBAK(object):
    """Parse SQL"""

    def __init__(self,sinput=None,  finput=None, foutput=None, nodename=None, *config):
        logging.info("Start Parse SQL"+config)
        self._finput = finput
        self._sinput = sinput
        self._output = foutput
        self._nodename = nodename
        self._config = config
        self.header = []
        self.body = []        

        if finput:
            self.setJsonFile(finput)                        

        if sinput:
            self.setJsonString(sinput)     


    def to_string(self,s):            
        try:
            return str(s)
        except:
            return s.encode('utf-8')

    def setJsonFile(self, filepath=None):                
        fp = open(filepath, 'r')
        json_value = fp.read()
        raw_data = json.loads(json_value)
        self.load(raw_data)

    def setJsonString(self, sJson=None):
        raw_data = json.loads(sJson)
        self.load(raw_data)

    def load(self,data):        
        try:
            self.data_json  = data[self._nodename]
        except:
            self.data_json = data


    def out(self, path=None):
        values = []
        fields = []

        for fld, val in self.data_json.iteritems():   
            val = self.check_oracle_funciton(fld,val)
            fields.append(fld)
            values.append(val)

        print fields
        print values                    

        # with open(path, 'w') as f:                 
        #     str_field = "%s %r" % (  str(self._cfg_table), tuple(fields), )
        #     str_value = "%r" % ( tuple(values), )
        #     f.write( "INSERT INTO %s VALUES %s ;" % (str_field.replace('\'', ''), str_value.replace('"', '') )  )


    def check_oracle_funciton(self,field=None,value=None):
        if self._config.has_option("oracle-function",field):            
            pattern =   self._config.get('oracle-function', field) 
            iRe = re.compile(re.escape( "{%s}" % field ), re.IGNORECASE)            
            return iRe.sub(value, pattern)

        return value
        

    # def out(self,path=None):
    #     try:                               
    #         with open(self._output, 'w') as f:
    #             writer = csv.DictWriter(f, self.data_json.keys(), quoting=csv.QUOTE_ALL)
    #             writer.writeheader()                
    #             writer.writerow(self.data_json)
    #             logging.info( "Just completed writing csv file with %d columns" % len(self.data_json.keys()) )
            
    #     except Exception, err:
    #         logging.error('ERROR: %s' % str(err))