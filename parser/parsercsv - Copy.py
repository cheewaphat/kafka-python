#!/usr/bin/env python
import sys,io
import json,csv
import logging
import ConfigParser

log = logging.getLogger(__name__)


class ParserCsv(object):
    """Parse CSV"""

    def __init__(self,sinput=None,  finput=None, foutput=None, nodename=None,config=None):
        logging.info("Start Parse CSV")
        self._finput = finput
        self._sinput = sinput
        self._output = foutput
        self._nodename = nodename
        self._config = config
        self.header = []
        self.body = []        
        print self._config
        # fp = open(self._finput, 'r')
        # json_value = fp.read()
        # raw_data = json.loads(json_value)
        # print raw_data
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

        #set headert
        # self._setHeader()
        # self._setBody()
    

    # def _setBody(self):        
    #     for k,v in self.data_json.iteritems():
    #         self.body.append( self.to_string(v) )
        

    # def _setHeader(self):                
    #     for k,v in self.data_json.iteritems():            
    #         self.header.append( k )
        

    def out(self,path=None):
        try:                               
            with open(self._output, 'w') as f:
                writer = csv.DictWriter(f, self.data_json.keys(), quoting=csv.QUOTE_ALL)
                writer.writeheader()                
                writer.writerow(self.data_json)
                logging.info( "Just completed writing csv file with %d columns" % len(self.data_json.keys()) )
            
        except Exception, err:
            logging.error('ERROR: %s' % str(err))