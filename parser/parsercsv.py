#!/usr/bin/env python
import sys
import json,csv
import logging

log = logging.getLogger(__name__)


class ParserCsv(object):
    """Parse CSV"""

    header = []
    body = []

    def __init__(self, injson=None, output=None, nodename=None):
        self._input = injson
        self._output = output
        self._nodename = nodename

        if json:
            self.load(json)            

    def to_string(self,s):
        try:
            return str(s)
        except:
            return s.encode('utf-8')

    def load(self,data):        
        try:
            self.data_json  = data[self._nodename]
        except:
            self.data_json = data

        #set headert
        self._setHeader()
        self._setBody()
    

    def _setBody(self):
        print "Body"
        print self.data_json

    def _setHeader(self):
        print "Header"
        print self.data_json
            

    def out(self,path=None):
        self._output = path
        
        try:            
            print self._output
        except Exception:
            print Exception.message


 