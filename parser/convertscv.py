#!/usr/bin/env python
import threading, logging, datetime,time
import multiprocessing
import json,csv
import re
import ConfigParser, os, sys


       

# consumer class
class ConvertCSV(object):
    daemon  = True
    
    def __init__(self,cfgFile,consumer):        
        self.set_config(cfgFile)       
        self.currentdate= datetime.date.today()
        self.date       = time.strftime("%Y%m%d")
        self.dir_csv    = "/tmp/lab/kafka-python/csv/%s/" % self.date
    
    
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
