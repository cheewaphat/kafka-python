#!/usr/bin/env python
import logging, datetime,time
import multiprocessing,threading
import ConfigParser, os, sys ,subprocess

from kafka import KafkaConsumer
from parser import ParserSQL
from parser import ParserCSV

class OracleLoader(threading.Thread):     
    daemon = True
    run_on = 5 # minute

    def set_config(self,path):       
        self.currentdate= datetime.date.today().strftime('%Y%m%d')           
        self.dir_ldr    = "/tmp/workspace/kafka-python/ldr/%s" % self.currentdate        
        self._abspath = os.path.abspath(path)
        self._execdir = os.path.dirname(self._abspath)        
        self._configfileName = os.path.basename(self._abspath)
        self._configPath= self._abspath        
        config = ConfigParser.ConfigParser()
        config.read(self._configPath)
        self.config = config
        logging.info( "Loadding config .ini = %s" % self._configPath )      

    def set_env(self):
        # You can set these in system variables but just in case you didnt
        os.putenv('ORACLE_HOME', '/opt/app/oracle/dbhdpmdv1/product/11.2.0.4/dbhome_1/') 
        os.putenv('LD_LIBRARY_PATH', '/opt/app/oracle/dbhdpmdv1/product/11.2.0.4/dbhome_1/lib/') 

    def set_source_dir(self,src):
        logging.info("CSV Source dir")

    def dir_exists(self,filepath):
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))
    
    def make_ctl_file(self,filename):        
        logging.info("ctl file [oracle lodder] {}".format(filename))
        self.dir_exists(filename)
        try:
            with open(filename, 'wb') as outfile:                
                outfile.write("LOAD DATA")
                outfile.write("\r\nCHARACTERSET UTF8")
                outfile.write("\r\nINFILE '/tmp/workspace/kafka-python/csv/20171019/ccp-all.csv'".format(self.dir_ldr) )
                outfile.write("\r\nBADFILE	'{}/20171019/ccp-all.bad'".format(self.dir_ldr))
                outfile.write("\r\nDISCARDFILE	'{}/20171019/ccp-all.dsc'".format(self.dir_ldr))
                outfile.write("\r\nAPPEND INTO TABLE {} ".format(self.config.get('target','table')))
                outfile.write("\r\nFIELDS TERMINATED BY '|'")
                outfile.write("\r\nOPTIONALLY ENCLOSED BY '\"'")
                outfile.write("\r\nTRAILING NULLCOLS")
                outfile.write("\r\n(")
                # fieds
                for key,val in self.config.items('database-mapper') :
                    logging.info( "debug " +key +" , "+ val )
                    if val:                        
                        if self.config.get('oracle-loader',key):
                            outfile.write("\r\n {} {}".format(key.upper(), self.config.get('oracle-loader',key)))
                        else:
                            outfile.write("\r\n {}".format(key.upper()))

                outfile.write("\r\n)")

        except :
            e = sys.exc_info()[0]            
            logging.error(e)     





    def set_loader_cmd(self):          
        self.oracle_conf = {            
            "home":os.getenv('ORACLE_HOME').strip("/"),
            "username":self.config.get('target','username'),
            "password":self.config.get('target','password'),
            "server":self.config.get('target','server'),
            "port":self.config.get('target','port'),
            "service_name":self.config.get('target','schema'),
            "log_file":"/tmp/workspace/kafka-python/sqlldr_kafkaf.log",
            "control_file":"/cisapp/cisetl_apps1/lcisetl1b/workspace/kafka-python/tgw_ccp_dev.ctl"
        }                
        
        self.cmd_ldr='/{ora[home]}/bin/sqlldr userid={ora[username]}/{ora[password]}@{ora[server]}:{ora[port]}/{ora[service_name]} control={ora[control_file]}  log={ora[log_file]}'.format(ora=self.oracle_conf)        
        


    def run(self):
        self.set_env()     
        f_ctl =  "{0}/{1}.ctl".format(self.dir_ldr,self.config.get('kafka','topic'))
        self.make_ctl_file(f_ctl) 
        self.set_loader_cmd()

        while True:
            if self.cmd_ldr:
                subprocess.call(self.cmd_ldr, shell=True)
            time.sleep(2)
    

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
    #consumer
    cons = Consumer()        
    cons.set_config(args.config)    
    #loader
    oraldr = OracleLoader()
    oraldr.set_config(args.config)    

    tasks = [
        #cons,
        oraldr
    ]

    for t in tasks:
        t.start()

    time.sleep(10)
 

if __name__ == "__main__":
    currentdate= datetime.date.today() 
    logging.basicConfig(
        # filename="/tmp/workspace/kafka-python/log/dev_consumer_main_%s.log" % (currentdate),
        # filemode='w',
        # format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',      
        format='%(asctime)s-%(name)s-%(levelname)s %(message)s',
        level=logging.INFO
        )
    main()
