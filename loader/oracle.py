#!/usr/bin/env python
import logging, datetime,time
import multiprocessing,threading
import ConfigParser, os, sys ,subprocess
import sched

log = logging.getLogger(__name__)

class OracleLoader(threading.Thread):     
    daemon = True
    run_on = 5 # minute

    def set_config(self,path):       
        if not os.path.exists(path):
            logging.error("No configuretion %s" % path)
            return False

        self.currentdate= datetime.date.today().strftime('%Y%m%d')        
        self.pid = os.getpid()   
        # self.dir_ldr    = "/tmp/workspace/kafka-python/ldr/%s" % self.currentdate  
        config = ConfigParser.ConfigParser()
        config.read(os.path.abspath(path))
        self.config = config        
        logging.info( "Loader config .ini = %s" % path )      

    def set_env(self):        
        os.putenv('ORACLE_HOME', '/opt/app/oracle/dbhdpmdv1/product/11.2.0.4/dbhome_1/') 
        os.putenv('LD_LIBRARY_PATH', '/opt/app/oracle/dbhdpmdv1/product/11.2.0.4/dbhome_1/lib/') 

    def set_source_dir(self,src):
        self.source_dir = src
        logging.info("CSV Source path : %s" % self.source_dir )

    def set_temp_dir(self,path):
        if not os.path.exists(path):
            os.makedirs(path)

        self.dir_ldr = path
        self.file_ctrl  = "{}/{}_{}.ctl".format( self.dir_ldr, self.config.get('kafka','topic'), self.pid )     
        self.file_data  = "{}/{}_{}.dat".format( self.dir_ldr, self.config.get('kafka','topic'), self.pid )     
        self.file_bad   = "{}/{}_{}.bad".format( self.dir_ldr, self.config.get('kafka','topic'), self.pid )     
        self.file_dsc   = "{}/{}_{}.dsc".format( self.dir_ldr, self.config.get('kafka','topic'), self.pid )
        self.file_log   = "{}/{}_{}.log".format( self.dir_ldr, self.config.get('kafka','topic'), self.pid )
        logging.info("Temp Loader path : %s" % self.dir_ldr )

    def dir_exists(self,filepath):
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

    def get_conf(self,topic,key):
        config = self.config
        try:
            data = config.get(topic, key)
        except (NoSectionError, NoOptionError):
            return ""
        return data

    def clean(self):        
        files = [
            self.file_ctrl,
            self.file_data,
            self.file_bad,
            self.file_dsc
        ]

        for f in files:            
            logging.info("Remove %s" % f )
            if os.path.exists(f):
                os.remove(f)

    def prepare(self):                
        datafile = "%s/%s.csv" % (self.source_dir, self.config.get('kafka','topic') )
        if not os.path.exists(datafile):
            logging.info("no data file %s" % datafile )
            return False
        
        if not os.path.exists(self.dir_ldr):
            os.makedirs(self.dir_ldr)
            

        target = self.file_data
        os.rename(datafile , target )
        filename = os.path.basename(datafile)
        self.src_csv=target
        logging.info("loader topic %s" % self.config.get('kafka','topic'))
        return True

    
    def make_ctl_file(self):      
        ctlfile     = self.file_ctrl
        datafile    = self.file_data
        if not os.path.exists(datafile) :
            return False      

        if os.stat(datafile).st_size == 0:
            logging.info("{} is empty".format(datafile))
            return False      

        logging.info(os.stat(datafile))  

        self.dir_exists(ctlfile)        
        ctl_dir =  os.path.dirname(ctlfile)    

        conf_ctrl = {
            "characterset":"UTF8",
            "infile" : self.file_data,
            "badfile" : self.file_bad,
            "discardfile" : self.file_dsc,
            "table" : "{}".format( self.config.get('target','table').upper() )
        }

        try:
            with open(ctlfile, 'wb') as outfile:       
                #CONFIG PARAMETER         
                outfile.write("LOAD DATA")
                outfile.write("\r\nCHARACTERSET {cfg[characterset]}".format(cfg=conf_ctrl))
                outfile.write("\r\nINFILE '{cfg[infile]}'".format(cfg=conf_ctrl) )
                outfile.write("\r\nBADFILE '{cfg[badfile]}'".format(cfg=conf_ctrl) )
                outfile.write("\r\nDISCARDFILE '{cfg[discardfile]}'".format(cfg=conf_ctrl) )
                outfile.write("\r\nAPPEND INTO TABLE {cfg[table]}".format(cfg=conf_ctrl) )
                outfile.write("\r\nFIELDS TERMINATED BY '|'")
                outfile.write("\r\nOPTIONALLY ENCLOSED BY '\"'")
                outfile.write("\r\nTRAILING NULLCOLS")
                outfile.write("\r\n(")
                
                #FOR DATA
                tmp_field = []
                for key,val in self.config.items('database-mapper') :                                        
                    if val:                        
                        if self.config.has_option('oracle-loader-format',key):
                            tmp_field.append("\r\n {} {}".format(key.upper(), self.config.get('oracle-loader-format',key)))
                        else:
                            tmp_field.append("\r\n {}".format(key.upper()))
                
                #STAMP FIELD
                for key,val in self.config.items('oracle-loader-fixed') :                          
                    tmp_field.append("\r\n {} {}".format(key.upper(), val) )

                outfile.write(",".join(tmp_field))
                outfile.write("\r\n)\r\n")
                logging.info("Control file %s" % ctlfile )

        except :
            e = sys.exc_info()[0]            
            logging.error(e)     

        return ctlfile


    def call_loader(self):          
        ctlfile = self.file_ctrl
        if not os.path.exists(ctlfile):
            logging.info("Control file not exists %s" % ctlfile)
            return False

        self.oracle_conf = {            
            "home":os.getenv('ORACLE_HOME').strip("/"),
            "username":self.config.get('target','username'),
            "password":self.config.get('target','password'),
            "server":self.config.get('target','server'),
            "port":self.config.get('target','port'),
            "service_name":self.config.get('target','schema'),
            "log_file":self.file_log,
            "control_file":ctlfile
        }                
        
        self.cmd_ldr='/{ora[home]}/bin/sqlldr userid={ora[username]}/{ora[password]}@{ora[server]}:{ora[port]}/{ora[service_name]} control={ora[control_file]}  log={ora[log_file]}'.format(ora=self.oracle_conf)        
        subprocess.call(self.cmd_ldr, shell=True)
        logging.info("Command loader : %s" % self.cmd_ldr)

    def run(self):
        logging.info("Run Loader")
        self.set_env()   
        
        while True:
            if self.prepare():
                self.make_ctl_file() 
                self.call_loader()
                self.clean

            time.sleep(self.run_on)