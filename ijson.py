#!/usr/bin/env python
# test lib ijson
import os
import datetime
from lib import ijson 

def main():
    print "main"

if __name__ == "__main__":
    _run_date = datetime.date.today().strftime('%Y%m%d')
    _project = os.path.basename(os.path.dirname(os.path.realpath(__file__)))
    _name = os.path.basename(__file__)
    print ("check log file path :/tmp/workspace/%s/%s_%s.log" %  ( _project,_name,_run_date ) )
    main()