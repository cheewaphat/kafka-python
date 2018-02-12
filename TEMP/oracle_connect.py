#!/usr/bin/env python
import threading, logging, time
import multiprocessing
import json
import csv
import os
import cx_Oracle

from kafka import KafkaConsumer



con = cx_Oracle.connect('pythonhol/welcome@127.0.0.1/orcl')
print con.version
con.close()