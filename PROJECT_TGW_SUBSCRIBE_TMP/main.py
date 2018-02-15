#!/usr/bin/env python
import logging
import datetime
import time
import multiprocessing
import threading
import ConfigParser
import os
import sys
import subprocess
import socket

from kafka import KafkaConsumer
from parser import ParserSQL
from parser import ParserCSV
from loader import OracleLoader

run_date = datetime.date.today().strftime('%Y%m%d')
app_home = os.environ["APP_HOME"] = os.path.dirname(os.path.realpath(__file__))
tmp_path = os.getenv('PATH_TMP')
log_path = os.getenv('PATH_LOG')

if os.getenv('PATH_TMP') is None:
    os.environ["PATH_TMP"] = os.path.join(os.getenv('APP_HOME'), "tmp")
    tmp_path = os.getenv('PATH_TMP')

if os.getenv('PATH_LOG') is None:
    os.environ["PATH_LOG"] = os.path.join(os.getenv('APP_HOME'), "log")
    log_path = os.getenv('PATH_LOG')

# consumer class


class Consumer(multiprocessing.Process):
    offset_earliest = "earliest"
    offset_latest = "latest"

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        self.hostname = socket.gethostname()
        # self.hostname

    def stop(self):
        self.stop_event.set()

    def set_offet(self, mode="latest"):
        self.auto_offset_reset = mode

    def set_config(self, path):
        self._abspath = os.path.abspath(path)
        self._execdir = os.path.dirname(self._abspath)
        self._configfileName = os.path.basename(self._abspath)
        self._configPath = self._abspath
        config = ConfigParser.ConfigParser()
        config.read(self._configPath)
        self.config = config
        logging.info("Loadding config .ini = %s" % self._configPath)

    def set_target_csv_dir(self, target):
        self.dir_csv = "%s" % (target)
        logging.info(self.dir_csv)

    def set_target_sql_dir(self, target):
        self.dir_sql = "%s" % (target)
        logging.info(self.dir_sql)

    def run(self):

        bootstrap_servers = self.config.get(
            'kafka', 'bootstrap_servers').split(',')
        topic = self.config.get('kafka', 'topic').split(',')
        group = self.config.get('kafka', 'group')
        client = "dhw-{}-{}-{}".format(self.hostname,
                                       "_".join(topic), self.pid)

        logging.info("client  %s : %s " % (client , group ))
        consumer = KafkaConsumer(
            client_id=client,
            group_id=group,
            max_poll_records=500,
            max_poll_interval_ms=300000,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=self.auto_offset_reset
        )
        consumer.subscribe(topic)
        logging.info(
            "Consumer [%s:%s] is running subscribe [%s]" % (group, client, topic))

        while not self.stop_event.is_set():
            for msg in consumer:
                pCSV = ParserCSV(message=msg, config=self.config)
                pCSV.out("%s/%s.csv" % (self.dir_csv, msg.topic))
                if self.stop_event.is_set():
                    logging.info("Topic:%s is stop" % topic)
                    break

        consumer.close()


def init_parser():
    import argparse
    parser = argparse.ArgumentParser(
        description="Kafka Consumer to ORACLE Datable")

    # parser.add_argument('config',help="configuretion file")
    parser.add_argument('-c', dest='config',
                        help='configuretion file', required=True)
    parser.add_argument(
        '-m', dest='mode', help='offet mode [ earliest | latest]', default="latest")
    parser.add_argument('-t', dest='tmp_path',
                        help='tempolary path get env PATH_TMP', default=tmp_path)
    parser.add_argument('-l', dest='log_path',
                        help='log path get env PATH_LOG', default=log_path)
    parser.add_argument('--log_name', dest='log_name',
                        help='log name', default="TGW_MSISDN")

    return parser


def main():

    parser = init_parser()
    args = parser.parse_args()
    #consumer
    cons = Consumer()
    cons.set_config(args.config)
    cons.set_offet(args.mode)
    cons.set_target_csv_dir("%s/data/%s/csv" % (args.tmp_path, run_date))

    #loader
    oraldr = OracleLoader()
    oraldr.set_config(args.config)
    oraldr.set_temp_dir("%s/ldr/%s" % (args.tmp_path, run_date))
    oraldr.set_source_dir("%s/data/%s/csv" % (args.tmp_path, run_date))

    threads = [
        cons,
        oraldr
    ]

    for t in threads:
        t.start()

    # time.sleep(10)

    # for task in threads:
    #     task.stop()

    # for task in threads:
    #     task.join()


# Start Process
if __name__ == "__main__":
    parser = init_parser()
    args = parser.parse_args()

    logging.basicConfig(
        filename="%s/%s" % (args.log_path, args.log_name),
        # filename="%s/%s_%s.log" % (args.log_path,args.log_name,run_date),
        filemode='a',
        # format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        format='%(asctime)s-%(name)s:%(thread)d-%(levelname)s %(message)s',
        level=logging.INFO
    )
    main()
