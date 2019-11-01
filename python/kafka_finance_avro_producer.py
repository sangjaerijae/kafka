#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import avro.io
import avro.schema
import fnmatch
import csv
import sys
import os.path
import glob
import logging
import argparse
import datetime
import json
import time
import requests
import threading, logging, time
import multiprocessing
import fastavro

from avro.io import DatumWriter
from fastavro import reader
from fastavro import json_reader
from dateutil.parser import parse
from json import dumps
from json import loads
from os.path import basename
from kafka import KafkaProducer
from kafka import KafkaClient
from kafka import SimpleProducer
from kafka.errors import KafkaError

csvpath="finance"
loghome="log"

columkeys = (
    "company_name","five_days","twenty_five_days","days_75","days_200","accounting_period_2017","accounting_period_2018","accounting_period_2019","amount_of_sales_2017","amount_of_sales_2018","amount_of_sales_2019",
    "ordinary_profit_2017","ordinary_profit_2018","ordinary_profit_2019","net_profit_2017","net_profit_2018","net_profit_2019","eps_2017","eps_2018","eps_2019","dps_2017","dps_2018","dps_2019",
    "announcement_date_2017","announcement_date_2018","announcement_date_2019"
)

kafka_host=str('localhost:9092')

def getAvroSchema(schema_registry_uri):
    resp=requests.get(schema_registry_uri)
    strjson=json.dumps(json.loads(resp.json()['schemaText']))
    schema = avro.schema.Parse(strjson)
    #strjson=json.dumps(json.loads(resp.json()['schemaText']))
    #avroschema=loads(strjson)
    #schema=fastavro.parse_schema(avroschema)
    #SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())
    return schema


def getFilesToWorkOn(inputPath):
    files = []
    if os.path.exists(inputPath) and os.path.isdir(inputPath):
        files=sorted(glob.glob(inputPath + "/*"), reverse=False)
    elif os.path.isdir(inputPath):
        for inputFile in os.listdir(inputPath):
            if fnmatch.fnmatch(inputFile, '/*'):
              files.append(inputPath + '/' +inputFile)
    return files


def isExistDirectory(dirName):
    isExistFlag = False
    if os.path.exists(dirName) and os.path.isdir(dirName):
        if not os.listdir(dirName):
            isExistFlag = False
        else:
            isExistFlag = True
    else:
        isExistFlag = False
    return isExistFlag


def isExistFile(fileName):
    isExistFlag = False
    file_extension = os.path.splitext(os.path.basename(fileName))
    if os.path.isfile(fileName) and file_extension[1] ==  '':
        isExistFlag = True
    else:
        isExistFlag = False
    return isExistFlag


def isValidSpecificYMD(date):
    try:
        return datetime.datetime.strptime(date, "%Y/%m/%d")
    except ValueError:
        msg = "Incorrect data format, should be YYYY-MM-DD: '{0}'.".format(date)
        raise argparse.ArgumentTypeError(msg)


def splitDateTime(ymd):
    financepath=""
    dt = parse(str(ymd))
    if len(str(dt.month)) == 1:
        month = "0" + str(dt.day)
        financepath = csvpath + '/' + str(dt.year)  + '/' + month + '/' + str(dt.day)
        if len(str(dt.day)) == 1:
            day = "0" + str(dt.day)
            financepath = csvpath + '/' + str(dt.year)  + '/' + month + '/' + day
    else:
        if len(str(dt.day)) == 1:
            day = "0" + str(dt.day)
            financepath = csvpath + '/' + str(dt.year)  + '/' + str(dt.month) + '/' + day
        else:
            financepath= csvpath + '/' + str(dt.year)  + '/' + str(dt.month)  + '/' + str(dt.day)
    return financepath


def addStockItem(output, filename):
    output[0]['stock_code']=getStockCd(filename)
    output[0]['request_publish_date']=getRequestPublishDate(filename)
    output[0]['producer_send_datetime']=datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    return output

def getStockCd(filename):
    filepath = os.path.basename(filename).split("_")
    return filepath[1]

def getRequestPublishDate(filename):
    filepath = os.path.basename(filename).split("_")
    return filepath[2]


def getFileLocation(filename):
    filepath = os.path.dirname(filename).split("/")
    return filepath[7] + filepath[8] + filepath[9] + filepath[10] + filepath[11] 


def convertAvro(data, schema):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write((json.loads(str(data).replace("'", '"'))), encoder) #dict
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes

def getJsonString(data):
    tmp=json.dumps([dict(finance=kv) for kv in data])
    strjson=json.loads(tmp)
    return strjson[0]['finance']



def loadFinanceInfo(producer, csvpath, schema):
    seenFiles = False
    if csvpath is not None:
        files = getFilesToWorkOn(csvpath)
        for filename in files:
            if isExistFile(filename):
                logger.info('scan file : %s', filename)
                with open(filename, encoding='utf-8') as f: 
                    cf = csv.reader(f) 
                    output = []
                    output = [dict(zip(columkeys, property)) for property in cf]
                    output = addStockItem(output, filename)
                    json=getJsonString(output)
                    raw_bytes=convertAvro(json, schema)
                    producer.send(kafka_topic, raw_bytes)
                    #producer.send_messages(kafka_topic, raw_bytes)
                time.sleep(0)
                seenFiles = True


class Producer(threading.Thread):
    def __init__(self, *args):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.args=args

        
    def stop(self):
        self.stop_event.set()

    def run(self):
        logger.info('kafka producer start ')
        #producer = SimpleProducer(kafka)
        producer = KafkaProducer(bootstrap_servers=kafka_host)

        crawlingTime = None
        schema=getAvroSchema(schema_registry_uri)

        #while True:
        while not self.stop_event.is_set():
            if isValidSpecificYMD(tdate):
                dt = parse(tdate)
                #if crawlingTime is None:
                crawlingTime =  dt + datetime.timedelta(days=0)
                #else:
                    #crawlingTime =  crawlingTime + datetime.timedelta(days=interval) 
                # find stock folder (csv)
                financepath=splitDateTime(crawlingTime)
                if isExistDirectory(financepath):
                    loadFinanceInfo(producer, financepath, schema)
            logger.info('alive kafka producer, current time = %s, path = %s', crawlingTime, financepath)
            time.sleep(10)
        producer.close()
        logger.info('kafka finance producer - daemon stop')


def main():
    tasks = [
        Producer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)
    
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()

def get_module_logger(logname):
    logfile = loghome + '/finance_' +  datetime.datetime.now().strftime("%Y%m%d_%H%M%S") +'.log'
    logger = logging.getLogger(logname)
    handler = logging.FileHandler(logfile)
    log_format = ('%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s')
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="kafka Producer Python API is publish Stock Info to kakfa broker ")
    parser.add_argument('--topic', type=str, required=True, default='', help=' kafka topic')
    parser.add_argument('--date', type=str, required=True, default='', help='yyyy/mm/dd or yyyy/mm/dd HH:mm ')
    parser.add_argument('--interval', type=int, required=False, default=5, help='monitoring interval second')
    parser.add_argument('--log', type=str, required=False, default='true', help=r"show console csv file, content, json dumps")

    args = parser.parse_args()
    kafka_topic = args.topic
    tdate = args.date
    interval=args.interval
    log = args.log.lower()

    schema_registry_uri="http://localhost:9090/api/v1/schemaregistry/schemas/finance/versions/latest"

    logger = get_module_logger('Kafka Finance Produce')


    if log not in {'true', 'false'}:
        logger.info("kafka_finace_producer.py: error: the following argument value is only true, false: --log")
        exit(1)

    main()

