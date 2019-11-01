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
import uuid
import json
import time
import requests
import threading, logging, time
import multiprocessing

from avro.io import DatumWriter
from dateutil.parser import parse
from json import dumps
from json import loads
from os.path import basename
from kafka import KafkaProducer
from kafka.errors import KafkaError

csvpath="/mapr/mapr.sbihd.com/user/mapr/kafka/stock"
loghome="/mapr/mapr.sbihd.com/user/mapr/kafka/python/log"

columkeys = (
    "company_name","stock_price","per","pbr","yield","credit_ratio","previous_closing_price","starting_price","high_price","low_price",
    "trading_price","vwap","contract_number","unit_price","min_purchase_price","shares_per_unit_number","total_market_value"
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
    #prevent to extension(.ferr.x) from read file
    file_extension = os.path.splitext(os.path.basename(fileName))
    if os.path.isfile(fileName) and file_extension[1] ==  '':
    #if os.path.isfile(fileName):
        isExistFlag = True
    else:
        isExistFlag = False
    return isExistFlag


def isValidSpecificYMD(date):
    try:
        return datetime.datetime.strptime(date, "%Y/%m/%d %H:%M")
    except ValueError:
        msg = "Incorrect data format, should be YYYY/MM/DD HH:MM '{0}'.".format(date)
        raise argparse.ArgumentTypeError(msg)


#def splitDateTime(ymd):
#    stockpath=""
#    dt = parse(str(ymd))
#    if len(str(dt.hour)) == 1 or len(str(dt.minute)) == 1 :
#        hour = "0" + str(dt.hour)
#        minute = "0" + str(dt.minute)
#        stockpath= csvpath + '/' + str(dt.year)  + '/' + str(dt.month)  + '/' + str(dt.day)  + '/' + hour  + '/' + minute 
#    else:
#        stockpath= csvpath + '/' + str(dt.year)  + '/' + str(dt.month)  + '/' + str(dt.day)  + '/' + str(dt.hour)  + '/' + str(dt.minute)
#    return stockpath

def splitDateTime(ymd):
    stockpath=""
    dt = parse(str(ymd))

    month, day, hour, minute =  ["" for _ in range(4)]

    if len(str(dt.month)) == 1:
        month = "0" + str(dt.month)
    else:
        month = str(dt.month)
    if len(str(dt.day)) == 1:
        day = "0" + str(dt.day)
    else:
        day = str(dt.day)
    if len(str(dt.hour)) == 1:
        hour = "0" + str(dt.hour)
    else:
        hour = str(dt.hour)
    if len(str(dt.minute)) == 1:
        minute = "0" + str(dt.minute)
    else:
        minute = str(dt.minute)

    stockpath = csvpath + '/' + str(dt.year)  + '/' + month + '/' + day + '/' + hour + '/' + minute
    
    return stockpath



def sendFromCsv(producer, filename):
    with open(filename, encoding='utf-8') as f: 
        reader = csv.reader(f) 
        for row in reader: 
            data = json.dumps(row, ensure_ascii=False)
            #print("csv record : ",data)

            #kafka_topic="stock"
            if type(kafka_topic) == bytes:
                kafka_topic = kafka_topic.decode('utf-8')

            # Block for 'synchronous' sends
            try:
                # Asynchronous by default
                future = producer.send(kafka_topic, data)

                record_metadata = future.get(timeout=1)
            except KafkaError:
                # Decide what to do if produce request failed...
                logger.exception("failed to error sending kafka producer")
                pass

            # Successful result returns assigned partition and offset
            print (record_metadata.topic)
            print (record_metadata.partition)
            print (record_metadata.offset)

            kafka_producer.flush()

            # Note that the application is responsible for encoding messages to type str
            #response = producer.send_messages("csv-data", line)
            #if response:
                #print(response[0].error)
                #print(response[0].offset)

def addStockItem(output, filename):
    output[0]['stock_code']=getStockCd(filename)
    output[0]['request_publish_date']=getRequestPublishDate(filename)
    output[0]['producer_send_datetime']=datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    output[0]['seqid']=getIdentifier()
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


def getIdentifier():
    return str(uuid.uuid4())

def convertAvro(data, schema):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write((json.loads(str(data).replace("'", '"'))), encoder) #dict
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes


def getJsonString(data):
    tmp=json.dumps([dict(stock=kv) for kv in data])
    strjson=json.loads(tmp)
    return strjson[0]['stock']


def loadStockInfo(producer, csvpath, schema):
    seenFiles = False
    if csvpath is not None:
        files = getFilesToWorkOn(csvpath)
        for filename in files:
            if isExistFile(filename):
                logger.info('scan file : %s', filename)
                #print('scan file : %s', filename)
                with open(filename, encoding='utf-8') as f: 
                    reader = csv.reader(f) 
                    output = []
                    output = [dict(zip(columkeys, property)) for property in reader]
                    output=addStockItem(output, filename)
                    json=getJsonString(output)
                    #data = json.dumps(output, ensure_ascii=False)
                    logger.info(' %s ', json)
                    raw_bytes=convertAvro(json, schema)
                    producer.send(kafka_topic, raw_bytes)
                    #producer.send(kafka_topic, data)
                time.sleep(0)
                seenFiles = True


def getCurrentTime():
    dt = None
    if tdate is not None and len(tdate) >= 1:
        dt = parse(tdate)
    else:
        dt = parse((datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime("%Y/%m/%d %H:%M:%S"))
    return dt


def sleepDaemon(stockpath):
    isSleep = False

    #sleeptime = parse(str(stocktime))
    filepath = os.path.dirname(stockpath).split("/")

    if (filepath[10] == "09" or filepath[10] == "10" or filepath[10] == "11" or filepath[10] == "13" or filepath[10] == "14"):
    #if (filepath[10] == "12" or filepath[10] == "15"):
        if os.path.exists(stockpath) and os.path.isdir(stockpath):
            isSleep = False
        else:
            isSleep = True
    else:
        isSleep = True

    return isSleep 


class Producer(threading.Thread):
    def __init__(self, *args):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.args=args

   
    def stop(self):
        #logger.info("Thread Stop Event")
        self.stop_event.set()


    def run(self):
        logger.info('kafka stock producer start ')
        time.sleep(15) # delay starttime (folder access = delay starttime + crontab starttime)
        #producer = KafkaProducer(bootstrap_servers=kafkaHost, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producer = KafkaProducer(bootstrap_servers=kafka_host)

        stocktime=None
        schema=getAvroSchema(schema_registry_uri)

        while True:
        #while not self.stop_event.is_set():
            if stocktime is None:
                stocktime = getCurrentTime()
            else:
                stocktime =  stocktime + datetime.timedelta(seconds=60)  # increase 1 minutes  to access  csv folder

            stockpath=splitDateTime(stocktime)

            if isExistDirectory(stockpath):
                loadStockInfo(producer, stockpath, schema)

            logger.info('alive kafka producer, current time = %s, path = %s', stocktime, stockpath)
            #print('alive kafka producer, current time = %s, path = %s', stocktime, stockpath)
            
            if sleepDaemon(stockpath):
                #logger.info("Not found stock folder is %s", stockpath)
                print("Not found stock folder is %s", stockpath)
                break
            else:
                logger.info("skip next folder .... sleep time = %s", interval) 
                #print("skip next folder .... sleep time = ", interval) 
                time.sleep(interval) # increase 1 minutes (update time is created csv file)  to access  csv folder

        producer.close()
        logger.info('kafka stock producer close - daemon stopped')
        #print('kafka stock producer close - daemon stopped')

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
    logfile = loghome + '/stock_' +  datetime.datetime.now().strftime("%Y%m%d_%H%M%S") +'.log'
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
    parser.add_argument('--date', type=str, required=False, default='', help='yyyy/mm/dd or yyyy/mm/dd HH:mm ')
    parser.add_argument('--interval', type=int, required=False, default=5, help='monitoring interval second')
    parser.add_argument('--log', type=str, required=False, default='true', help=r"show console csv file, content, json dumps")

    args = parser.parse_args()
    kafka_topic = args.topic
    interval=args.interval
    tdate = args.date
    log = args.log.lower()

    schema_registry_uri="http://localhost:9090/api/v1/schemaregistry/schemas/stock/versions/latest"

    logger = get_module_logger('Kafka Stock Produce')

    if log not in {'true', 'false'}:
        print("kafka_producer.py: error: the following argument value is only true, false: --log")
        exit(1)

    main()

