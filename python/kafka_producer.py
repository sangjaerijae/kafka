#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import threading, logging, time
import multiprocessing

from dateutil.parser import parse
from json import dumps
from os.path import basename
from kafka import KafkaProducer
from kafka.errors import KafkaError

csvpath="/stock"

columkeys = ("col1","col2","col3","col4","col5","col6","col7","col8","col9","col10",
    "col11","col12","col13","col14","col15","col16","col17","col18","col19","col20",
    "col21","col22","col23","col24","col25","col26","col27","col28","col29","col30",
    "col31","col32","col33","col34","col35","col36","col37","col38","col39","col40",
    "col41","col42","col43")

kafkaHost="10.233.5.15:9092"


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
    if os.path.isfile(fileName):
        isExistFlag = True
    else:
        isExistFlag = False
    return isExistFlag


def isValidSpecificYMD(date):
    try:
        return datetime.datetime.strptime(date, "%Y/%m/%d %H:%M")
    except ValueError:
        msg = "Incorrect data format, should be YYYY-MM-DD: '{0}'.".format(date)
        raise argparse.ArgumentTypeError(msg)


def splitDateTime(ymd):
    stockpath=""
    dt = parse(str(ymd))
    if len(str(dt.hour)) == 1 or len(str(dt.minute)) == 1 :
        hour = "0" + str(dt.hour)
        minute = "0" + str(dt.minute)
        stockpath= csvpath + '/' + str(dt.year)  + '/' + str(dt.month)  + '/' + str(dt.day)  + '/' + hour  + '/' + minute 
    else:
        stockpath= csvpath + '/' + str(dt.year)  + '/' + str(dt.month)  + '/' + str(dt.day)  + '/' + str(dt.hour)  + '/' + str(dt.minute)
    return stockpath


def sendFromCsv(producer, filename):
    with open(filename, encoding='utf-8') as f: 
        reader = csv.reader(f) 
        for row in reader: 
            data = json.dumps(row, ensure_ascii=False)
            print("csv record : ",data)

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
    output[0]['stockid']=getStockId(filename)
    output[0]['fileloc']=getFileLocation(filename)
    output[0]['publish_datetime']=datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    return output

def getStockId(filename):
    filepath = os.path.basename(filename).split("_")
    return filepath[1]


def getFileLocation(filename):
    filepath = os.path.dirname(filename).split("/")
    return filepath[7] + filepath[8] + filepath[9] + filepath[10] + filepath[11] 


def loadStockInfo(producer, csvpath):
    seenFiles = False
    if csvpath is not None:
        files = getFilesToWorkOn(csvpath)
        for filename in files:
            if isExistFile(filename):
                print( "scan file : " + filename )
                with open(filename, encoding='utf-8') as f: 
                    reader = csv.reader(f) 
                    output = []
                    output = [dict(zip(columkeys, property)) for property in reader]
                    output=addStockItem(output, filename)
                    data = json.dumps(output, ensure_ascii=False)
                    producer.send(kafka_topic, data.encode('utf-8'))
                time.sleep(1)
                seenFiles = True


class Producer(threading.Thread):
    def __init__(self, *args):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.args=args

        
    def stop(self):
        self.stop_event.set()

    def run(self):
        print('Args are: {}'.format(self.args))
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        stocktime=None
        while True:
        #while not self.stop_event.is_set():
            if isValidSpecificYMD(tdate):
                dt = parse(tdate)
                if stocktime is None:
                    stocktime =  dt + datetime.timedelta(seconds=interval) 
                else:
                    stocktime =  stocktime + datetime.timedelta(seconds=interval) 
                # find stock folder (csv)
                stockpath=splitDateTime(stocktime)
                if isExistDirectory(stockpath):
                    loadStockInfo(producer, stockpath)
            print("alive kafka producer, current time = {0}, path = {1}".format(stocktime, stockpath))
            time.sleep(10)
        producer.close()

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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="kafka Producer Python API is publish Stock Info to kakfa broker ")
    parser.add_argument('--topic', type=str, required=True, default='', help=' kafka topic')
    parser.add_argument('--date', type=str, required=True, default='', help='yyyy/mm/dd or yyyy/mm/dd HH:mm ')
    parser.add_argument('--interval', type=int, required=False, default=5, help='monitoring interval second')
    parser.add_argument('--log', type=str, required=False, default='true', help=r"show console csv file, content, json dumps")

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

    args = parser.parse_args()
    kafka_topic = args.topic
    tdate = args.date
    interval=args.interval

    log = args.log.lower()

    if log not in {'true', 'false'}:
        print("kafka_producer.py: error: the following argument value is only true, false: --log")
        exit(1)

    main()

