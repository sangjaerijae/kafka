#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# avro-python3==1.8.2

import io
import avro.schema
import avro.io
import requests
import json
from kafka import KafkaConsumer

# To consume messages
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 #group_id='sample_avro_group',
                                 consumer_timeout_ms=1000)

consumer.subscribe(['avro'])

# read avro schema from registry (port 9090)
resp=requests.get("http://localhost:9090/api/v1/confluent/schemas/ids/1")
SCHEMA=avro.schema.Parse(resp.json()['schema'])

for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    user1 = reader.read(decoder)
    print (user1)
