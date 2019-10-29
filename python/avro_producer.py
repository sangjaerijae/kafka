import io
import random
import avro.schema
from avro.io import DatumWriter
from kafka import SimpleProducer
from kafka import KafkaClient

# To send messages synchronously
KAFKA = KafkaClient('localhost:9092')
PRODUCER = SimpleProducer(KAFKA)

# Kafka topic
TOPIC = "avro"

# Path to user.avsc avro schema
SCHEMA_PATH = "user.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

for i in range(10):
    writer = DatumWriter(SCHEMA)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"name": "日本語", "favorite_color": "ストリームデータ", "favorite_number": random.randint(0, 10)}, encoder)
    raw_bytes = bytes_writer.getvalue()
    PRODUCER.send_messages(TOPIC, raw_bytes)

###############################################
#1 get avro schema registry url
#2 convert csv file to json records
#3 convert json records to avro records
#4 send avro message to topic kafka broker
###############################################