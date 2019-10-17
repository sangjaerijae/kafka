##### Multi Broker On Single Node ####
#   kakfa (3 )   +   Zookeepr ( 1 )   #
######################################

1. Preliminary work
- install docker, docker-compose

2. Create a docker-compose file
- See Docker-compose.yml

3. Execute docker-compose
- Docker-compose up -d

4. Kakfa Test
 1) Create Topic
$docker exec -it docker_kafka1 /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 3 --topic kafka-test

 2)Send some messages
$docker exec -it docker_kafka2_1 /opt/kafka/bin/kafka-console-producer.sh --brocker-list kafka3:90 --topic kafka-test

3) start a consumer
$ docker exec -it docker_kafka1_1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka2:9090 --from-beginning --topic kafka-test

4) describe topic
$ docker exec -it docker_kafka2_1 /opt/kafka/bin/kafka-topics.sh --describe  --zookeeper zookeeper:2181 --topic kafka-test



5.  Spark-Stream API Test
 1) send a message 
$ docker exec -it docker_kafka2_1 /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka3:9090 --t
>heheyhey
>antonaotob;
>hoodsi
>`akdk
>`akao
>한글

2) spark-shell 
# /opt/mapr/spark/spark-2.4.0/bin/spark-shell --jars /opt/mapr/spark/spark-2.4.0/jars/spark-streaming-kafka-0-10_2.11-2.4.0.0-mapr-620.jar

SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/mapr/zeppelin/zeppelin-0.8.1/interpreter/spark/spark-interpreter-
SLF4J: Found binding in [jar:file:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/lib/slf4j-log4j12-1.
SLF4J: Found binding in [jar:file:/opt/mapr/lib/slf4j-log4j12-1.7.12.jar!/org/slf4j/impl/StaticLoggerBin
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
19/10/03 07:56:33 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using
Spark context Web UI available at http://10.233.5.15:11002
Spark context available as 'sc' (master = local[*], app id = local-1570089399868).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.0.0-mapr-620
      /_/
         
Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_212)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val ds1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").op
ds1: org.apache.spark.sql.DataFrame = [key: binary, value: binary ... 5 more fields]

scala> val sql = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
sql: org.apache.spark.sql.Dataset[(String, String)] = [key: string, value: string]

scala> val query = sql.writeStream.format("console").start()
query: org.apache.spark.sql.streaming.StreamingQuery = org.apache.spark.sql.execution.streaming.Streamin

scala> -------------------------------------------
Batch: 0
-------------------------------------------
+---+-----+
|key|value|
+---+-----+
+---+-----+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+----+--------+
| key|   value|
+----+--------+
|null|heheyhey|
+----+--------+

-------------------------------------------
Batch: 2
-------------------------------------------
+----+-----------+
| key|      value|
+----+-----------+
|null|antonaotob;|
+----+-----------+

-------------------------------------------
Batch: 3
-------------------------------------------
+----+------+
| key| value|
+----+------+
|null|hoodsi|
+----+------+

-------------------------------------------
Batch: 4
-------------------------------------------
+----+-----+
| key|value|
+----+-----+
|null|`akdk|
+----+-----+

-------------------------------------------
Batch: 5
-------------------------------------------
+----+-----+
| key|value|
+----+-----+
|null|`akao|
+----+-----+

-------------------------------------------
Batch: 6
-------------------------------------------
+----+-----+
| key|value|
+----+-----+
|null| 한글|
+----+-----+


References 
 https://medium.com/@marcelo.hossomi/running-kafka-in-docker-machine-64d1501d6f0b
 https://jaceklaskowski.gitbooks.io/apache-kafka/kafka-docker.html
 https://github.com/wurstmeister/kafka-docker/wiki/Connectivity
