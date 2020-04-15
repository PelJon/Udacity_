# Udacity SF Crime Statistics with Spark Streaming

## 1. Introduction

In this project, you will be provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, and you will provide statistical analyses of the data using Apache Spark Structured Streaming. You will draw on the skills and knowledge you've learned in this course to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

## 2. Run the project

1. Start by installing necessary libraries and packages

```
./start.sh
```

2. Start Zookeeper and your Kafka Server using the following commands:

```
/usr/bin/zookeeper-server-start ./config/zookeeper.properties
```
```
/usr/bin/kafka-server-start ./config/server.properties
```

3. Run the kafka_producer.py to initialize the producer and the topic
4. Test if topic creation was successful and topic is listed
```
kafka-topics --list --zookeeper localhost:2181
```

5. Run the consumer_server.py to get consumed messages and check correctness

## 3. Screenshots

## 4. Requirements

* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka build with Scala 2.11.x
* Python 3.6.x or 3.7.x

## 5. Respond to questions for successful project submission
