# Kafka Hands On

## Initial Setup for Kafka

Run Zookeeper
```shell
cd kafka_2.12-2.0.0
zookeeper-server-start.sh config/zookeeper.properties
```

Run Kafka
```shell
cd kafka_2.12-2.0.0
kafka-server-start.sh config/server.properties
```

Consumer
```shell
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first-topic
```

Consumer Group
```shell
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first-topic --group my-first-application
```

Producer
```shell
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic first-topic
```

