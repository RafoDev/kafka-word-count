# Instructions

## Download Kafka (all nodes)

```shell
wget https://archive.apache.org/dist/kafka/2.7.0/kafka_2.13-2.7.0.tgz
tar -xzf kafka_2.13-2.7.0.tgz
mv kafka_2.13-2.7.0 kafka   
```
## Optional config for ssh

In /etc/sshd/sshd_config add:

```bash
ClientAliveInterval 300
ClientAliveCountMax 10
```

## Brokers config

### Broker 1 (zookeeper-server)

Config data dir in zookeeper.properties`:

```shell
nano kafka/config/zookeeper.properties
mkdir zookeeper-data
```
Then modify:

```shell
dataDir=./zookeeper-data
```
Finally, start the server:

```shell
kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties
```

### Broker 2 - 3

Config broker.id and dir in `server.properties`:

```shell
nano kafka/config/server.properties
```

Modify (n needs to be different):

```bash
broker.id=n
log.dirs=\kafka-logs
zookeeper.connect=<zookeeper-server-ip>:2181
```
Start the server:

kafka/bin/kafka-server-start.sh kafka/config/server.properties


## Producer - Consumer

### Create a topic

```shell
kafka/bin/kafka-topics.sh --create --bootstrap-server 172.31.37.52:9092 --replication-factor 2 --partitions 2 --topic test-server
```

### Producer

```
kafka/bin/kafka-console-producer.sh --broker-list 172.31.37.52:9092 --topic test-server
```

### Consumer
```
kafka/bin/kafka-console-consumer.sh --bootstrap-server 172.31.37.52:9092 --topic test-server --from-beginning
```
