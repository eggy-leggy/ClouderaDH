# 创建topic
kafka-topics.sh --create --topic connection_test --replication-factor 1 --partitions 3 --zookeeper node1:2181,node2:2181,node3:2181


# 查看topic信息
kafka-topics.sh --describe --topic connection_test --zookeeper node1:2181,node2:2181,node3:2181


# 查看所有topic
kafka-topics.sh --list --zookeeper node1:2181,node2:2181,node3:2181


# 消息发送者
kafka-console-producer.sh --topic connection_test --broker-list node1:9092,node2:9092,node3:9092


# 消息消费者
kafka-console-consumer.sh --topic connection_test --from-beginning --zookeeper node1:2181,node2:2181,node3:2181
kafka-console-consumer.sh --topic channel2 --from-beginning --zookeeper node1:2181,node2:2181,node3:2181