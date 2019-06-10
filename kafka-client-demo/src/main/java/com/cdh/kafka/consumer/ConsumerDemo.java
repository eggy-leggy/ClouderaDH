package com.cdh.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static String TOPIC_NAME = "connection_test";
    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition partition1 = new TopicPartition(TOPIC_NAME, 1);
        TopicPartition partition2 = new TopicPartition(TOPIC_NAME, 2);

        consumer.assign(Arrays.asList(partition0, partition1, partition2));

        ConsumerRecords<String, String> records = null;

        while (true) {
            try {
                Thread.sleep(10000l);
                System.out.println();
                records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
