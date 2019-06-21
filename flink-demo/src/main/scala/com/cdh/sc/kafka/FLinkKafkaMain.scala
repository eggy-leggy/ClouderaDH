package com.cdh.sc.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}


object FLinkKafkaMain {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node1:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "node1:2181")
    properties.setProperty("group.id", "test")

    val myConsumer = new FlinkKafkaConsumer[String]("connection_test", new SimpleStringSchema(), properties)
    import org.apache.flink.streaming.api.scala._
    val stream = env.addSource(myConsumer)
    stream.print()
    val result = stream.map(x=>"hello " + x)
    result.print()

    val myProducer = new FlinkKafkaProducer[String](
      "node1:9092", // broker list
      "channel2", // target topic
      new SimpleStringSchema) // serialization schema

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(true)
    result.addSink(myProducer)
    env.execute()
  }
}
