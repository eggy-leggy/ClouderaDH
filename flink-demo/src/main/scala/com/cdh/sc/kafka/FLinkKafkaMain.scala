package com.cdh.sc.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}


object FLinkKafkaMain {
  def main(args: Array[String]): Unit = {
    /**
      * flink 流处理分为 source  -> transaction  -> sink
      * source：获取数据源位置
      * transaction： 数据处理逻辑
      * sink ： 数据目标位置
      */
    // flink初始化
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //配置文件 kafka source配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node1:9092")

    //    properties.setProperty("zookeeper.connect", "node1:2181") // only required for Kafka 0.8
    properties.setProperty("group.id", "test") // kafka group 同一group不会重复获取数据

    //    flink官方提供 kafka 消费者连接器
    val myConsumer = new FlinkKafkaConsumer[String]("connection_test", new SimpleStringSchema(), properties)
    import org.apache.flink.streaming.api.scala._ // scala 一种声明  没有此生面 scala表达式 无法编译通过
    //    定义source 源 kafka
    val stream = env.addSource(myConsumer)
    stream.print() // 打印source获取数据
    //    定义transaction
    val result = stream.map(x => "hello " + x) // 在数据前面增加 hello
    result.print()
    //    flink官方提供 kafka 生产者连接器
    val myProducer = new FlinkKafkaProducer[String](
      "node1:9092", // broker list
      "channel2", // target topic
      new SimpleStringSchema) // serialization schema

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(true)
    //    定义 sink 目标kafka
    result.addSink(myProducer)

    //    执行flink 流处理
    env.execute()
  }
}
