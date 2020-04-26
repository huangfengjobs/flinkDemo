package com.practice.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_source_kafka {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(3)从Kafka读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop105:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaDStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    //sink输出
    kafkaDStream.print("kafka stream").setParallelism(1)

    env.execute("API kafka source")
  }

}

