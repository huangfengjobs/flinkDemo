package com.practice.sink

import java.util.Properties

import com.practice.entity.SensorRecord
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_sink_kafka {

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

    val kafkaDStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val dataStream: DataStream[SensorRecord] = kafkaDStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorRecord(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //拆分流
    val splitDstream: SplitStream[SensorRecord] = dataStream.split(data => {
      if (data.temperature > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })
    //获取分流
    val high: DataStream[SensorRecord] = splitDstream.select("high")
    val low: DataStream[SensorRecord] = splitDstream.select("low")
    val all: DataStream[SensorRecord] = splitDstream.select("high", "low")

    //union
    val unionDstream: DataStream[String] = high.union(low).map(_.temperature.toString)

    //sink输出
    //high.print("high").setParallelism(1)
    //low.print("low").setParallelism(1)
    //dataStream.print("all").setParallelism(1)
    unionDstream.print("unionDstream").setParallelism(1)

    //kafka sink
    unionDstream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "test", new SimpleStringSchema()))

    env.execute("API")
  }

}
