package com.practice.eventTime

import java.util.Properties

import com.practice.entity.SensorRecord
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_eventTime_sliding_window {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //指定时间类型为事件发生时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置默认watermark的周期
    //env.getConfig.setAutoWatermarkInterval(5000)

    //(3)从Kafka读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop105:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaDStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val dataStream = kafkaDStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorRecord(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    //sink输出
    //kafkaDStream.print("kafka stream").setParallelism(1)

    //接收事件的时间是按照顺序的，不需要设置watermark
    //dataStream.assignAscendingTimestamps(_.timestamp * 1000)

    //接收事件的时间是乱序的，需要设置watermark做延迟处理
    //注意：BoundedOutOfOrdernessTimestampExtractor需要设置时间参数，该参数是延迟时间，即watermark
    val minTempPerWindow: DataStream[(String, Double)] = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorRecord](Time.milliseconds(1000)) {
      override def extractTimestamp(t: SensorRecord): Long = {
        //事件发生的时间戳，注意单位是毫秒
        t.timestamp * 1000
      }
    }).map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((left, right) => (left._1, left._2.min(right._2)))

    minTempPerWindow.print("minTempPerWindow").setParallelism(1)

    env.execute("API kafka source")
  }

}

