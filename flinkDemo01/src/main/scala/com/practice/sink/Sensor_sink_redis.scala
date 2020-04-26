package com.practice.sink

import java.util.Properties

import com.practice.entity.SensorRecord
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_sink_redis {

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

    //dataStream输出
    dataStream.print("dataStream").setParallelism(1)

    //redis sink
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop105").setPort(6379).build()
    dataStream.addSink(new RedisSink[SensorRecord](conf, new MyRedisMapper()))

    env.execute("API")
  }

}

class MyRedisMapper extends RedisMapper[SensorRecord] {
  override def getCommandDescription: RedisCommandDescription = {
    //定义保存到redis时的命令
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  override def getKeyFromData(data: SensorRecord): String = {
    data.id.toString
  }

  override def getValueFromData(data: SensorRecord): String = {
    data.temperature.toString
  }
}
