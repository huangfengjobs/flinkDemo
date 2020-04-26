package com.practice.sink

import java.util
import java.util.Properties

import com.practice.entity.SensorRecord
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_sink_es {

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

    //es sink
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop105", 9200))
    //创建esSink的Builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorRecord](httpHosts, new ElasticsearchSinkFunction[SensorRecord] {
      override def process(t: SensorRecord, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("saving data: " + t)
        val json = new util.HashMap[String, String]()
        json.put("sensor_id", t.id)
        json.put("timestamp", t.timestamp.toString)
        json.put("temperature", t.temperature.toString)
        //创建index request
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")
      }
    })

    dataStream.addSink(esSinkBuilder.build())

    env.execute("API")
  }

}
