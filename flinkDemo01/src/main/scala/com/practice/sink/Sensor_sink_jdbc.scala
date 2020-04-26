package com.practice.sink

import java.util.Properties

import com.alibaba.druid.support.ibatis.DruidDataSourceFactory
import com.mysql.jdbc.{Connection, PreparedStatement}
import com.practice.entity.SensorRecord
import javax.activation.DataSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_sink_jdbc {

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

    //jdbc sink
    val jdbcSink = new MyJdbcSink("insert into sensorReading values(?,?,?)")
    dataStream.map(data => Array(data.id, data.timestamp, data.temperature)).addSink(jdbcSink)

    env.execute("API")
  }

}

class MyJdbcSink(sql: String) extends RichSinkFunction[Array[Any]] {

  val driver = "com.mysql.jdbc.Driver"

  val url = "jdbc:mysql://localhost:3306/sensor?useSSL=false"

  val username = "root"

  val password = "123456"

  val maxActive = "20"

  var connection: Connection = null;

  //创建连接
  override def open(parameters: Configuration): Unit = {
    val properties = new Properties()
    properties.put("driverClassName", driver)
    properties.put("url", url)
    properties.put("username", username)
    properties.put("password", password)
    properties.put("maxActive", maxActive)


    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection = dataSource.getConnection()
  }

  //反复调用连接，执行sql
  override def invoke(values: Array[Any]): Unit = {
    // 预编译器
    val ps: PreparedStatement = connection.prepareStatement(sql)
    println(values.mkString(","))
    for (i <- 0 until values.length) {
      // 坐标从1开始
      ps.setObject(i + 1, values(i))
    }
    // 执行操作
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
  }
}
