package com.practice.transform

import com.practice.entity.SensorRecord
import org.apache.flink.streaming.api.scala._

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_transform_file {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(2)从文件读取
    val stream1 = env.readTextFile("D:\\development_tools\\ideaprojects\\flinkDemo\\flinkDemo01\\src\\main\\resources\\sensor.txt")

    val dataStream = stream1.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorRecord(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).keyBy("id") //.sum("temperature")
      .reduce((x, y) => SensorRecord(x.id, x.timestamp.min(y.timestamp) + 5, x.temperature + y.temperature))

    //sink输出
    dataStream.print("data transform").setParallelism(1)
    env.execute("API file source")
  }

}



