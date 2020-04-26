package com.practice.source

import com.practice.entity.SensorRecord
import org.apache.flink.streaming.api.scala._

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_source_list {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(1)从集合读取
    val stream1 = env.fromCollection(List(
      SensorRecord("sensor_1", 1547718199, 35.80018327300259),
      SensorRecord("sensor_6", 1547718201, 15.402984393403084),
      SensorRecord("sensor_7", 1547718202, 6.720945201171228),
      SensorRecord("sensor_10", 1547718205, 38.101067604893444)
    ))
    //sink输出
    stream1.print("list stream").setParallelism(1)
    env.execute("API list source")
  }

}
