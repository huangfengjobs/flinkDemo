package com.practice.source

import org.apache.flink.streaming.api.scala._

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_source_file {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(2)从文件读取
    val stream1 = env.readTextFile("D:\\development_tools\\ideaprojects\\flinkDemo\\flinkDemo01\\src\\main\\resources\\sensor.txt")
    //sink输出
    stream1.print("file stream").setParallelism(1)
    env.execute("API file source")
  }

}
