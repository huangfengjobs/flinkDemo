package com.practice.transform

import com.practice.entity.SensorRecord
import org.apache.flink.streaming.api.scala._

/**
  * @author Huangfeng
  * @create 2020-04-25 下午 4:52
  */
object Sensor_transform_union {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(2)从文件读取
    val stream1 = env.readTextFile("D:\\development_tools\\ideaprojects\\flinkDemo\\flinkDemo01\\src\\main\\resources\\sensor.txt")

    val dataStream = stream1.map(data => {
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
    val unionDstream: DataStream[SensorRecord] = high.union(low)

    //sink输出
    //high.print("high").setParallelism(1)
    //low.print("low").setParallelism(1)
    //dataStream.print("all").setParallelism(1)
    unionDstream.print("unionDstream").setParallelism(1)

    env.execute("API")
  }

}
