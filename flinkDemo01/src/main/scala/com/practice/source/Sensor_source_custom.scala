package com.practice.source

import com.practice.entity.SensorRecord
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

/**
  * @author Huangfeng
  * @create 2020-05-03 下午 1:39
  *         自定义source
  */
class Sensor_source_custom extends SourceFunction[SensorRecord] {
  //定义flag，表示数据源是否正常运行
  var running: Boolean = true

  //随机生成SensorRocord数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorRecord]): Unit = {
    //定义一个随机数发生器
    val random = new Random()
    // 生成10个传感器温度，并且不断变化
    val curTemps: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 50 + random.nextGaussian() * 2)
    )
    // 循环生成传感器温度
    while (running) {
      val curTemp: immutable.IndexedSeq[(String, Double)] = curTemps.map(
        data => (data._1, data._2 + random.nextGaussian())
      )
      //样例类包装
      curTemp.foreach(
        data => sourceContext.collect(SensorRecord(data._1, System.currentTimeMillis(), data._2))
      )
      //间隔时间
      Thread.sleep(1000L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

object Sensor_source_custom {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //自定义source
    val stream = env.addSource(new Sensor_source_custom())
    //sink输出
    stream.print("custom source").setParallelism(1)
    env.execute("API file source")
  }
}
