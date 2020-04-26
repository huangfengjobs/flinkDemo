package com.practice.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


/**
  * @author Huangfeng
  * @create 2020-04-25 上午 11:09
  */
/**
  * 流处理
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //创建一个流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流式数据
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")
    val socketStream: DataStream[String] = env.socketTextStream(host,port)
    //转换操做
    val dataStream: DataStream[(String, Int)] = socketStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    //输出
    dataStream.print().setParallelism(1)
    //启动executor
    env.execute()
  }
}
