package com.practice.wc

import org.apache.flink.api.scala._

/**
  * @author Huangfeng
  * @create 2020-04-25 上午 10:54
  */

/**
  * 数据批处理
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中获取数据源
    val inputPath = "D:\\development_tools\\ideaprojects\\flinkDemo\\flinkDemo01\\src\\main\\resources\\hello.txt"
    //转换操做，做聚合统计
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    //输出
    wordCountDS.print()
  }
}
