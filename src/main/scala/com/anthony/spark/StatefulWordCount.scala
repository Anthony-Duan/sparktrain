package com.anthony.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Description: 使用StatefulWordCount 带状态的算子
  * @ Date: Created in 16:36 2018/4/4
  * @ Author: Anthony_Duan
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey(updateFunction _) //最后的 _ 是隐式转换

    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把当前的数据去更新已有的或者老的数据
    * @param currentValues 当前的数据
    * @param preValues 老的数据或者是已有的数据
    * @return
    */
  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int]={
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current+pre)
  }


}
