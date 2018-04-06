package com.anthony.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Description: 使用StatefulWordCount 带状态的算子
  * @ Date: Created in 16:36 2018/4/4
  * @ Author: Anthony_Duan
  */
object WordCountAndWindow {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(4), Seconds(2))

    state.print()
    ssc.start()
    ssc.awaitTermination()
  }




}
