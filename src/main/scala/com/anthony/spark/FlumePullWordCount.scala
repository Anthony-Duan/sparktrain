package com.anthony.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Description:
  * @ Date: Created in 09:12 2018/4/6
  * @ Author: Anthony_Duan
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length!=2){
      System.err.println("Usage:FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname,port) =args


    val sparkConf = new SparkConf().setAppName("FlumePushWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val flumeStream = FlumeUtils.createPollingStream(ssc,hostname,port.toInt)


    flumeStream.map(x=> new String(x.event.getBody.array()).trim).flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_).print()




    ssc.start()
    ssc.awaitTermination()
  }



}
