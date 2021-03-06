package com.anthony.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Description:
  * @ Date: Created in 10:18 2018/4/6
  * @ Author: Anthony_Duan
  */
object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    println(messages)
    messages.map(_._2).flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_).print()


    ssc.start()
    ssc.awaitTermination()


  }

}
