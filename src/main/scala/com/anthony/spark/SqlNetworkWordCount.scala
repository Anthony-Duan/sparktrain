package com.anthony.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @ Description:
  * @ Date: Created in 11:33 2018/4/5
  * @ Author: Anthony_Duan
  */
object SqlNetworkWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(4))

    val lines = ssc.socketTextStream("localhost",6789)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD{rdd=>
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val wordDF = rdd.map(w => Record(w)).toDF()

      wordDF.show()


    }

    ssc.start()
    ssc.awaitTermination()

  }

  case class Record(word: String)

}
