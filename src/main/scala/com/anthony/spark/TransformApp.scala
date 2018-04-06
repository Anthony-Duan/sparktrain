package com.anthony.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Description:
  * @ Date: Created in 10:55 2018/4/5
  * @ Author: Anthony_Duan
  */
object TransformApp {

  def main(args: Array[String]): Unit = {

    val saprkConf = new SparkConf().setAppName("TransformApp").setMaster("local[2]")
    val ssc = new StreamingContext(saprkConf,Seconds(2))

    val blacks = List("zs","ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map((_,true))
//    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))


    val lines = ssc.socketTextStream("localhost",6789)

    val clicklog = lines.map(x=>(x.split(",")(1),x)).transform(
      rdd=>{
        rdd.leftOuterJoin(blacksRDD)
          .filter(x=> x._2._2.getOrElse(false)!=true)
          .map(x=>x._2._1)
      }
    )


    clicklog.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
