package com.anthony.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Description:使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
  * @ Date: Created in 09:41 2018/4/5
  * @ Author: Anthony_Duan
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    result.print()

    result.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecords=>{
        val connection = createConnection()
        partitionOfRecords.foreach(record=>{
          //scala字符串插值
          val sql1 = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"

          val sql2 = s"insert into wordcount(word, wordcount) values('${record._1}',${record._2})"//注意'${record._1}' 两边的单引号
          println("Sql1:"+sql1)
          println("Sql2:"+sql2)

          connection.createStatement().execute(sql2)

        })

        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark","root","xiaoduan")
  }

}
