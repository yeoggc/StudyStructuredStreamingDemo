package com.ggc.structured_streaming.outputsink

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ConsoleSinkDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName(getClass.getSimpleName)
            .getOrCreate()
        import spark.implicits._
        // 1. 从载数据数据源加
        val lines: DataFrame = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 10000)
            .load
        

        //风格一：DSL
//        val wordCount = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()

        //风格二：SQL
        lines.as[String].flatMap(_.split(" ")).createOrReplaceTempView("w")
        val wordCount = spark.sql(
            """
              |select
              | *
              |from w
            """.stripMargin)
        // 2. 输出
        val result: StreamingQuery = wordCount.writeStream
            .format("console")
            .outputMode("append")   // complete append update
            .trigger(Trigger.ProcessingTime("2 seconds"))
            .start
        
        result.awaitTermination()
        spark.stop()
    }
}
