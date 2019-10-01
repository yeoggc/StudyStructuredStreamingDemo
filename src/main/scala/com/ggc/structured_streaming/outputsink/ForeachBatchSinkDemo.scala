package com.ggc.structured_streaming.outputsink

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object ForeachBatchSinkDemo extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val lines: DataFrame = spark.readStream
    .format("socket") // 设置数据源
    .option("host", "localhost")
    .option("port", 10000)
    .load

  val wordCount: DataFrame = lines.as[String]
    .flatMap(_.split("\\W+"))
    .groupBy("value")
    .count()

  val props = new Properties()
  props.setProperty("user", "root")
  props.setProperty("password", "111111")

  wordCount.writeStream
    .outputMode("complete")
    .foreachBatch((df, batchId) => { // 当前分区id, 当前批次id
      if (df.count() != 0) {
        df.persist()
        //保存到文件
        df.write.mode("overwrite").json(s"./outputPath/ForeachBatchSinkDemo")
        //保存到数据库
        df.write
          .mode("overwrite")
          .jdbc("jdbc:mysql://dw1:3306/SparkStructuredStreamingDB",
            "word_count",
            props)
        df.unpersist()
      }

    })
    .start().awaitTermination()

}
