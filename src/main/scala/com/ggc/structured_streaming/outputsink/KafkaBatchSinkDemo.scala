package com.ggc.structured_streaming.outputsink

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaBatchSinkDemo extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .getOrCreate()
  import spark.implicits._

  val wordCount: DataFrame = spark.sparkContext.parallelize(Array("hello hello atguigu", "atguigu, hello"))
    .toDF("word")
    .groupBy("word")
    .count()
    .map(row => row.getString(0) + "," + row.getLong(1))
    .toDF("value")  // 写入数据时候, 必须有一列 "value"

  wordCount.write  // batch 方式
    .format("kafka")
    .option("kafka.bootstrap.servers", "dw1:9092,dw2:9092,dw3:9092") // kafka 配置
    .option("topic", "SparkStructuredStreamingTopic") // kafka 主题
    .save()

}
