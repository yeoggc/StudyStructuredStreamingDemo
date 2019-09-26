package com.ggc.structured_streaming.outputsink

import com.ggc.structured_streaming.outputsink.KafkaBatchSinkDemo.getClass
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaStreamingSinkDemo extends App {

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

  val words = lines.as[String]
    .flatMap(_.split("\\W+"))
    .groupBy("value")
    .count()
    .map(row => row.getString(0) + "," + row.getLong(1))
    .toDF("value") // 写入数据时候, 必须有一列 "value"

  words.writeStream
    .outputMode("update")
    .format("kafka")
    .trigger(Trigger.ProcessingTime(0))
    .option("kafka.bootstrap.servers", "dw1:9092,dw2:9092,dw3:9092") // kafka 配置
    .option("topic", "SparkStructuredStreamingTopic") // kafka 主题
    .option("checkpointLocation", "./checkPoint/KafkaSinkDemo") // 必须指定 checkpoint 目录
    .start
    .awaitTermination()


}
