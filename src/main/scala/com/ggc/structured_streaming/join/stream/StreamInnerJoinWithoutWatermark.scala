package com.ggc.structured_streaming.join.stream

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
//noinspection DuplicatedCode
object StreamInnerJoinWithoutWatermark extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("StreamStream1")
    .getOrCreate()

  import spark.implicits._

  // 第 1 个 stream
  val nameSexStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 10000)
    .load()
    .as[String]
    .map(line => {
      val split = line.split(",")
      (split(0), split(1), Timestamp.valueOf(split(2)))
    })
    .toDF("name", "sex", "timestamp1")


  // 第 2 个 stream
  val nameAgeStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 20000)
    .load
    .as[String]
    .map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
    }).toDF("name", "age", "timestamp2")


  val joinDF = nameSexStream.join(nameAgeStream, "name")
  joinDF.writeStream.outputMode(OutputMode.Append())
    .format("console")
    .option("truncate", value = false)
    .trigger(Trigger.ProcessingTime(0))
    .start()
    .awaitTermination()

}
