package com.ggc.structured_streaming.join.stream

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * 外连接必须使用 watermark
 */

//noinspection DuplicatedCode
object StreamLeftJoinWithWatermark extends App {

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
    .withWatermark("timestamp1", "2 minutes")


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
    .withWatermark("timestamp2", "1 minutes")

//  val joinDF = nameSexStream.join(
//    nameAgeStream,
//    expr(
//      """
//        |name1=name2 and
//        |ts2 >= ts1 and
//        |ts2 <= ts1 + interval 1 minutes
//                """.stripMargin),
//    joinType = "left_join")


//  joinDF.writeStream.outputMode(OutputMode.Append())
//    .format("console")
//    .option("truncate", value = false)
//    .trigger(Trigger.ProcessingTime(0))
//    .start()
//    .awaitTermination()

}
