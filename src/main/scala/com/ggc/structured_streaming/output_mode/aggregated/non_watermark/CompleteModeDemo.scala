package com.ggc.structured_streaming.output_mode.aggregated.non_watermark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
/**
 * 在输出模式是complete的时候(必须有聚合), 要求每次输出所有的聚合结果.
 * 我们使用 watermark 的目的是丢弃一些过时聚合数据,
 * 所以complete模式使用wartermark无效也无意义.
 */

//noinspection DuplicatedCode
object CompleteModeDemo extends App {
  val sparkSession = SparkSession.builder().master("local[*]").appName(getClass.getSimpleName).getOrCreate()

  import sparkSession.implicits._

  sparkSession.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 10000)
    .load
    .as[String]
    .map(line => {
      val splits = line.split(",")
      (splits(0), splits(1))
    })
    .toDF("name", "sex")
    .groupBy("name")
    .count()
    .writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    .start()
    .awaitTermination()

}
