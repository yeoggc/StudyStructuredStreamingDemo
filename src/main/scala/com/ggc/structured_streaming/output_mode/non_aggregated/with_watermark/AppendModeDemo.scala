package com.ggc.structured_streaming.output_mode.non_aggregated.with_watermark

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

//noinspection DuplicatedCode
object AppendModeDemo extends App {

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
      (Timestamp.valueOf(splits(0)), splits(1), splits(2))
    })
    .toDF("timestamp", "name", "sex")
    /** 添加了水印 **/
    .withWatermark("timestamp", "2 minutes")
    .writeStream
    .format("console")
    /** Append 输出模式 **/
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime(2000))
    .start()
    .awaitTermination()

  /**
   * 模拟数据：
    2019-08-14 11:00:00,lisi,male
    2019-08-14 11:00:00,lisi,male
    2019-08-14 11:10:00,lisi,male
    2019-08-14 11:00:00,lisi,male
    2019-08-14 11:00:00,lisi,male
    2019-08-14 10:00:00,lisi,male


   */

}
