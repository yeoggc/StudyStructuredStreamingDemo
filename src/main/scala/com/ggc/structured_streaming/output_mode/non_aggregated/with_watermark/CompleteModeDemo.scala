package com.ggc.structured_streaming.output_mode.non_aggregated.with_watermark

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
 * 此程序运行会报错：org.apache.spark.sql.AnalysisException: Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;
 *
 * 意思是：Complete输出模式不能用在基于流的DataFrames/Datasets数据结构上没有进行流聚合操作
 *
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
      (Timestamp.valueOf(splits(0)), splits(1), splits(2))
    })
    .toDF("timestamp", "name", "sex")

    /** 添加了水印 **/
    .withWatermark("timestamp", "2 minutes")
    .writeStream
    .format("console")

    /** Complete输出模式 **/
    .outputMode(OutputMode.Complete())
    .start()
    .awaitTermination()

}
