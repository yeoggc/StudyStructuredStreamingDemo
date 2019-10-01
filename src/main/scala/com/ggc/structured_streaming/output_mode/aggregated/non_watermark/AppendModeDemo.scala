package com.ggc.structured_streaming.output_mode.aggregated.non_watermark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
 * 运行报错：Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
 *
 * 意思是：Append输出模式在没有水印的流的聚合中不被支持
 *
 */

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
      (splits(0), splits(1))
    })
    .toDF("name", "sex")
    .groupBy("name")
    .count()
    .writeStream
    .format("console")
    .outputMode(OutputMode.Append())
    .start()
    .awaitTermination()

  /**
  模拟数据：
   */

}
