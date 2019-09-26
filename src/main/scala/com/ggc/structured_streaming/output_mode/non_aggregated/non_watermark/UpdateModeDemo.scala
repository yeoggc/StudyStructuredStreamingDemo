package com.ggc.structured_streaming.output_mode.non_aggregated.non_watermark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

//noinspection DuplicatedCode
object UpdateModeDemo extends App {

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
    .writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .start()
    .awaitTermination()

  /**
   模拟数据：
    lisi,male
    zhangsan,male
    lisi,male

   */

}
