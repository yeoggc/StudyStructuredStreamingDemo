package com.ggc.structured_streaming.outputsink

import java.util.{Timer, TimerTask}

import com.ggc.structured_streaming.outputsink.ConsoleSinkDemo.getClass
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object MemorySinkDemo extends App {


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

  val words: DataFrame = lines.as[String]
    .flatMap(_.split("\\W+"))
    .groupBy("value")
    .count()

  val query: StreamingQuery = words.writeStream
    .outputMode("complete")
    .format("memory") // memory sink
    .queryName("word_count") // 内存临时表名
    .start

  // 测试使用定时器执行查询表
  val timer = new Timer(true)
  val task: TimerTask = new TimerTask {
    override def run(): Unit = spark.sql("select * from word_count").show
  }
  timer.scheduleAtFixedRate(task, 0, 2000)

  query.awaitTermination()

}
