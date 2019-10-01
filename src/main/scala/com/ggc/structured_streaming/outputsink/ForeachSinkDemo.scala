package com.ggc.structured_streaming.outputsink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

object ForeachSinkDemo extends App {

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

  val wordCount: DataFrame = lines.as[String]
    .flatMap(_.split("\\W+"))
    .groupBy("value")
    .count()

  val query: StreamingQuery = wordCount.writeStream
    .outputMode("update")
    .foreach(new ForeachWriter[Row] {
      var conn: Connection = _
      var ps: PreparedStatement = _
      var batchCount = 0

      // 一般用于 打开链接. 返回 false 表示跳过该分区的数据
      override def open(partitionId: Long, epochId: Long): Boolean = {
        println("open ..." + partitionId + "  " + epochId)
        Class.forName("com.mysql.jdbc.Driver")
        conn = DriverManager.getConnection("jdbc:mysql://dw1:3306/SparkStructuredStreamingDB", "root", "111111")
        // 插入数据, 当有重复的 key 的时候更新
        val sql = "insert into word_count values(?, ?) on duplicate key update word=?, count=?"
        ps = conn.prepareStatement(sql)

        conn != null && !conn.isClosed && ps != null
      }

      // 把数据写入到连接
      override def process(value: Row): Unit = {
        println("process ...." + value)
        val word: String = value.getString(0)
        val count: Long = value.getLong(1)
        ps.setString(1, word)
        ps.setLong(2, count)
        ps.setString(3, word)
        ps.setLong(4, count)
        ps.execute()
      }

      // 用户关闭连接
      override def close(errorOrNull: Throwable): Unit = {
        println("close...")
        if (ps != null && !ps.isClosed) {
          ps.close()
        }
        if (conn != null && !conn.isClosed) {
          conn.close()
        }
      }
    })
    .start()

  query.awaitTermination()

}
