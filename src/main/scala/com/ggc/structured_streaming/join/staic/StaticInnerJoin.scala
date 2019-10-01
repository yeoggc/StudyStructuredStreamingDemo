package com.ggc.structured_streaming.join.staic

import org.apache.spark.sql.{DataFrame, SparkSession}
//noinspection DuplicatedCode
object StaticInnerJoin {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SteamingStatic")
            .getOrCreate()
        import spark.implicits._
        
        // 得到静态的df
        val arr = Array(("lisi", 20), ("zs", 10), ("ww", 15))
        val staticDF = spark.sparkContext.parallelize(arr).toDF("name", "age")
        // 动态df
        val steamingDF = spark.readStream
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
        
        // 内连接
        val joinedDF: DataFrame = steamingDF.join(staticDF, Seq("name"))
        
        joinedDF.writeStream
            .format("console")
            .outputMode("update")
            .start()
            .awaitTermination()
    }

    /**
     模拟数据：
        lisi,male
        zhiling,female
        zs,male
     */

}
