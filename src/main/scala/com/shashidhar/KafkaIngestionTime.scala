package com.shashidhar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaIngestionTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "wordcount")
      .load()

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS LONG)")
      .as[(String, String, Long)]

    val wordsDs = data.flatMap(line => line._2.split(" ").map(word => {
      Thread.sleep(15000)
      (word, line._3)
    })).toDF("word", "timestamp")

    val windowedCount = wordsDs
      .groupBy(
        window($"timestamp", "15 seconds")
      )
      .count()
      .orderBy("window")

    val query = windowedCount.writeStream.format("console").outputMode("append").start()

    query.awaitTermination()
  }
}