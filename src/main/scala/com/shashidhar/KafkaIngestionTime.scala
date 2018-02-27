package com.shashidhar

import org.apache.spark.sql.SparkSession

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
      .option("startingOffsets", """{"wordcount":{"0":0}}""")
      .load()

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS LONG)")
      .as[(String, String, Long)]

    val query = data.writeStream.format("console").outputMode("append").start()

    query.awaitTermination()
  }
}