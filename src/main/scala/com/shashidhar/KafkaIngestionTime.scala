package com.shashidhar

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaIngestionTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "wordcount")
      .option("startingOffsets", "earliest")
      .load()

    val data = df.selectExpr("CAST(value AS STRING)", "from_unixtime(CAST(timestamp AS LONG),'YYYY-MM-dd HH:mm:ss')")
      .as[(String, Timestamp)]

    val wordsDs = data.flatMap(line => line._1.split(" ").map(word => {
      (word, line._2)
    })).toDF("word", "timestamp")

    val windowedCount = wordsDs
      .groupBy(
        window($"timestamp", "30 seconds")
      )
      .count()
      .orderBy("window")

    val query = windowedCount.writeStream
      .format("console")
      .option("truncate","false")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}