package com.shashidhar

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType


object KafkaSinkUpdate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("kafkasink")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

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

  val result = windowedCount
    .selectExpr("concat(CAST(window.start AS String)," +
    "','" +
    ",CAST(window.end AS String)," +
    "','" +
    ",CAST(count AS STRING)) as value")

    val query = result
      .writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic","wcOutput")
      .option("checkpointLocation", "src/main/kafkaUpdateSink/chkpoint")
      .start()

    query.awaitTermination()
  }
}