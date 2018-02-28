package com.shashidhar

import org.apache.spark.sql.SparkSession

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
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

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val results = data
                    .map(_._2)
                    .flatMap(value => value.split("\\s+"))
                    .groupByKey(_.toLowerCase)
                    .count()

    val query = results.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()

  }
}