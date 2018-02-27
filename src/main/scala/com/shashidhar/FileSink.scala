package com.shashidhar

import org.apache.spark.sql.SparkSession

object FileSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("kafkasink")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "wordcount")
      .load()

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val results = data
      .map(_._2)
      .flatMap(value => value.split("\\s+"))
      .groupByKey(_.toLowerCase)
      .count()

    val query = results.writeStream.format("parquet")
      .outputMode("complete")
      .option("checkpointLocation", "/Users/mcbk01/interests/structuredstreaming/kafka-streaming/src/main/filesink/chkpoint")
      .option("path", "/Users/mcbk01/interests/structuredstreaming/kafka-streaming/src/main/filesink/output")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .start()
    query.awaitTermination()
  }

}
