package com.shashidhar

import org.apache.spark.sql.SparkSession

object KafkaSink {
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

    val data = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]

    val query = data.writeStream.format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic","wcOutput")
      .option("checkpointLocation", "src/main/kafkasink/chkpoint")
      .start()

    query.awaitTermination()
  }

}
