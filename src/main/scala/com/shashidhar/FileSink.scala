package com.shashidhar

import org.apache.spark.sql.SparkSession

object FileSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("kafkasink")
      .getOrCreate()
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "wordcount")
      .option("startingOffsets", "earliest")
      .load()

    val data = df.selectExpr("CAST(value AS STRING)").as[String]

    val query = data.writeStream.format("text")
      .outputMode("append")
      .option("checkpointLocation", "src/main/filesink/chkpoint")
      .option("path", "src/main/filesink/output")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .start()
    query.awaitTermination()
  }

}
