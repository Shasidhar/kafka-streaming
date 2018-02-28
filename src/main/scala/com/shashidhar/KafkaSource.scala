package com.shashidhar

import org.apache.spark.sql.SparkSession

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("kafkasource")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    //Subscribe to one topic
//    val oneTopifDF = spark
//      .read
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "wordcount")
//      .option("startingOffsets", "earliest")
//      .load()
//    val result = oneTopifDF.selectExpr("CAST(value AS STRING)","topic","partition","offset") //Same schema as what we get in readStream
//      .as[(String,String,String,String)]
//      result.show()

    //Subscribe to multiple topics
    val multipleTopicdf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "wordcount,wcOutput")
      .option("startingOffsets", """{"wordcount":{"0":20},"wcOutput":{"0":0}}""")
      .option("endingOffsets", """{"wordcount":{"0":25},"wcOutput":{"0":5}}""")
      .load()
    val result1 = multipleTopicdf.selectExpr("CAST(value AS STRING)","topic","partition","offset") //Same schema as what we get in readStream
      .as[(String,String,String,String)]
    result1.show()

  }
}
