package com.shashidhar

import org.apache.spark.sql.SparkSession

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("kafkasource")
      .getOrCreate()

    //Subscribe to one topic
    val oneTopifDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
    oneTopifDF.selectExpr("CAST(value AS STRING)") //Same schema as what we get in readStream
      .as[(String)]
    oneTopifDF.show()

    //Subscribe to multiple topics
    val multipleTopicdf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
//      .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
//      .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
      .load()
    multipleTopicdf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    multipleTopicdf.show()

  }
}
