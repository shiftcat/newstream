package com.example.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object BypassDFStream extends App {

  val spark = SparkSession.builder()
    .appName("BypassDFStream")
    .master("local")
    .getOrCreate()

  val inputDf = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", Config.bootstrapServers)
    .option("subscribe", Config.subscribe)
    .option("startingOffsets", Config.startingOffsets)
    .load()

  val consoleOutput = inputDf.writeStream
    .trigger(Trigger.ProcessingTime(s"${Config.trigger} seconds"))
    .outputMode("append")
    .format("console")
    .start()

  consoleOutput.awaitTermination()

}
