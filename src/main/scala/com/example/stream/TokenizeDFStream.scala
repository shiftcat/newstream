package com.example.stream

import com.example.stream.output.{MongoStreamWriter, RedisForeachWriter}
import com.example.stream.types.struct.Schema
import com.example.stream.utils.Utils.wordCount
import com.github.uosdmlab.nkp.Tokenizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.streaming.Trigger

object TokenizeDFStream extends App
{


  val wordCountUDF = udf( wordCount(_:Seq[String]):List[(String, Int)] )


  val spark = SparkSession.builder()
    .appName("TokenizeDFStream")
    .master("local")
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017")
    .config("spark.mongodb.output.database", "crawl")
    .config("spark.mongodb.output.collection", "tokenize")
    .getOrCreate()

  val readDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", Config.bootstrapServers)
    .option("subscribe", Config.subscribe)
    //.option("startingOffsets", "earliest")
    .option("startingOffsets", Config.startingOffsets)
    .load()

  readDF.printSchema()

  val readValueDF = readDF.selectExpr("CAST(value AS STRING)")


  val articleDF =
    readValueDF.select(from_json(col("value").cast("string"), Schema.jsonSchem).as("data"))
      .select("data.*")
      .where(col("subject").isNotNull.and(col("subject") =!= "").and(col("content").isNotNull))

  val tokenizer = new Tokenizer()
    .setInputCol("content")
    .setOutputCol("tokenize")
    .setFilter("N", "SN", "SL", "SH")

  val tokenizeDF =
    tokenizer.transform(articleDF)
      .withColumn("wordCount", wordCountUDF(col("tokenize")))


//  val countVectorizer = new CountVectorizer()
//    .setInputCol("tokenize")
//    .setOutputCol("termFreqs")
//    .setVocabSize(2000)
//
//  val idf = new IDF()
//    .setInputCol(countVectorizer.getOutputCol)
//    .setOutputCol("tfidfVec")
//
//  val pipe = new Pipeline()
//
//  val resDF =
//    pipe.setStages(Array(tokenizer, countVectorizer, idf))
//      .fit(articleDF)
//      .transform(articleDF)

  val outputStream =
    tokenizeDF.writeStream
      .trigger(Trigger.ProcessingTime(s"${Config.trigger} seconds"))
      .format("console")
      .outputMode("append")
      .start()


  val mongoOutStream = MongoStreamWriter.createStreamWriter(tokenizeDF)
  val redisOutStream = RedisForeachWriter.createStreamWriter(tokenizeDF)

  outputStream.awaitTermination()
  mongoOutStream.awaitTermination()
  redisOutStream.awaitTermination()

}
