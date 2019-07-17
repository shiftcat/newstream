package com.example.stream

import com.github.uosdmlab.nkp.Tokenizer
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object Word2VectorStream extends App
{


  val spark = SparkSession.builder()
    .appName("Word2VectorStream")
    .master("local")
    .getOrCreate()

  val readDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", Config.bootstrapServers)
    .option("subscribe", Config.subscribe)
    .option("startingOffsets", Config.startingOffsets)
    .load()

  readDF.printSchema()

  val readValueDF = readDF.selectExpr("CAST(value AS STRING)")

  val schema = new StructType()
    .add("subject", StringType)
    .add("content", StringType)
    .add("writer", StringType)


  val articleDF =
    readValueDF.select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

  val tokenizer = new Tokenizer()
    .setInputCol("content")
    .setOutputCol("tokenize")
    .setFilter("N", "SN", "SL", "SH")

  val tokenizeDF = tokenizer.transform(articleDF)
  tokenizeDF.createOrReplaceTempView("tokenizeDF")

  // org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
  // https://stackoverflow.com/questions/46541998/queries-with-streaming-sources-must-be-executed-with-writestream-start

  val word2vec = new Word2Vec()
    .setInputCol("tokenize")
    .setOutputCol("vector")
    .setVectorSize(300)
    .setMaxIter(5)

  val model = word2vec.fit(tokenizeDF)
  val resDF = model.transform(tokenizeDF)

  //model.write.overwrite.save("nsmc/model/w2v")

  val outputStream =
    resDF.writeStream
      .trigger(Trigger.ProcessingTime(s"${Config.trigger} seconds"))
      .format("console")
      .outputMode("append")
      .start()

  outputStream.awaitTermination()

}
