package com.example.stream

import com.example.stream.types.struct.Schema
import com.example.stream.utils.Converter
import com.mongodb.spark.MongoSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TokenizeDStream extends App {


  val conf = new SparkConf()
    .setAppName("TokenizeDStream")
//    .setMaster("local")
    .set("spark.mongodb.output.uri", Config.mongoUrl)
    .set("spark.mongodb.output.database", Config.mongoDatabase)
    .set("spark.mongodb.output.collection", Config.mongoCollecition)

  val ssc = new StreamingContext(conf, Seconds(Config.trigger))

  val sc = ssc.sparkContext

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> Config.bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> Config.startingOffsets,
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array(Config.subscribe)

  val stream = KafkaUtils
    .createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )




  val messages = stream.map(_.value())

  messages.foreachRDD(rdd => {
    val articles = Converter.rddToList(rdd)

    if (articles.size > 0) {

      val topwordSchem = new StructType()
        .add("word", StringType)
        .add("count", IntegerType)

      val artiSchema = Schema.jsonSchem
        .add("tokenize", ArrayType(StringType))
        .add("wordCount", ArrayType(topwordSchem))

      val spark = SparkSession.builder()
        .config(sc.getConf)
        .getOrCreate()

      import spark.implicits._

      // val articleDF = articles.toDF("articleId", "subject", "content", "cate", "wirte", "tokenize", "topwords")
      val artiRDD = sc.parallelize(articles)
      val articleDF = spark.createDataFrame(artiRDD, artiSchema)

      //      articleDF.printSchema()
      //      articleDF.show()


      val countVectorizer = new CountVectorizer()
        .setInputCol("tokenize")
        .setOutputCol("termFreqs")
        .setVocabSize(2000)

      val idf = new IDF()
        .setInputCol(countVectorizer.getOutputCol)
        .setOutputCol("tfidfVec")

      //      val hashingTF = new HashingTF()
      //        .setInputCol("tokenize")
      //        .setOutputCol("features")
      //        .setNumFeatures(100)
      //
      //      val stringIdx = new StringIndexer()
      //        .setInputCol("cate")
      //        .setOutputCol("label")

      // input column { features, label }
      //      val lr = new LogisticRegression()

      val pipe = new Pipeline()

      val resDF =
        pipe.setStages(Array(countVectorizer, idf))
          .fit(articleDF)
          .transform(articleDF)

      resDF.printSchema()
      resDF.show(false)

      val mogoDF = resDF.map(e => {
        Converter.toArticleTfIdfDbType(e)
      })

      MongoSpark.save(mogoDF)

    }

  })


  ssc.start()
  ssc.awaitTermination()

}
