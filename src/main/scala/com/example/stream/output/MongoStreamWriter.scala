

package com.example.stream.output

import com.example.stream.Config
import com.example.stream.types.db.{ArticleId, ArticleTokenizeDBType, WordCount}
import com.example.stream.utils.{BSonUtil, Converter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

private class MongoForeachWriter1 extends ForeachWriter[Row] {

  import com.mongodb.client.MongoCollection
  import com.mongodb.spark.MongoConnector
  import com.mongodb.spark.config.WriteConfig
  import org.bson.BsonDocument

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> Config.mongoUrl, "database" -> Config.mongoDatabase, "collection" -> Config.mongoCollecition))
  var mongoConnector: MongoConnector = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    mongoConnector = MongoConnector(writeConfig.asOptions)
    true
  }

  override def process(record: Row): Unit = {
    mongoConnector.withCollectionDo(writeConfig, {
      collection: MongoCollection[BsonDocument] =>
        val doc = BSonUtil.rowToDocument(record)
        collection.insertOne(doc)
    })
  }

  override def close(errorOrNull: Throwable): Unit = {
  }
}


private class MongoForeachWriter2 extends ForeachWriter[Row] {

  import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
  import org.mongodb.scala.bson.codecs.{DEFAULT_CODEC_REGISTRY, Macros}
  import org.mongodb.scala.model.InsertOneOptions
  import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}

  import scala.concurrent.Await
  import scala.concurrent.duration.Duration


  var mongoClient: MongoClient = _
  var database: MongoDatabase = _
  var collection: MongoCollection[ArticleTokenizeDBType] = _

  def log : Logger = LoggerFactory.getLogger( this.getClass )

  override def open(partitionId: Long, epochId: Long): Boolean = {
    /*
    val codecRegistry: CodecRegistry =
       fromRegistries(MongoClient.getDefaultCodecRegistry(), fromProviders(PojoCodecProvider.builder().automatic(true).build()))
    fromRegistries(fromCodecs())
    */

    val codecRegistry = fromRegistries(fromProviders(
      Macros.createCodecProvider[ArticleTokenizeDBType](),
      Macros.createCodecProvider[ArticleId](),
      Macros.createCodecProvider[WordCount]()
    ), DEFAULT_CODEC_REGISTRY)

    mongoClient = MongoClient(Config.mongoUrl)
    database = mongoClient.getDatabase(Config.mongoDatabase).withCodecRegistry(codecRegistry)
    collection = database.getCollection[ArticleTokenizeDBType](Config.mongoCollecition)

    true
  }


  override def process(value: Row): Unit = {
    val article = Converter.toArticleTokenizeDBType(value)

    log.debug(s" >> Mongodb write ${article}")

    val option = new InsertOneOptions().bypassDocumentValidation(true)
    val observable = collection.insertOne(article, option)
    Await.result(observable.toFuture(), Duration.Inf)
  }


  override def close(errorOrNull: Throwable): Unit = {
    log.debug(" >> Foreach writer clode")
  }

}


/*
import com.mongodb.spark.config._

val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))

MongoSpark.save(sparkDocuments, writeConfig)
 */
private class MongoForeachWriter3(conf: SparkConf) extends ForeachWriter[Row] {

  import com.mongodb.spark.MongoSpark
  import com.mongodb.spark.config.WriteConfig

  //  var spark: SparkSession = _
  var buffer: mutable.ArrayBuffer[ArticleTokenizeDBType] = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    buffer = new mutable.ArrayBuffer[ArticleTokenizeDBType]()
    true
  }

  override def process(value: Row): Unit = {
    val arti = Converter.toArticleTokenizeDBType(value)
    println("Mongo append : " + arti)
    buffer.append(arti)
  }

  //  def close2(errorOrNull: Throwable): Unit = {
  //    if (buffer.nonEmpty) {
  //      val rdd = spark.sparkContext.parallelize(buffer.toList)
  //      spark.createDataFrame(rdd)
  //        //SparkSession.builder().getOrCreate().createDataFrame(rdd)
  //        .write
  //        .option("uri", "mongodb://localhost:27017/crawl.feedly")
  //        .mode("overwrite")
  //        .format("com.mongodb.spark.sql")
  //        .save()
  //      }
  //    }

  override def close(errorOrNull: Throwable): Unit = {
    if (buffer.nonEmpty) {
      val spark = SparkSession.builder()
        .config(conf)
        .config("spark.mongodb.output.uri", Config.mongoUrl)
        .config("spark.mongodb.output.database", Config.mongoDatabase)
        .config("spark.mongodb.output.collection", Config.mongoCollecition)
        .getOrCreate()
      val rdd = spark.sparkContext.parallelize(buffer.toList)
      val df = spark.createDataFrame(rdd)

      if (!df.isEmpty) {
        val writeConfig = WriteConfig(Map("uri" -> Config.mongoUrl, "database" -> Config.mongoDatabase, "collection" -> Config.mongoCollecition))
        MongoSpark.save(df, writeConfig)
      }
    }
  }

}


object MongoStreamWriter {

  def createStreamWriter(df: DataFrame): StreamingQuery = {
    df.writeStream
      .trigger(Trigger.ProcessingTime(s"${Config.trigger} seconds"))
      .outputMode("append")
      .foreach(new MongoForeachWriter1())
      .start()
  }

}