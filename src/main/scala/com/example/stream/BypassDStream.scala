package com.example.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object BypassDStream extends App
{

  def log : Logger = LoggerFactory.getLogger( this.getClass )


  val sc = new SparkConf()
    .setAppName("BypassDStream")
    .setMaster("local")

  val ssc = new StreamingContext(sc, Seconds(Config.trigger))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> Config.bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> Config.startingOffsets,
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array(Config.subscribe)
  val stream = KafkaUtils.createDirectStream[String, String](
                  ssc,
                  PreferConsistent,
                  Subscribe[String, String](topics, kafkaParams)
                )

//  stream.foreachRDD { rdd =>
//    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//    rdd.foreachPartition { iter =>
//      val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${o.count()}")
//    }
//  }

  stream.foreachRDD(rdd => {
    rdd.foreachPartition(iter => {
      iter.foreach(cr => {
        log.debug(cr.value())
      })
    })
  })

  ssc.start()
  ssc.awaitTermination()
}
