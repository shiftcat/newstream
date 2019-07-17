package com.example.stream.output

import com.example.stream.Config
import com.example.stream.types.db.WordCount
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}
import redis.clients.jedis.Jedis


private class RedisForeachWriter(host: String, port: Int) extends ForeachWriter[Row] {

  var jedis: Jedis = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    jedis = new Jedis(host, port)
    true
  }

  override def process(record: Row): Unit = {
    val artiStruct = record.getStruct(0)
    val time = record.getString(6).split(":")(0)
    // ns:2019071113:PRESS cnt word
    val key = s"ns:${record.getString(5)}:${time}:${artiStruct.getString(1)}"

    record.getSeq[Row]( 8 ).map {
      case Row(str: String, cnt: Int) => WordCount(str, cnt)
    }.foreach(wc => {
      jedis.zincrby(s"${key}", wc.count, s"${wc.word}")
      jedis.expire(key, 60 * 60 * 3)
    })
  }

  override def close(errorOrNull: Throwable): Unit = {
    if(jedis != null) {
      jedis.close()
    }
  }
}


object RedisForeachWriter {

  def createStreamWriter(df: DataFrame) = {
    df.writeStream
      .trigger(Trigger.ProcessingTime(s"${Config.trigger} seconds"))
      .outputMode("append")
      .foreach(new RedisForeachWriter("localhost", 6379))
      .start()
  }

}
