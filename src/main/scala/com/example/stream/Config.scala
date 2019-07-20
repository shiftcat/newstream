package com.example.stream

object Config {

  val bootstrapServers = "zka01:9092,zka02:9092,zka03:9092"

  // val bootstrapServers = "localhost:9092"

  val subscribe = "newscrawl"

  val startingOffsets = "latest"

  val trigger = 5



  var mongoUrl = "mongodb://192.168.100.31:27027"

  val mongoDatabase = "crawl"

  val mongoCollecition = "tokenize"


  val redisHost = "192.168.100.31"

  val redisPort = 6379



}
