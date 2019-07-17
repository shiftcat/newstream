package com.example.stream

object Config {

  // val bootstrapServers = "zka01:9092,zka02:9092,zka03:9092"

  val bootstrapServers = "localhost:9092"

  val subscribe = "newscrawl"

  val startingOffsets = "latest"

  val trigger = 5

}
