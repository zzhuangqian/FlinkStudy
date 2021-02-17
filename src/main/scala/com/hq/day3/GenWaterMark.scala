package com.hq.day3

import org.apache.flink.streaming.api.scala._
object GenWaterMark {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)


  env.execute()

}
