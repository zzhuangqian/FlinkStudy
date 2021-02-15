package com.hq.day2

import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val steam = env.addSource(new SensorSource)
    steam.filter(r => r.id.equals("sensor_1")).print()
    env.execute()
  }
}