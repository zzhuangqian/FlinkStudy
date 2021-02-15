package com.hq.day2

import org.apache.flink.streaming.api.scala._

object KeyedSteamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val steam = env.addSource(new SensorSource)
    val keyed = steam.keyBy(_.id)
    keyed.min(2).print()
    //    keyed.print()

    keyed.reduce((r1, r2) => SensorReading(r1.id, 0L, r1.temperature.min(r2.temperature)))

    env.execute()
  }
}