package com.hq.day3

import com.hq.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    val keyedStream = stream.keyBy(_.id)
    val windowedStream : WindowedStream[SensorReading,String,TimeWindow] = keyedStream.timeWindow(Time.seconds(5))

    env.execute()

  }

}
