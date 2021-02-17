package com.hq.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val clickStream = env
      .fromElements(
        ("1","click",3600*1000L)
      )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)
    val browseSteam = env
      .fromElements(
        ("1","browse",2000*1000L),
        ("1","brose",3100*1000L),
        ("1","brose",3200*1000L)
      )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    clickStream
      .intervalJoin(browseSteam)
      .between(Time.seconds(-600),Time.seconds(0))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[(String,String,Long),(String,String,Long),String]{
    override def processElement(in1: (String, String, Long), in2: (String, String, Long), context: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context, collector: Collector[String]): Unit ={
      collector.collect(in1+"========"+ in2)
    }
  }

}
