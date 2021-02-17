package com.hq.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object RedirectLateEventCustom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val steam = env.socketTextStream("localhost", 9999,
      "]n")
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .process(new LateEventProc)


    steam.print()
    steam.getSideOutput(new OutputTag[(String, Long)]("late"))
      .print()

    env.execute()

  }

  class Count extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口中共有" + elements.size)
    }
  }

  class LateEventProc extends ProcessFunction[(String,Long),(String,Long)]{
    val late  = new OutputTag[(String, Long)]("late")
    override def processElement(i: (String, Long), context: ProcessFunction[(String, Long), (String, Long)]#Context, collector: Collector[(String, Long)]): Unit = {
      if(i._2 < context.timerService().currentWatermark()){
        context.output(late,"迟到事件来了")
      }else{
        collector.collect(i)
      }
    }
  }

}
