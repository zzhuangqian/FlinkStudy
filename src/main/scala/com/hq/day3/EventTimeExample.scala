package com.hq.day3

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

//import java.security.Timestamp
object EventTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    系统默认是200ms
    // 手动设置是1分钟
    env.getConfig.setAutoWatermarkInterval(60000)

    val stream = env.socketTextStream("localhost",9999,'\n')
      .map(line=>{
        val arr = line.split(" ")
//        时间单位必须是毫秒
        (arr(0),arr(1).toLong*1000L)
      })
//      分配时间和水位线一定要在keyBy之前进行
      .assignTimestampsAndWatermarks(
//        设置事件的最大延迟时间是5秒
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(1000)) {
//           告诉系统，时间是元组的第二个字段
          override def extractTimestamp(t: (String, Long)): Long = t._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult)

    stream.print()
    env.execute()

  }
  class WindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
          out.collect(new Timestamp(context.window.getStart) +"~")
    }
  }

}
