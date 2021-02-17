package com.hq.day5
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

object WindowJoinExample {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val input1 = env
    .fromElements(
      ("a",1,1000L),
      ("a",2,2000L),
      ("b",2,3000L),
      ("b",2,4000L)
    )
    .assignAscendingTimestamps(_._3)

  val input2 = env.fromElements(
    ("a",1,1000L),
    ("a",2,2000L),
    ("b",2,3000L),
    ("b",2,4000L)
  )
    .assignAscendingTimestamps(_._3)

  input1
    .join(input2)
    .where(_._1)
    .equalTo(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .apply(new MyJoin)

  env.execute()

//  分流开窗口以后，属于同一个窗口的input1 中元素和input2中的元素做笛卡尔积
  class MyJoin extends JoinFunction[(String,Int,Long),(String,Int,Long),String]{
    override def join(in1: (String, Int, Long), in2: (String, Int, Long)): String = {
      in1+"======>"+in2
    }
  }

}
