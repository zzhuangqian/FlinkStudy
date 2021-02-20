package com.hq.day4

import com.hq.day2.SensorSource
import org.apache.flink.api.common.state._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    第一条流是一条无限流
    val reading = env.addSource(new SensorSource)
// 第二条流是一条有限流，只有一个元素
//    用来做开关，对`sensor_2`的数据旅行10s
    val switches= env.fromElements(
      ("sensor_2",10*1000L)
    )

//    val result = reading.connect(switches)
//      .keyBy(_.id,_._1)
//      .process(new ReadingFilter)

      env.execute()
//      .process(new ReadingFilter)
  }

//  class ReadingFilter extends CoProcessFunction[SensorSource,(String,Long)]{
//    lazy val forwardingEnable = getRuntimeContext.getState(
//      new ValueStateDescriptor[Boolean]("switch",Types.of[Boolean])
//    )
//
//    override def processElement1(in1: SensorSource, context: CoProcessFunction[SensorSource, (String, Long), OUT]#Context, collector: Collector[OUT]): Unit = {
//      if(forwardingEnable.value()){
//        collector.collect(in1)
//      }
//    }
//
//    override def processElement2(in2: (String, Long), context: CoProcessFunction[SensorSource, (String, Long), OUT]#Context, collector: Collector[OUT]): Unit = {
//      forwardingEnable.update(true)
//      val ts = context.timerService().currentProcessingTime() + in2._2
//      context.timerService().registerProcessingTimeTimer(ts)
//
//    }
//
//    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorSource, (String, Long), OUT]#OnTimerContext, out: Collector[OUT]): Unit = {
//      forwardingEnable.clear()
//    }
//  }

}
