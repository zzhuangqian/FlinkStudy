package com.hq.day4

import com.hq.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FresszingAlarm {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
      .process(new FresszingAlarmFunction)

    stream.getSideOutput(new OutputTag[String]("freezing-alarm")).print()
    env.execute()

  }
//  ProcessFunction 处理的是没有KeyBy的流

  class FresszingAlarmFunction extends ProcessFunction[SensorReading,SensorReading]{

    // 定义一个侧输出标签，实际上就是侧输出的流的名字
    // 侧输出流中元素的泛型是String
    lazy val freezingAlarmOut = new OutputTag[String]("freezing-alarm")

    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if(i.temperature < 32.0){
        context.output(freezingAlarmOut,s"${i.id}的传感器低温报警")
      }
      // 将所有元素常规输出
      collector.collect(i)
    }
  }

}
