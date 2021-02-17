package com.hq.day4

import com.hq.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
   val stream = env.addSource(new SensorSource)
    stream.keyBy(_.id)
      .process(new TemIncreaseAlertFunction)

      .print()

    env.execute()


  }


  class TemIncreaseAlertFunction extends KeyedProcessFunction[String,SensorReading,String]{

    // 初始化一个状态变量
    // 懒加载 惰性赋值
    // 当执行到process 算子时，才会初始化，所以才是懒加载
//    为什么不直接使用scala 变量？ 比如是 var lastTemp :Double
    // 通过配置，状态亦是可以通过检查点操作，保存在hdfs里面
//    当程序故障时，可以从最近一次检查点恢复
    // 所以要用一个 名字'last-temp` 和变量类型（需要明确告诉flink状态变量的类型）
    // 状态亦是只会被初始人一次，运行程序时，如果没有这个状态变量，就初始化一次
    // 如果有这个状态变量，直接读取
    // 所以是单例模式

    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp",Types.of[Double])
    )
    lazy val timerTs = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts",Types.of(Long))
    )
    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
      // 获取最近一次的温度，需要使用 `.value()`方法
      // 如果来的是第一条温度，那么prevTemp 是0.0
      val prevTemp = lastTemp.value()
//      将来的温度值 更新到lastTemp 状态亦是，使用update方法
      lastTemp.update(i.temperature)
      val curTimerTs = timerTs.value()

      if(prevTemp == 0.0 || i.temperature< prevTemp){
        context.timerService().deleteEventTimeTimer(curTimerTs)
        timerTs.clear()
      }else if(i.temperature > prevTemp && curTimerTs == 0L){
        val ts = context.timerService().currentProcessingTime()+1000L
        context.timerService().registerProcessingTimeTimer(ts)
        timerTs.update(ts)
      }


    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("传感器ID为：" + ctx.getCurrentKey +" 的传感器温度连续1s 报警")
        timerTs.clear()

    }
  }

}
