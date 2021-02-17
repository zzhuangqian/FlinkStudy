package com.hq.day5
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost",9999,'\n')
      .map(line=>{
        val arr = line.split(" ")
        (arr(0),arr(1).toLong *1000L)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new WindowCount)

    stream.print()
    env.execute()

  }

  class OneSecondIntervalTrigger extends Trigger[(String,Long),TimeWindow]{
//    每来一条数据都要调用 一次
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
// 默认值为false  当第一条事件来的时候，会在后面的代码中将first-seen置 为true
      val firstSeen = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen",Types.of[Boolean])
      )

      if(!firstSeen.value()){
//        如果当前水位线是1234 那么 t =2000
        val t = triggerContext.getCurrentWatermark +(1000- (triggerContext.getCurrentWatermark %1000))
        triggerContext.registerEventTimeTimer(t)
        triggerContext.registerEventTimeTimer(w.getEnd)
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      if(l == w.getEnd){
        TriggerResult.FIRE_AND_PURGE
      }else{
        val t = triggerContext.getCurrentWatermark + (1000 - triggerContext.getCurrentWatermark %1000)
        if(t< w.getEnd){
          triggerContext.registerEventTimeTimer(t)
        }
        TriggerResult.FIRE

      }
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
      val firstSeen = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen",Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class WindowCount extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口中有 "+ elements.size +"条数据" + context.window.getEnd)
    }
  }

}
