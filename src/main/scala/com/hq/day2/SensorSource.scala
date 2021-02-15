package com.hq.day2

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

// 泛型是sensorReading 表明产生的流中的事件类型是SensorReading
class SensorSource extends RichParallelSourceFunction[SensorReading] {
  // 表示数据源是否正常运行
  var runing: Boolean = true


  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random
    var curFtemp = (1 to 10).map(
      i => ("sensor_" + i, (rand.nextGaussian() * 20))
    )
    while (runing) {
      curFtemp = curFtemp.map(
        t => (t._1, t._2 + (rand.nextGaussian() * 0.5))
      )

      val curTime = Calendar.getInstance.getTimeInMillis

      curFtemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      Thread.sleep(100)

    }

  }

//  定义当关闭传感器时，取消数据源
  override def cancel(): Unit = runing = false


}
