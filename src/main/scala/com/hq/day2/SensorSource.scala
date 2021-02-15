package com.hq.day2

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading]{
  // 表示数据源是否正常运行
  val runing: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random
    var curFtemp = (1 to 10).map(
      i=>('sensor_'+i,(rand.nextGaussian() *20))
    )
    while(runing){
      curFtemp = curFtemp.map(
        t=> (t._1,t._2+(rand.nextGaussian() *0.5))
      )

      val curTime = Calendar.getInstance.getTimeInMillis

      curFtemp.foreach(t=> ctx.collection())

    }

  }


}
