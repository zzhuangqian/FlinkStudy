package com.hq.day3

import com.hq.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AvgTempByAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTemAgg)
      .print()
    env.execute()


  }
  // 第一个泛型： 流中元素的类型
  // 第二个泛型： 累加器的类型
  // 第三个泛型： 增量聚合函数的输出类型
  class AvgTemAgg extends AggregateFunction[SensorReading,(String,Long,Double),(String,Double)]{
    override def createAccumulator(): (String, Long, Double) = ("",0L,0.0)

//    聚合的逻辑是什么
    override def add(in: SensorReading, acc: (String, Long, Double)): (String, Long, Double) = {
      (in.id,acc._2+1,acc._3+in.temperature)
    }

//    窗口闭合时，输出的结果是什么
    override def getResult(acc: (String, Long, Double)): (String, Double) = {
      (acc._1,acc._3/acc._2)

    }

    // 两个累加器合并的逻辑是什么
    override def merge(acc: (String, Long, Double), acc1: (String, Long, Double)): (String, Long, Double) = {
      (acc._1,acc._2+acc1._2,acc._3+acc1._3)
    }
  }

}
