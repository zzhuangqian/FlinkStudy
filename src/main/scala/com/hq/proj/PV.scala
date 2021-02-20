package com.hq.proj


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object PV {

  case class UserBehaviour(userId: Long,
                           itemId: Long,
                           categoryId: Int,
                           behaviour: String,
                           timestamp: Long)

  case class ItemViewCount(itemId: Long, // 商品id
                           windowEnd: Long, // 窗口结束时间
                           count: Long) // itemid 在windowEnd 的数量
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("")
      .map(line => {
        val arr = line.split(",")
        UserBehaviour(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000L)

      })
      .filter(_.behaviour.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳
      .timeWindowAll(Time.hours(1))
      .aggregate(new CountAgg,new WindowResult)

    env.execute()
  }

  class CountAgg extends  AggregateFunction[UserBehaviour,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehaviour, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
  }

  // processWindowFunction 用于keyBy.timewindow以后的流
  class WindowResult extends ProcessAllWindowFunction[Long,String,TimeWindow]{
    override def process(context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("结束时间为"+ new Timestamp(context.window.getEnd)+"的窗口PV统计值是"+ elements.size)
    }
  }

}
