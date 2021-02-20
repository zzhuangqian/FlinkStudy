package com.hq.proj

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object UV {

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
      .map(r =>("key",r.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new WinodwResult)

    env.execute()
  }


  // 如果访问量怎么办? 这里的方法会把所有的Pv 数据放在窗口里面，然后去重
  class WinodwResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      var s: Set[Long] = Set()
      for (e <- elements) {
        {
          s += e._2
        }
        out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "窗口的UV统计值"+ elements.size)
      }
    }
  }

}
