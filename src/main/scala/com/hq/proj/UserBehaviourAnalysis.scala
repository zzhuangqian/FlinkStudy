package com.hq.proj

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer
//import org.apache.flink.table.functions.AggregateFunction

object UserBehaviourAnalysis {

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
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //       增量聚合和全窗口聚合结合使用
      //      聚合结果ItemviewCount 是每个窗口中每个商品被浏览的次数
      .aggregate(new CountAgg, new windowResult)
//       对DataStream[itemViewCount] 使用窗口结束时间进行分流
//      每一条支流里面的元素都属于同一个窗口，元素还是ItemViewCount
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    stream.print()

    env.execute()


  }


  class CountAgg extends AggregateFunction[UserBehaviour, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehaviour, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class windowResult extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.head))
    }
  }

  case class TopN(i: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{

    lazy val itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-state",Types.of[ItemViewCount])
    )

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd+100L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()

      import scala.collection.JavaConversions._
      for(item <- itemState.get){
        allItems+=item
      }
      itemState.clear()
      val sortItems = allItems.sortBy(_.count).take(i)

      val ruslut = new StringBuilder
      ruslut.append("++++++++++++++++\n")
        .append("窗口结束时间:")
        .append(new Timestamp(timestamp-100L))
      for(i <- sortItems.indices){
        val currItem = sortItems(i)
        ruslut.append("第")
          .append(i+1)
          .append("名的商品ID是：")
          .append(currItem.itemId)
          .append(",浏览量是：")
          .append(currItem.count)
          .append("\n")
      }
      ruslut.append("+++++++++++++++++++++")
      out.collect(ruslut.toString())
    }
  }

}
