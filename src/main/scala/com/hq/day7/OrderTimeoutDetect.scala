package com.hq.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutDetect {

  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val steam = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 2000L),
        OrderEvent("order_3", "create", 2000L),
        OrderEvent("order_1", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)


    val pattern = Pattern
      .begin[OrderEvent]("create")
      .where(_.eventType.equals("create"))
      .next("pay")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(steam, pattern)

    val orderTimeoutTag = new OutputTag[String]("timeout")

    val timeoutFunc = (map: scala.collection.Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {

      val orderCreate = map("create").iterator.next()
      out.collect("超时订单" + orderCreate.orderId)
    }
    val selectFunc = (map: scala.collection.Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {
      val orderCreate = map("pay").iterator.next()
      out.collect("订单" + orderCreate.orderId)
    }

    val detectStream = patternStream
      .flatSelect(orderTimeoutTag)(timeoutFunc)(selectFunc)

    detectStream.print()
    detectStream.getSideOutput(orderTimeoutTag).print()

    env.execute()
  }

}
