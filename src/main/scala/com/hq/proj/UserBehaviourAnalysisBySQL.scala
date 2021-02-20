package com.hq.proj

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object UserBehaviourAnalysisBySQL {
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


    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env,settings)

    tEnv.createTemporaryView("t",stream,'itemId,'timestamp.rowtime as 'ts)

    tEnv.sqlQuery(
      """
        |SELECT *
        |FROM(
        |""".stripMargin
    )

    stream.print()

    env.execute()


  }

}
