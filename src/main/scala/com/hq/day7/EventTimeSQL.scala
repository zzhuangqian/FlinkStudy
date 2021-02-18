package com.hq.day7

import com.hq.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object EventTimeSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env,settings)

    val stream = env.addSource(new SensorSource)
    //     指定了timestamp.rowtime  指定了timestamp数据处理事件时间
    val table = tEnv.fromDataStream(stream,'id,'timestamp.rowtime as 'ts,'temperature)
    table
      .window(Tumble over 10.seconds on 'pt as 'w)
      .groupBy('id,'w)
      .select('id,'id.count)
      .toRetractStream[Row]
      .print()
    env.execute()

  }

}
