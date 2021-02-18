package com.hq.day7

import com.hq.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SensorReadingBySql {

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

    tEnv.createTemporaryView("sensor",stream,'id,'timestamp as 'ts,'temperature)

    tEnv.sqlQuery("SELECT id ,count(id) FROM sensor GROUP BY id")
      .toRetractStream[Row]
      .print()


    env.execute()

  }

}
