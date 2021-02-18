package com.hq.day7

import com.hq.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableFromDataStreamExample {

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

    val table = tEnv.fromDataStream(stream,'id,'timestamp as 'ts,'temperature)
    table
      .select('id)
      .toAppendStream[Row]
      .print()

    tEnv.createTemporaryView("sensor",stream,'id,'timestamp as 'ts,'temperature)
    tEnv
      .sqlQuery("select * from sensor where id='sensor_1'")
      .toRetractStream[Row]
      .print()


    env.execute()

  }

}
