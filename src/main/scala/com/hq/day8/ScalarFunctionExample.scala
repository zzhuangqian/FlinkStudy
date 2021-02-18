package com.hq.day8

import com.hq.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val steam = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env,settings)

    val hashCode = new HashCode(10)
    tEnv.registerFunction("hashCode",hashCode)

    val table = tEnv.fromDataStream(steam)
    table.select('id,hashCode('id))
      .toAppendStream[Row]
      .print()

    env.execute()
  }

  class HashCode(factor: Int) extends ScalarFunction{
    def eval(s:String) :Int={
      s.hashCode *factor
    }

  }
}
