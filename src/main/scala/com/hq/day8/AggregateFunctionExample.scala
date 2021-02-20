package com.hq.day8

import com.hq.day2.{SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object AggregateFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val avgTemp = new AvgTemp()
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env,settings)
    var avgTemp = new AvgTemp()

    val table = tEnv.fromDataStream(stream,'id,'timestamp as 'ts,'temperature as 'temp)

    table
      .groupBy('id)
//      .aggregate('avgTemp("temp") as 'avgTemp)



    env.execute()

  }

  class AvgTempAcc{
    var sum: Double = 0.0
    var count : Int =0
  }
  // 第一个是泛型是温度值的类型
  // 第二个泛型是累加器的类型

  class AvgTemp extends AggregateFunction[Double,AvgTempAcc]{
    override def getValue(acc: AvgTempAcc): Double = {
      acc.sum/acc.count
    }

    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    def accumulate(acc: AvgTempAcc,temp: Double):Unit={
      acc.sum += temp
      acc.count+= 1
    }


  }



}
