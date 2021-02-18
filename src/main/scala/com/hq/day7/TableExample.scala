package com.hq.day7

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row


object TableExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
// 有关表环境的配置
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // blink planner 是流批统一的
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env,settings)

    tEnv.connect(new FileSystem().path("/User/ddd"))
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())

      )
      .createTemporaryTable("inputTable")

    val sensorTable = tEnv.from("inputTable")
    val resultTable = sensorTable.select("id,temperature")
      .filter("id='sensor_1'")

      tEnv.toAppendStream[Row](resultTable).print()


    tEnv.sqlQuery("SELECT id,temperature FROM inputTable where id = 'sensor_1'")
      .toString()

    env.execute()


  }

}
