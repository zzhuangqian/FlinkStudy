package com.hq.day8

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "hello#world",
        "atguigu#bigdata"
      )

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env,settings)

    val table = tEnv.fromDataStream(stream,'s)
    val split = new Split("#")

    table
      //    为了将hello#world 和 hello 5'join到一行
      .joinLateral(split('s) as ('word,'length))
      .select('s,'word,'length)
      .toAppendStream[Row]
      .print()

    tEnv.registerFunction("split",split)



    tEnv
      //    T 的意思是元组，flink 里面的固定语法
      .sqlQuery("SELECT s,word,length FROM t, LATERAL TABLE(split(s)) as T(word,length)")
      .toAppendStream[Row]
      .print()

    env.execute()

  }
  class Split(sep: String) extends TableFunction[(String,Int)]{
    def eval(s:String):Unit={
      s.split(sep).foreach(x=> collect((x,x.length)))
    }
  }


}

