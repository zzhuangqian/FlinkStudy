package com.hq.day6

import org.apache.flink.api.common.time.Time
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
object FlinkCep {


  case class LoginEvent(userId: String,eventType: String,ipAddr: String,eventTime: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val stream = env
      .fromElements(
        LoginEvent("user_1","fail","0.0.0.1",1000L),
        LoginEvent("user_1","fail","0.0.0.2",2000L),
        LoginEvent("user_1","fail","0.0.0.3",3000L),
        LoginEvent("user_1","fail","0.0.0.4",4000L),
        LoginEvent("user_1","fail","0.0.0.5",5000L),
        LoginEvent("user_5","fail","0.0.0.1",1000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    val pattern = Pattern
      .begin[LoginEvent]("first")
      .where(_.eventType.equals("fail"))
      .next("second")
      .where(_.eventType.equals("fail"))
      .next("third")
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10))


    val patternStrem = CEP.pattern(stream,pattern)

    patternStrem
      .select((pattern:scala.collection.Map[String,Iterable[LoginEvent]])=>{
        val first = pattern("first").iterator.next()
        val second = pattern("second").iterator.next()
        val third = pattern("third").iterator.next()
        "用户"+first.userId + "分别在Ip"+first.ipAddr+second.ipAddr
      })

    env.execute()
  }

}
