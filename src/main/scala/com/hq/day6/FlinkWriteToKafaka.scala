package com.hq.day6

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object FlinkWriteToKafaka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(
      "hello",
      "world"
    )

    stream
      .addSink(
        new FlinkKafkaProducer011[String](
          "localhost:9092",
          "test",
//          使用字符串格式写入kafaka
          new SimpleStringSchema()
        )
      )
  }

}
