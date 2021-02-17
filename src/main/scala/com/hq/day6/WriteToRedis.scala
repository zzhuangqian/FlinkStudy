package com.hq.day6

import com.hq.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
object WriteToRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val steam = env.addSource(new SensorSource)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build()

    steam.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))
  }

  class MyRedisMapper extends RedisMapper[SensorReading]{
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"sensor")
    }

    override def getKeyFromData(t: SensorReading): String = {
      t.id
    }

    override def getValueFromData(t: SensorReading): String = {
      t.temperature.toString
    }
  }
}
