package com.ray.test

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * Created by Administrator on 2017-5-8.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181");
    //properties.setProperty("group.id", "test");
    var  env = StreamExecutionEnvironment.getExecutionEnvironment
    var stream = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties))
        //.keyBy(0).timeWindow(Time.days(1)).
      .filter(e=>true)
      .print
    env.execute()

  }
}
