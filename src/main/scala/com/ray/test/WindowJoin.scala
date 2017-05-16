package com.ray.test

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
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
object WindowJoin {
  def main(args: Array[String]): Unit = {
    //    val properties = new Properties();
    //    properties.setProperty("bootstrap.servers", "localhost:9092");
    //    // only required for Kafka 0.8
    //    //properties.setProperty("zookeeper.connect", "localhost:2181");
    //    //properties.setProperty("group.id", "test");
    //    var  env = StreamExecutionEnvironment.getExecutionEnvironment
    //    var stream = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties))
    //      .filter(e=>true)
    //      .print
    //    env.execute()
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    var s1 = env.socketTextStream("192.168.133.78",9999).map(e=>(e,new Date().getTime,1)).assignAscendingTimestamps(_._2)//
    var s2 = env.socketTextStream("192.168.133.78",9998).map(e=>(2,new Date().getTime,e)).assignAscendingTimestamps(_._2)//



    s1.join(s2).where(e=>{
      e._1
    }).equalTo(e=>{
      e._3
    }).window(TumblingEventTimeWindows.of(Time.seconds(10))).trigger(CountTrigger.of(1))
      .apply((e1,e2)=>{
        e1+":"+e2
      }).print().setParallelism(1)

    env.execute()

  }
}
