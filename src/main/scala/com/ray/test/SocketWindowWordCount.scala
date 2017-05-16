package com.ray.test

import java.lang.Iterable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.evictors.{CountEvictor, Evictor}
import org.apache.flink.streaming.api.windowing.evictors.Evictor.EvictorContext
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

/**
  * Created by Administrator on 2017-5-11.
  */
object SocketWindowWordCount {

  def main(args: Array[String]) : Unit = {

    // the port to connect to
//    val port: Int = try {
//      ParameterTool.fromArgs(args).getInt("port")
//    } catch {
//      case e: Exception => {
//        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
//        return
//      }
//    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5)).evictor(new Evictor[WordWithCount,Window] {
      override def evictBefore(elements: Iterable[TimestampedValue[WordWithCount]], i: Int, w: Window, evictorContext: EvictorContext): Unit = {
        var cout = 0;
        var iterable = elements.iterator();
        while (iterable.hasNext){
          iterable.next
          cout = cout+1;
          if (cout-i!=0){
            iterable.remove()
          }
        }
      }

      override def evictAfter(iterable: Iterable[TimestampedValue[WordWithCount]], i: Int, w: Window, evictorContext: EvictorContext): Unit = {}
    })
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)
}
