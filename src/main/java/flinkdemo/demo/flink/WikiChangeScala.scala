package flinkdemo.demo.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.datastream.{DataStreamSource, KeyedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}

/**
  * @author congyang.guo
  */
object WikiChangeScala {

 def main(args: Array[String]): Unit = {
   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
   val source: DataStreamSource[WikipediaEditEvent] = env.addSource(new WikipediaEditsSource)
   // 按修改事件的用户分组
   val keyedEdits:KeyedStream[WikipediaEditEvent, String] =  source.keyBy(new KeySelector[WikipediaEditEvent,String] {
     override def getKey(value: WikipediaEditEvent): String = value.getUser
   })

   val aggregate = keyedEdits.timeWindow(Time.seconds(10)).aggregate(new AggregateFunction[WikipediaEditEvent, Tuple3[String, String, Long], Tuple3[String, String, Long]]() {
     def createAccumulator = new Tuple3[String, String, Long]("", "", 0L)
     def add(value: WikipediaEditEvent, acc: Tuple3[String, String, Long]): Tuple3[String, String, Long] = {
       acc.f0 = value.getUser
       acc.f1 = value.getTitle
       acc.f2 += value.getByteDiff
       acc
     }
     def getResult(acc: Tuple3[String, String, Long]): Tuple3[String, String, Long] = acc
     override
     def merge(a: Tuple3[String, String, Long], b: Tuple3[String, String, Long]): Tuple3[String, String, Long] = null
   })
   aggregate.print
   env.execute
 }

}
