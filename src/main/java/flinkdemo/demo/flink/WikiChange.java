package flinkdemo.demo.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @author congyang.guo
 */
public class WikiChange {
    public static void main(String[] args) throws Exception {
        // 创建上下文 stream
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置输入源
        DataStreamSource<WikipediaEditEvent> source = env.addSource(new WikipediaEditsSource());
        // 按修改事件的用户分组
        KeyedStream<WikipediaEditEvent, String> keyedEdits = source.keyBy((KeySelector<WikipediaEditEvent, String>) event -> event.getUser());

        DataStream<Tuple3<String, String, Long>> aggregate = keyedEdits.timeWindow(Time.seconds(10)).aggregate(new AggregateFunction<WikipediaEditEvent, Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> createAccumulator() {
                return new Tuple3<>("", "", 0L);
            }

            @Override
            public Tuple3<String, String, Long> add(WikipediaEditEvent value, Tuple3<String, String, Long> acc) {
                acc.f0 = value.getUser();
                acc.f1 = value.getTitle();
                acc.f2 += value.getByteDiff();
                return acc;
            }

            @Override
            public Tuple3<String, String, Long> getResult(Tuple3<String, String, Long> acc) {
                return acc;
            }

            @Override
            public Tuple3<String, String, Long> merge(Tuple3<String, String, Long>  a, Tuple3<String, String, Long>  b) {
                return null;
            }
        });

        aggregate.print();
        env.execute();
    }

}

