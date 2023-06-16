package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,String>> inputStream = env.fromElements(
        Tuple2.of("en","tea"),Tuple2.of("fr","good"), Tuple2.of("fr","game"), Tuple2.of("en","cake")
        );
        DataStream<Tuple2<String,String>> resultStream = inputStream
                .keyBy(0)
                .reduce((x,y)-> Tuple2.of(x.f0,x.f1+" "+y.f1));
        resultStream.print();
        env.execute();
    }
}
