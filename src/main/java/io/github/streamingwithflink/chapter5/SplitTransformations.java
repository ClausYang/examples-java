package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer,String>> inputStream = env.fromElements(
                Tuple2.of(1,"hello"),Tuple2.of(2,"world"),Tuple2.of(3,"!"),
                Tuple2.of(4,"I"),Tuple2.of(5,"am"),Tuple2.of(6,"fine"),
                Tuple2.of(7,"!"),Tuple2.of(8,"!"),Tuple2.of(9,"!"),
                Tuple2.of(10,"!"),Tuple2.of(11,"!"),Tuple2.of(12,"!")
        );
        SplitStream<Tuple2<Integer,String>> splitted = inputStream
                .split(x->{
                    if(x.f0%2==0){
                        return java.util.Collections.singleton("even");
                    }else{
                        return java.util.Collections.singleton("odd");
                    }
                });
        DataStream<Tuple2<Integer,String>> evenStream = splitted.select("even");
        DataStream<Tuple2<Integer,String>> oddStream = splitted.select("odd");
        evenStream.print();
        oddStream.print();
        env.execute();
    }
}
