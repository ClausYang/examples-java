package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class ConnectTransformations {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> one = env.fromElements(1,2,3);
        DataStream<String> two = env.fromElements("a","b","c");

        ConnectedStreams<Integer,String> connected = one.connect(two);

        DataStream<String> result1 = connected.map(new MyCoMap());

        result1.print();


        DataStream<Tuple2<Integer,Long>> first = env.fromElements(
                Tuple2.of(1,3000L),Tuple2.of(1,4000L),Tuple2.of(1,8000L));
        DataStream<Tuple2<Integer,String>> second = env.fromElements(
                Tuple2.of(1,"hello"),Tuple2.of(1,"world"),Tuple2.of(1,"!"));

        ConnectedStreams<Tuple2<Integer,Long>,Tuple2<Integer,String>> keyConnect1 = first
                .connect(second)
                .keyBy(0,0);//以第一个属性作为key

        ConnectedStreams<Tuple2<Integer,Long>,Tuple2<Integer,String>> keyConnect2 = first
                .keyBy(0)//先keyBy再connect
                .connect(second.keyBy(0));

        ConnectedStreams<Tuple2<Integer,Long>,Tuple2<Integer,String>> keyConnect = first
                .connect(second.broadcast());//不keyby直接broadcast

        DataStream<String>  result2 = keyConnect.flatMap(new MyCoFlatMap());
        DataStream<String> result3 = keyConnect1.flatMap(new MyCoFlatMap());
        DataStream<String> result4 = keyConnect2.flatMap(new MyCoFlatMap());

        result2.print();
        result3.print();
        result4.print();

        env.execute();
    }

    private static class MyCoMap implements org.apache.flink.streaming.api.functions.co.CoMapFunction<Integer,String,String>{
        public String map1(Integer value){
            return value.toString();
        }
        public String map2(String value){
            return value;
        }

    }

    private static class MyCoFlatMap implements org.apache.flink.streaming.api.functions.co.CoFlatMapFunction<Tuple2<Integer,Long>,Tuple2<Integer,String>,String> {
        public void flatMap1(Tuple2<Integer,Long> value, org.apache.flink.util.Collector<String> out){
            out.collect(value.f0.toString()+" "+value.f1.toString());
        }
        public void flatMap2(Tuple2<Integer,String> value, org.apache.flink.util.Collector<String> out){
            out.collect(value.f0.toString()+" "+value.f1);
        }
    }
}
