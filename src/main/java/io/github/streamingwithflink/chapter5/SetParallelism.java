package io.github.streamingwithflink.chapter5;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SetParallelism {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get default parallelism
        int defaultParallelism = env.getParallelism();
        System.out.println("Default parallelism: " + defaultParallelism);
        // set parallelism to 32
        env.setParallelism(32);
        System.out.println("Setting Parallelism: " + env.getParallelism());
        // datasource with default parallelism = 32
        DataStreamSink<String> result = env.addSource(new SensorSource())
                // map with default parallelism * 2 = 64
                .map(new MyMapper()).setParallelism(defaultParallelism*2)
                // print with parallelism 2 = 2
                .print().setParallelism(2);
        env.execute();
    }

    private static class MyMapper implements MapFunction<SensorReading, String> {

        public String map(SensorReading r) throws Exception {
            return r.toString();
        }
    }
}
