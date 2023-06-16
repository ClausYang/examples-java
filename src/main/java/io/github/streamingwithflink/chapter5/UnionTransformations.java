package io.github.streamingwithflink.chapter5;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionTransformations {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> parisStream = env
                .addSource(new SensorSource());
        DataStream<SensorReading> tokyoStream = env
                .addSource(new SensorSource());
        DataStream<SensorReading> rioStream = env
                .addSource(new SensorSource());
        DataStream<SensorReading> allCities = parisStream
                .union(tokyoStream,rioStream);
        allCities.print();
        env.execute();
    }
}
