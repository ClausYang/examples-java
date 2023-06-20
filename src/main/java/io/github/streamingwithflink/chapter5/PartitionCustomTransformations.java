package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

public class PartitionCustomTransformations {
    public static class MyPartitioner implements Partitioner<Integer> {
        private Random r = new Random();

        @Override
        public int partition(Integer key, int numPartitions) {
            if (key < 0) {
                return 0;
            } else {
                return r.nextInt(numPartitions);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> numbers = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        numbers.partitionCustom(new MyPartitioner(),0).print();
        env.execute();
    }
}
