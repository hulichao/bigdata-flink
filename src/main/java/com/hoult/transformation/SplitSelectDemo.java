package com.hoult.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SplitSelectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 4, 5, 6, 6, 7, 8, 9);
        SplitStream<Integer> splited = data.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                ArrayList<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                } else
                    output.add("odd");
                return output;
            }
        });

        DataStream<Integer> even = splited.select("even");
        even.print();
        env.execute();

    }
}
