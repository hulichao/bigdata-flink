package com.hoult.transformation;

import com.hoult.stream.SelfSourceParallel;
import com.hoult.stream.SelfSourceParallelExtendsRich;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConntectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data1 = env.addSource(new SelfSourceParallel());
        DataStreamSource<String> data2 = env.addSource(new SelfSourceParallelExtendsRich());
        ConnectedStreams<String, String> connected = data1.connect(data2);
        SingleOutputStreamOperator<String> mapred = connected.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        mapred.print();
        env.execute();


    }
}
