package com.hoult.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        String inputPath = "E:\\hadoop_res\\input\\a.txt";

        String hdfsPath = "hdfs://linux121:9000/data/wc.txt";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.readTextFile(hdfsPath);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndeOne = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //hello,you,me,hello  [hello  you   me]
                for (String word : s.split("\\s+")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1)); // (hello 1) (hello 1)
                }
            }
        });


        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = wordAndeOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2StringKeyedStream.sum(1);
        result.print();
        env.execute();
    }
}
