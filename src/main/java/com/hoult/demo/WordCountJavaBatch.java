package com.hoult.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountJavaBatch {
    public static void main(String[] args) throws Exception {

        String inputPath = "E:\\hadoop_res\\input\\a.txt";
        String outputPath = "E:\\hadoop_res\\output";

        //获取flink的运行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = executionEnvironment.readTextFile(inputPath);
        FlatMapOperator<String, Tuple2<String, Integer>> wordsOne = text.flatMap(new SplitClz());

        //hello,1  you,1 hi,1  him,1
        UnsortedGrouping<Tuple2<String, Integer>> groupWordAndOne = wordsOne.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> wordCount = groupWordAndOne.sum(1);

        wordCount.writeAsCsv(outputPath, "\n", "\t").setParallelism(1);

        executionEnvironment.execute();
    }

    static class SplitClz implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] strs = s.split("\\s+");
            for (String str : strs) {
                collector.collect(new Tuple2<String, Integer>(str, 1));
            }
        }
    }
}
