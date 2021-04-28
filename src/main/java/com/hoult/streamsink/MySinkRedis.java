package com.hoult.streamsink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MySinkRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> data = env.fromElements("hello");

        DataStreamSource<String> data = env.socketTextStream("linux121", 7777);
        SingleOutputStreamOperator<Tuple2<String, String>> my_word = data.map((MapFunction<String, Tuple2<String, String>>) s -> new Tuple2<>("my_word", s));

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("linux121").
                        setPort(6379).build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data.f1;
            }
        });
        my_word.addSink(redisSink);

        env.execute();

    }
}
