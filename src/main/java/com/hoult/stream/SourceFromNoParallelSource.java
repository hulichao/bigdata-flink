package com.hoult.stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 没有并行度的自定义数据源
 */
public class SourceFromNoParallelSource implements SourceFunction<String> {
    long count = 0;
    boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(String.valueOf(count));
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
