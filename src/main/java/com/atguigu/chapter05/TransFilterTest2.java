package com.atguigu.chapter05;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TransFilterTest2 {

    private static OutputTag<Event> pageTag = new OutputTag<Event>("pageTag"){};
    private static OutputTag<Event> homeTag = new OutputTag<Event>("HomeTag"){};

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "", 1000L),
                new Event("Mary", "home", 2000L),
                new Event("", "home", 3000L),
                new Event("", "home", 3000L)
        );

        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                Event result = new Event(value.user, value.url, value.timestamp);
                if (value.url.equals("page")) {
                    ctx.output(pageTag, result);
                } else if (value.url.equals("home")) {
                    ctx.output(homeTag, result);
                } else {
                    out.collect(value);
                }
            }
        });

        DataStream<Event> pageTagOutput = processedStream.getSideOutput(pageTag);
        pageTagOutput.print("pageTagOutput");
        DataStream<Event> homeTagOutput = processedStream.getSideOutput(homeTag);
        homeTagOutput.print("homeTagOutput");
        DataStream<Event> union = pageTagOutput.union(homeTagOutput);
        union.print("union");

        env.execute();
    }

}

