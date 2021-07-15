package com.puhuilink.networkflw_analysis;

import com.puhuilink.beans.PageViewCount;
import com.puhuilink.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Random;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/12 16:32
 * @description：
 * @modified By：
 * @version: $
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });


        SingleOutputStreamOperator<PageViewCount> apply = userBehaviorStream.filter(data -> "pv".equals(data.getBehavior())).timeWindowAll(Time.hours(1))
                .apply(new UvCount());

        apply.print();

        env.execute();

    }

    private static class UvCount implements AllWindowFunction<UserBehavior,PageViewCount,TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            HashSet<Long> longs = new HashSet<>();
            for (UserBehavior value : values) {
                longs.add(value.getUserId());
            }
            out.collect(new PageViewCount("uv",window.getEnd(),(long) longs.size()));
        }
    }
}
