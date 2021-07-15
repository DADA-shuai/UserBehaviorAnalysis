package com.puhuilink.networkflw_analysis;

import com.puhuilink.beans.PageViewCount;
import com.puhuilink.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.util.Random;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/12 16:32
 * @description：
 * @modified By：
 * @version: $
 */
public class PageView {
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

//        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 = userBehaviorStream.filter(data -> {
//            return "pv".equals(data.getBehavior());
//        })
//                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
//                        return new Tuple2<String, Long>("pv", 1L);
//                    }
//                })
//                .keyBy(0)  // 按商品ID分组
//                .timeWindow(Time.hours(1))  //开一小时滚动窗口
//                .sum(1);

        //并行任务改进，设计随机key，分配到不同分区
        SingleOutputStreamOperator<PageViewCount> pvStream = userBehaviorStream.filter(data -> {
            return "pv".equals(data.getBehavior());
        }).map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                Random random = new Random();
                return new Tuple2<Integer, Long>(random.nextInt(10) , 1L);
            }
        }).keyBy(data -> data.f0).timeWindow(Time.hours(1)).aggregate(new PvCountAgg(), new PvCountResult());

        //将各分区数据汇总起来
        SingleOutputStreamOperator<PageViewCount> count = pvStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TotleProcess());

        count.print();

        env.execute();

    }

    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> longLongTuple2, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer aLong, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(aLong.toString(),window.getEnd(),input.iterator().next()));
        }
    }

    private static class TotleProcess extends KeyedProcessFunction<Long,PageViewCount,PageViewCount> {

        private ValueState<Long> valueState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value",Long.class,0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            //定义一个状态，保存总count值
            valueState.update(valueState.value()+value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Long totalCount = this.valueState.value();
            out.collect(new PageViewCount("pv",ctx.getCurrentKey(),totalCount));
            valueState.clear();
        }
    }
}
