package com.puhulink.market;

import com.puhulink.market.beans.AdClickEvent;
import com.puhulink.market.beans.AdCountViewByProvince;
import com.puhulink.market.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.omg.CORBA.DATA_CONVERSION;

import java.sql.Timestamp;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/13 17:00
 * @description：
 * @modified By：
 * @version: $
 */
public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv");

        SingleOutputStreamOperator<AdClickEvent> dataSource = stringDataStreamSource.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new AdClickEvent(Long.parseLong(split[0]),Long.parseLong(split[1]),split[2],split[3],Long.parseLong(split[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<AdClickEvent> process = dataSource.keyBy("userId", "adId").process(
                new FilterBlackListUser(100)
        );

        SingleOutputStreamOperator<AdCountViewByProvince> aggregate = process.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new AggregateFunction<AdClickEvent, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(AdClickEvent adClickEvent, Long aLong) {
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
                }, new WindowFunction<Long, AdCountViewByProvince, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
                        out.collect(
                                new AdCountViewByProvince(s,new Timestamp(window.getEnd()).toString(),input.iterator().next())
                        );
                    }
                });

        aggregate.print();

        env.execute();
    }

    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent> {
        private Integer clickUpperBound;

        private ValueState<Long> countState;

        private ValueState<Boolean> isSendState;

        public FilterBlackListUser(int i) {
            this.clickUpperBound = i;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state",Long.class,0L));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("send-state",Boolean.class,false));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            //判断当前用户对同一广告的点击次数
            Long count = countState.value();

            if (count == 0) {
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1 ) * (24 * 60 * 60 * 1000) - (8*60*60*1000);
                ctx.timerService().registerEventTimeTimer(ts);
            }

            if ( count  >= clickUpperBound) {
                if (!isSendState.value()) {
                    isSendState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blackList"){},new BlackListUserWarning(value.getUserId(),value.getAdId(),"click over" + clickUpperBound + "times."));
                }
                return;
            }
            countState.update(count+1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            countState.clear();
            isSendState.clear();
        }
    }
}
