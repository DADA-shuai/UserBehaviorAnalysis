package com.puhuilink.networkflw_analysis;

import com.puhuilink.beans.ApacheLogEvent;
import com.puhuilink.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.awt.*;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/8 14:50
 * @description：
 * @modified By：
 * @version: $
 */
public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log");

        SingleOutputStreamOperator<ApacheLogEvent> inputData = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).filter(date -> "GET".equals(date.getMethod())).filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });

        OutputTag<ApacheLogEvent> late = new OutputTag<ApacheLogEvent>("late"){};

        SingleOutputStreamOperator<PageViewCount> windowAgg = inputData
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new PageCount(), new PageCountResult());

        windowAgg.getSideOutput(late).print();

        SingleOutputStreamOperator<String> result = windowAgg.keyBy(PageViewCount::getWindowEnd).process(new TopHotPages(3));

        result.print();

        env.execute("host pages job");
    }

    private static class PageCount implements AggregateFunction<ApacheLogEvent,Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
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

    private static class PageCountResult implements WindowFunction<Long,PageViewCount,String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(s,window.getEnd(),input.iterator().next()));
        }
    }


    private static class TopHotPages extends KeyedProcessFunction<Long,PageViewCount,String> {
        private Integer top;

        private MapState<String,Long> mapState;

        public TopHotPages(int i) {
            this.top = i;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,Long>("map-page",String.class,Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            mapState.put(value.getUrl(),value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60*1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            if (timestamp == ctx.getCurrentKey() + 60*1000L){
                mapState.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> entries = Lists.newArrayList(mapState.entries());
            entries.sort((a,b)-> b.getValue().intValue()-a.getValue().intValue());

            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(top, entries.size()); i++) {
                Map.Entry<String, Long> stringLongEntry = entries.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(stringLongEntry.getKey())
                        .append(" 浏览量 = ").append(stringLongEntry.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
