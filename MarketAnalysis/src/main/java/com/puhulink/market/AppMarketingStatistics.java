package com.puhulink.market;


import com.puhulink.market.beans.ChannelPromotionCount;
import com.puhulink.market.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/13 16:41
 * @description：
 * @modified By：
 * @version: $
 */
public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                }).filter(data -> !"UNINSTALL".equals(data.getBehavior()));

        SingleOutputStreamOperator<ChannelPromotionCount> total = dataStream
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingStatisticAgg(), new MarketingStatisticResult());

        total.print();

        env.execute();
    }

    private static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

        private Boolean running = true;

        private List<String> behaviorList = Arrays.asList("CLICK","DOWNLOAD","INSTALL","UNINSTALL");

        private List<String> channelList = Arrays.asList("app store","weChat","weibo");

        Random random = new Random();


        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

            while(running) {
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();
                ctx.collect(new MarketingUserBehavior(id,behavior,channel,timestamp));

                Thread.sleep(100);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class MarketingStatisticAgg implements AggregateFunction<Tuple2<String,Long>,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
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

    private static class MarketingStatisticResult implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelPromotionCount> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long next = input.iterator().next();
            out.collect(new ChannelPromotionCount("total","total",windowEnd,next));
        }
    }
}
