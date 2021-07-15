package com.puhulink.market;

import com.puhulink.market.beans.ChannelPromotionCount;
import com.puhulink.market.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/13 11:18
 * @description：
 * @modified By：
 * @version: $
 */
public class AppMarketByChannel {
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

        SingleOutputStreamOperator<ChannelPromotionCount> aggregate = dataStream.keyBy("channel", "behavior").timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketCountAgg(), new MarketCountResult());

        aggregate.print();

        env.execute();
    }

    private static class MarketCountAgg implements AggregateFunction<MarketingUserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long aLong) {
            return aLong + 1L;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    private static class MarketCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
                String channel = tuple.getField(0);
                String behavior = tuple.getField(1);
                String windowEnd = new Timestamp(context.window().getEnd()).toString();
                Long Count = elements.iterator().next();
                out.collect(new ChannelPromotionCount(channel,behavior,windowEnd,Count));
        }
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

}
