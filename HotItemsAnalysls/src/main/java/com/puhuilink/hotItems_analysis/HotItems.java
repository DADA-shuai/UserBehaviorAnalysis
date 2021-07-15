package com.puhuilink.hotItems_analysis;

import com.puhuilink.beans.ItemViewCount;
import com.puhuilink.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/8 11:10
 * @description：
 * @modified By：
 * @version: $
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = env.readTextFile("C:\\Users\\无敌大大帅逼\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysls\\src\\main\\resources\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = dataStream.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<ItemViewCount> itemId = userBehaviorStream.filter(data -> {
            return "pv".equals(data.getBehavior());
        }).keyBy("itemId").timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        SingleOutputStreamOperator<String> windowEnd = itemId.keyBy("windowEnd").process(new TopHotItems(5));

        windowEnd.print();


        env.execute("hot items analysis");

    }


    private static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
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


    private static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
                Long itemId = tuple.getField(0);
                Long windowEnd = timeWindow.getEnd();
                Long count = iterable.iterator().next();
                collector.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }

    private static class TopHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String> {
        private Integer top ;

        private ListState<ItemViewCount> valueState;

        public TopHotItems(int i) {
            this.top = i;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            valueState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(valueState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue()-o1.getCount().intValue();
                }
            });

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("===============================");
            stringBuilder.append("窗口结束时间:").append(new Timestamp(timestamp-1)).append("\n");

            for (int i=0;i<Math.min(top,itemViewCounts.size());i++){
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                stringBuilder.append("No. ").append(i+1).append(":").append("商品Id = ").append(itemViewCount.getItemId())
                        .append("热门度 = ").append(itemViewCount.getCount()).append("\n");
            }
            stringBuilder.append("================================\n");

            Thread.sleep(1000L);

            out.collect(stringBuilder.toString());

        }
    }
}
