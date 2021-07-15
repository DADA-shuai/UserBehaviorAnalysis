package com.puhuilink.networkflw_analysis;

import com.puhuilink.beans.PageViewCount;
import com.puhuilink.beans.UserBehavior;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.E;
import org.apache.flink.util.Collector;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Objects;

/**
 * @author ：yjj
 * @date ：Created in 2021/7/12 16:32
 * @description：
 * @modified By：
 * @version: $
 */
public class UVWithBloomFilter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
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
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFliter());

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

    private static class MyTrigger extends org.apache.flink.streaming.api.windowing.triggers.Trigger<UserBehavior,TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    private static class UvCountResultWithBloomFliter extends ProcessAllWindowFunction<UserBehavior,PageViewCount,TimeWindow> {

        private JedisCluster jedis;

        private MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            HashSet<HostAndPort> hostAndPorts = new HashSet<>();
            hostAndPorts.add(new HostAndPort("cdh1",7000));
            hostAndPorts.add(new HostAndPort("cdh1",7001));
            hostAndPorts.add(new HostAndPort("cdh1",7002));
            hostAndPorts.add(new HostAndPort("cdh1",7003));
            hostAndPorts.add(new HostAndPort("cdh1",7004));
            hostAndPorts.add(new HostAndPort("cdh1",7005));
            jedis = new JedisCluster(hostAndPorts,1000000,10000,8,"redis123QAZ",new GenericObjectPoolConfig());

            myBloomFilter = new MyBloomFilter(1 << 18); //要处理一亿个数据，用64MB大小的位图
        }

        @Override
        public void close() throws Exception {
            super.close();
            jedis.close();
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口的count值全部存入redis  ， windowEnd作为key
            Long end = context.window().getEnd();
            String s = end.toString();

            //把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = s;


            //1.取当前的userId
            Long userId = elements.iterator().next().getUserId();

            //2.计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            //3.用redis的getbit，判断对应位置的值
            Boolean isExist = jedis.getbit(s, offset);

            //如果不存在，就把对应位图置1 ，更新redis中保存的count值
            if (!isExist){
                jedis.setbit(s,offset,true);

                Long uvCount = 0L;
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString!= null && !"".equals(uvCountString)){
                     uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName,countKey,String.valueOf(uvCount+1));
                out.collect(new PageViewCount("uv",end,uvCount + 1));
            }
        }
    }

    public static class MyBloomFilter {
        // 定义位图大小，一般定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        //实现一个hash函数

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MyBloomFilter)) return false;
            MyBloomFilter that = (MyBloomFilter) o;
            return Objects.equals(cap, that.cap);
        }

        public Long hashCode(String value,Integer seed) {
            Long result = 0L;
            for (int i = 0;i<value.length();i++){
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }
}
